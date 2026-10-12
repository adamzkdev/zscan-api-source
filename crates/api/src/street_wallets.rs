//! ZStreet's wallet towers: ZODL, Vizor, Noir and ZLab each have a tower in the city, its walls hung with
//! their tweets and their story (releases, commits, stars, store ratings). Every quarter hour this
//! reads the public sources, none of which needs a key: FxTwitter (the open source embed fixer,
//! api.fxtwitter.com: profiles and whole timelines, paged), X's own embed timeline and
//! single-tweet endpoints (syndication) as a second source, Apple's lookup; every hour GitHub
//! (releases, repositories, the latest commits with their diffs, open pull requests: about twenty
//! calls, under the 60 an hour GitHub allows without a token). What comes back is kept in
//! `street_wallet_feed`, so a source that fails keeps its last good answer. Served by
//! `GET /v1/street/wallets`.

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{extract::State, Json};
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};

use crate::error::ApiError;
use crate::AppState;

const REFRESH: Duration = Duration::from_secs(15 * 60);
const UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36";
const TWEETS_MAX: usize = 60;
/// Pages of 20 read per account from FxTwitter.
const FX_PAGES: usize = 3;
/// GitHub is read every this many refreshes (hourly).
const GITHUB_EVERY: u32 = 4;
/// Commits listed per repository, and how many of them come with their diffs.
const COMMITS: usize = 8;
const COMMIT_DIFFS: usize = 3;
/// Lines of a file's diff kept, files per commit.
const PATCH_LINES: usize = 90;
const PATCH_FILES: usize = 4;
/// Tweets kept per timeline: @zodl_co goes back to 2018, the posters want the recent ones.
const PER_TIMELINE: usize = 24;
const NOTES_MAX: usize = 1200;
/// Tweets read one by one per refresh, at most.
const SINGLE_MAX: usize = 30;

pub(crate) struct Wallet {
    pub(crate) key: &'static str,
    name: &'static str,
    /// The account whose profile heads the tower.
    handle: &'static str,
    /// Timelines read: FxTwitter first, then the embed endpoint (empty for young accounts).
    pub(crate) timelines: &'static [&'static str],
    /// Tweets read one by one (the community's, or the account's when its timeline is empty).
    /// `STREET_WALLET_TWEETS_<KEY>` adds more, comma-separated ids.
    tweets: &'static [&'static str],
    pub(crate) repos: &'static [&'static str],
    store: Store,
    site: &'static str,
}

/// Where the wallet is installed from: Apple's App Store (lookup API) or the Chrome Web Store
/// (its page: rating, users, version).
enum Store {
    Apple(u64),
    Chrome(&'static str),
    /// Not in a store yet (ZLab's apps): no rating card.
    None,
}

pub(crate) const WALLETS: &[Wallet] = &[
    Wallet {
        key: "zodl",
        name: "Zodl",
        handle: "zodl_app",
        timelines: &["zodl_app", "zodl_co"],
        tweets: &[],
        repos: &["zodl-inc/zodl-ios", "zodl-inc/zodl-android"],
        store: Store::Apple(1672392439),
        site: "https://www.zodl.com",
    },
    Wallet {
        key: "vizor",
        name: "Vizor",
        handle: "vizorwallet",
        timelines: &["vizorwallet"],
        // the words of its users on vizor.cash
        tweets: &[
            "2054958969467330760",
            "2071623986522239384",
            "2071654991727890903",
            "2071964162260156621",
            "2072068526978855054",
        ],
        repos: &["chainapsis/vizor-wallet"],
        store: Store::Apple(6776591245),
        site: "https://vizor.cash",
    },
    Wallet {
        key: "noir",
        name: "Noir",
        handle: "noir_wallet",
        timelines: &["noir_wallet"],
        tweets: &[],
        repos: &[
            "NoirWallet/noir-wallet-sdk",
            "NoirWallet/zcash-wallet-adapter",
        ],
        store: Store::Chrome("mfoghjbpfanobmnoemoepenjjcmfpmdn"),
        site: "https://zknoir.com",
    },
    // ZLab: no X account, no store listing yet; its tower shows GitHub only, a floor per repo
    Wallet {
        key: "zlab",
        name: "ZLab",
        handle: "",
        timelines: &[],
        tweets: &[],
        repos: &[
            "ZLabcash/zapp",
            "ZLabcash/zapp-chrome",
            "ZLabcash/zfirm",
            "ZLabcash/kindlewallet",
        ],
        store: Store::None,
        site: "https://zlab.cash",
    },
];

pub async fn run(state: AppState) {
    let mut cycle: u32 = 0;
    loop {
        let github = cycle.is_multiple_of(GITHUB_EVERY);
        cycle = cycle.wrapping_add(1);
        for w in WALLETS {
            let old: Option<Value> =
                sqlx::query_scalar("SELECT feed FROM street_wallet_feed WHERE wallet = $1")
                    .bind(w.key)
                    .fetch_optional(&state.db.pool)
                    .await
                    .ok()
                    .flatten();
            let old = old.unwrap_or(Value::Null);
            // a feed from before commits were kept: read GitHub now
            let github = github || old["commits"].is_null();
            match load(&state.http, w, &old, github).await {
                Ok(feed) => {
                    let saved = sqlx::query(
                        "INSERT INTO street_wallet_feed (wallet, feed, fetched_at) VALUES ($1, $2, now())
                         ON CONFLICT (wallet) DO UPDATE SET feed = EXCLUDED.feed, fetched_at = now()",
                    )
                    .bind(w.key)
                    .bind(&feed)
                    .execute(&state.db.pool)
                    .await;
                    if let Err(e) = saved {
                        tracing::warn!("street wallets {}: {e}", w.key);
                    }
                }
                Err(e) => tracing::warn!("street wallets {}: {e:#}", w.key),
            }
        }
        tokio::time::sleep(REFRESH).await;
    }
}

/// `GET /v1/street/wallets`: `{ zodl: {...}, vizor: {...} }`, each with its profile, tweets,
/// releases, repositories and store ratings.
pub async fn wallets(State(s): State<AppState>) -> Result<Json<Value>, ApiError> {
    let rows: Vec<(String, Value, DateTime<Utc>)> =
        sqlx::query_as("SELECT wallet, feed, fetched_at FROM street_wallet_feed")
            .fetch_all(&s.db.pool)
            .await?;
    let mut out = Map::new();
    for (k, mut feed, at) in rows {
        feed["fetched_at"] = json!(at);
        out.insert(k, feed);
    }
    Ok(Json(Value::Object(out)))
}

/// A repository just moved (the pulse saw a commit, a release or a pull request): its part of the
/// wallet's feed is read again now rather than at the next hourly GitHub round, so the lab's boards
/// show what the zkSNARK announces. Five calls or so.
pub(crate) async fn refresh_repo(state: &AppState, wallet: &str, r: &str) -> Result<()> {
    let old: Option<Value> =
        sqlx::query_scalar("SELECT feed FROM street_wallet_feed WHERE wallet = $1")
            .bind(wallet)
            .fetch_optional(&state.db.pool)
            .await?;
    let Some(mut feed) = old else { return Ok(()) };
    let http = &state.http;
    let (releases, commits, pulls) = (
        github_releases(http, r).await?,
        github_commits(http, r).await?,
        github_pulls(http, r).await?,
    );
    let swap = |feed: &mut Value, k: &str, fresh: Vec<Value>, by: &str| {
        let mut list: Vec<Value> = feed[k]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|v| v["repo"].as_str() != Some(r))
            .collect();
        list.extend(fresh);
        list.sort_by(|a, b| b[by].as_str().cmp(&a[by].as_str()));
        feed[k] = json!(list);
    };
    swap(&mut feed, "releases", releases, "date");
    swap(&mut feed, "commits", commits, "date");
    swap(&mut feed, "pulls", pulls, "updated");
    // the repository card keeps its place among the others
    if let (Ok(v), Some(list)) = (repo(http, r).await, feed["repos"].as_array_mut()) {
        match list.iter_mut().find(|x| x["repo"].as_str() == Some(r)) {
            Some(slot) => *slot = v,
            None => list.push(v),
        }
    }
    sqlx::query("UPDATE street_wallet_feed SET feed = $2, fetched_at = now() WHERE wallet = $1")
        .bind(wallet)
        .bind(&feed)
        .execute(&state.db.pool)
        .await?;
    Ok(())
}

/// One wallet's feed. Each source on its own: one that fails leaves its part as it was in `old`
/// (the last feed saved).
async fn load(http: &reqwest::Client, w: &Wallet, old: &Value, github: bool) -> Result<Value> {
    // fxtwitter says "User not found" now and then: a couple more tries before the old profile
    let mut profile_now = Err(anyhow::anyhow!("not tried"));
    // no X account: no profile to read
    let attempts = if w.handle.is_empty() { 0 } else { 3 };
    for attempt in 0..attempts {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        profile_now = profile(http, w.handle).await;
        if profile_now.is_ok() {
            break;
        }
    }
    let profile = profile_now.unwrap_or_else(|e| {
        if !w.handle.is_empty() {
            tracing::warn!("profile {}: {e:#}", w.handle);
        }
        old["profile"].clone()
    });
    let mut tweets = Vec::new();
    let mut seen = HashSet::new();
    for h in w.timelines {
        match fx_timeline(http, h, FX_PAGES).await {
            Ok(list) => {
                for t in list {
                    if seen.insert(t["id"].as_str().unwrap_or_default().to_string()) {
                        tweets.push(t);
                    }
                }
            }
            Err(e) => tracing::warn!("fxtwitter {h}: {e:#}"),
        }
        match timeline(http, h).await {
            Ok(list) => {
                for t in list.into_iter().take(PER_TIMELINE) {
                    if seen.insert(t["id"].as_str().unwrap_or_default().to_string()) {
                        tweets.push(t);
                    }
                }
            }
            Err(e) => tracing::debug!("timeline {h}: {e:#}"),
        }
    }
    let extra =
        std::env::var(format!("STREET_WALLET_TWEETS_{}", w.key.to_uppercase())).unwrap_or_default();
    let mut queue: Vec<String> = w
        .tweets
        .iter()
        .map(|s| s.to_string())
        .chain(extra.split(',').map(|s| s.trim().to_string()))
        .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
        .collect();
    // a tweet read alone brings the wallet's own tweet it quotes or answers (Vizor's own timeline
    // isn't served to embeds yet; its users quote it)
    let mut reads = 0;
    while let Some(id) = queue.pop() {
        if seen.contains(&id) || reads >= SINGLE_MAX {
            continue;
        }
        reads += 1;
        match single(http, &id).await {
            Ok((t, linked)) => {
                seen.insert(id);
                tweets.push(t);
                queue.extend(
                    linked
                        .into_iter()
                        .filter(|(h, _)| h.eq_ignore_ascii_case(w.handle))
                        .map(|(_, id)| id),
                );
            }
            Err(e) => tracing::debug!("tweet {id}: {e:#}"),
        }
    }
    tweets.sort_by(|a, b| b["at"].as_str().cmp(&a["at"].as_str()));
    tweets.truncate(TWEETS_MAX);

    let (mut releases, mut repos, mut commits, mut pulls) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    if github {
        for r in w.repos {
            match repo(http, r).await {
                Ok(v) => repos.push(v),
                Err(e) => tracing::warn!("github {r}: {e:#}"),
            }
            let rels = github_releases(http, r).await;
            let cs = github_commits(http, r).await;
            // over GitHub's quota: the Atom feeds for this repository's commits and releases
            let (atom_commits, atom_releases) = if rels.is_err() || cs.is_err() {
                crate::street_pulse::atom_fallback(http, r).await
            } else {
                Default::default()
            };
            releases.extend(rels.unwrap_or(atom_releases));
            commits.extend(cs.unwrap_or(atom_commits));
            if let Ok(list) = github_pulls(http, r).await {
                pulls.extend(list);
            }
        }
    }
    let newest = |a: &Value, b: &Value, k: &str| b[k].as_str().cmp(&a[k].as_str());
    releases.sort_by(|a, b| newest(a, b, "date"));
    commits.sort_by(|a, b| newest(a, b, "date"));
    pulls.sort_by(|a, b| newest(a, b, "updated"));
    let store = match w.store {
        Store::Apple(id) => app_store(http, id).await.ok(),
        Store::Chrome(id) => chrome_store(http, id).await.ok(),
        Store::None => None,
    };

    // nothing at all came back: keep the last feed rather than an empty one
    anyhow::ensure!(
        !(tweets.is_empty()
            && repos.is_empty()
            && commits.is_empty()
            && store.is_none()
            && old.is_null()),
        "every source failed"
    );
    let or_old = |v: Value, k: &str| match &v {
        // nothing new: the last list, if there was one (a first feed keeps its empty lists)
        Value::Array(a) if a.is_empty() && !old[k].is_null() => old[k].clone(),
        Value::Null => old[k].clone(),
        _ => v,
    };
    let (tweets, releases, repos, store) = (
        or_old(json!(tweets), "tweets"),
        or_old(json!(releases), "releases"),
        or_old(json!(repos), "repos"),
        or_old(json!(store), "store"),
    );
    let (commits, pulls) = (
        or_old(json!(commits), "commits"),
        or_old(json!(pulls), "pulls"),
    );
    Ok(json!({
        "key": w.key,
        "name": w.name,
        "handle": w.handle,
        "site": w.site,
        "profile": profile,
        "tweets": tweets,
        "releases": releases,
        "repos": repos,
        "commits": commits,
        "pulls": pulls,
        "store": store,
    }))
}

fn get(http: &reqwest::Client, url: &str) -> reqwest::RequestBuilder {
    http.get(url)
        .header("User-Agent", UA)
        .timeout(Duration::from_secs(20))
}

/// The tweet's text with its t.co links written out and the media links dropped (the media is
/// shown as a picture).
fn clean_text(t: &Value) -> String {
    let mut text = t["full_text"]
        .as_str()
        .or(t["text"].as_str())
        .unwrap_or_default()
        .to_string();
    for u in t["entities"]["urls"].as_array().into_iter().flatten() {
        if let (Some(short), Some(shown)) = (u["url"].as_str(), u["display_url"].as_str()) {
            text = text.replace(short, shown);
        }
    }
    for m in t["entities"]["media"].as_array().into_iter().flatten() {
        if let Some(short) = m["url"].as_str() {
            text = text.replace(short, "");
        }
    }
    text.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .trim()
        .to_string()
}

fn media(t: &Value) -> Vec<Value> {
    let list = [
        &t["extended_entities"]["media"],
        &t["mediaDetails"],
        &t["entities"]["media"],
    ]
    .into_iter()
    .filter_map(|v| v.as_array())
    .find(|a| !a.is_empty());
    list.into_iter()
        .flatten()
        .filter_map(|m| {
            let url = m["media_url_https"].as_str()?;
            let (w, h) = (
                m["original_info"]["width"].as_u64().unwrap_or(0),
                m["original_info"]["height"].as_u64().unwrap_or(0),
            );
            Some(
                json!({ "url": url, "w": w, "h": h, "video": m["type"].as_str() != Some("photo") }),
            )
        })
        .take(4)
        .collect()
}

fn avatar(u: &Value) -> Value {
    u["profile_image_url_https"]
        .as_str()
        .map(|s| json!(s.replace("_normal.", "_200x200.")))
        .unwrap_or(Value::Null)
}

/// X's "Mon Oct 08 16:29:00 +0000 2026" or an RFC 3339 date, as RFC 3339.
fn date(s: &str) -> Option<String> {
    DateTime::parse_from_str(s, "%a %b %d %H:%M:%S %z %Y")
        .or_else(|_| DateTime::parse_from_rfc3339(s))
        .ok()
        .map(|d| d.with_timezone(&Utc).to_rfc3339())
}

/// A tweet as the posters read it. A retweet becomes the original, with who retweeted it.
fn tweet(raw: &Value) -> Option<Value> {
    let (t, rt_by) = match raw.get("retweeted_status").filter(|v| v.is_object()) {
        Some(o) => (o, raw["user"]["screen_name"].as_str()),
        None => (raw, None),
    };
    let user = &t["user"];
    let handle = user["screen_name"].as_str()?;
    let id = t["id_str"]
        .as_str()
        .filter(|s| *s != "0")
        .or_else(|| {
            // retweeted originals come with id 0: the permalink carries it
            t["permalink"].as_str().and_then(|p| p.rsplit('/').next())
        })
        .or(raw["id_str"].as_str())?;
    Some(json!({
        "id": id,
        "url": format!("https://x.com/{handle}/status/{id}"),
        "handle": handle,
        "name": user["name"],
        "avatar": avatar(user),
        "verified": user["is_blue_verified"].as_bool().unwrap_or(false) || user["verified"].as_bool().unwrap_or(false),
        "text": clean_text(t),
        "at": date(t["created_at"].as_str()?)?,
        "likes": t["favorite_count"].as_u64().unwrap_or(0),
        "retweets": t["retweet_count"].as_u64().unwrap_or(0),
        "replies": t["reply_count"].as_u64().or(t["conversation_count"].as_u64()).unwrap_or(0),
        "media": media(t),
        "rt_by": rt_by,
    }))
}

/// The embed timeline of an account (its latest tweets and retweets), from the page's Next.js data.
async fn timeline(http: &reqwest::Client, handle: &str) -> Result<Vec<Value>> {
    let html = get(
        http,
        &format!("https://syndication.twitter.com/srv/timeline-profile/screen-name/{handle}"),
    )
    .send()
    .await?
    .error_for_status()?
    .text()
    .await?;
    let start = html
        .find("id=\"__NEXT_DATA__\"")
        .and_then(|i| html[i..].find('>').map(|j| i + j + 1))
        .context("no timeline data")?;
    let end = start
        + html[start..]
            .find("</script>")
            .context("unterminated data")?;
    let data: Value = serde_json::from_str(&html[start..end])?;
    let entries = data["props"]["pageProps"]["timeline"]["entries"]
        .as_array()
        .context("no entries")?;
    Ok(entries
        .iter()
        .filter_map(|e| tweet(&e["content"]["tweet"]))
        .collect())
}

/// One tweet by id, from the embed's single-tweet endpoint, with the tweets it quotes or answers
/// (handle, id).
async fn single(http: &reqwest::Client, id: &str) -> Result<(Value, Vec<(String, String)>)> {
    let t: Value = get(
        http,
        &format!("https://cdn.syndication.twimg.com/tweet-result?id={id}&token=4"),
    )
    .send()
    .await?
    .error_for_status()?
    .json()
    .await?;
    let linked = [&t["quoted_tweet"], &t["parent"]]
        .into_iter()
        .filter_map(|o| {
            Some((
                o["user"]["screen_name"].as_str()?.to_string(),
                o["id_str"].as_str()?.to_string(),
            ))
        })
        .collect();
    Ok((tweet(&t).context("unreadable tweet")?, linked))
}

/// An account's timeline from FxTwitter, `pages` pages of twenty (its tweets, its reposts).
/// FxTwitter answers 404 now and then: each page gets a second try.
pub(crate) async fn fx_timeline(
    http: &reqwest::Client,
    handle: &str,
    pages: usize,
) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..pages {
        let mut url = format!("https://api.fxtwitter.com/2/profile/{handle}/statuses");
        if let Some(c) = &cursor {
            url.push_str(&format!("?cursor={}", urlencode(c)));
        }
        let mut page: Option<Value> = None;
        for attempt in 0..2 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            let res = http
                .get(&url)
                .header("User-Agent", "zscan")
                .timeout(Duration::from_secs(20))
                .send()
                .await;
            if let Ok(r) = res {
                if let Ok(v) = r.json::<Value>().await {
                    if v["results"].as_array().is_some_and(|a| !a.is_empty()) {
                        page = Some(v);
                        break;
                    }
                }
            }
        }
        let Some(v) = page else { break };
        out.extend(
            v["results"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(fx_tweet),
        );
        cursor = v["cursor"]["bottom"].as_str().map(str::to_string);
        if cursor.is_none() {
            break;
        }
    }
    anyhow::ensure!(!out.is_empty(), "no statuses");
    Ok(out)
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// A tweet from FxTwitter as the posters read it (the same shape as `tweet`).
fn fx_tweet(t: &Value) -> Option<Value> {
    let a = &t["author"];
    let handle = a["screen_name"].as_str()?;
    let id = t["id"].as_str()?;
    let media: Vec<Value> = t["media"]["all"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| {
            let video = m["type"].as_str() != Some("photo");
            let url = if video {
                m["thumbnail_url"].as_str()
            } else {
                m["url"].as_str()
            }?;
            Some(json!({ "url": url, "w": m["width"], "h": m["height"], "video": video }))
        })
        .take(4)
        .collect();
    let mut text = t["text"].as_str().unwrap_or_default().trim().to_string();
    // a bare quote: the quoted tweet's words
    let quote = t["quote"]["author"]["screen_name"].as_str().map(|h| {
        json!({ "handle": h, "name": t["quote"]["author"]["name"], "text": t["quote"]["text"] })
    });
    if text.is_empty() {
        if let Some(q) = t["quote"]["text"].as_str() {
            text = q.to_string();
        }
    }
    Some(json!({
        "id": id,
        "url": format!("https://x.com/{handle}/status/{id}"),
        "handle": handle,
        "name": a["name"],
        "avatar": a["avatar_url"],
        "verified": a["verification"]["verified"].as_bool().unwrap_or(false),
        "text": text,
        "at": date(t["created_at"].as_str()?)?,
        "likes": t["likes"].as_u64().unwrap_or(0),
        "retweets": t["reposts"].as_u64().unwrap_or(0),
        "replies": t["replies"].as_u64().unwrap_or(0),
        "views": t["views"].as_u64(),
        "media": media,
        "rt_by": t["reposted_by"]["screen_name"],
        "quote": quote,
    }))
}

/// The latest commits on the default branch, the newest few with their diffs (the code the
/// developers' screens in the lab scroll through).
async fn github_commits(http: &reqwest::Client, repo: &str) -> Result<Vec<Value>> {
    let list: Vec<Value> = github(
        http,
        &format!("https://api.github.com/repos/{repo}/commits?per_page={COMMITS}"),
    )
    .send()
    .await?
    .error_for_status()?
    .json()
    .await?;
    let mut out = Vec::new();
    for (i, c) in list.iter().enumerate() {
        let sha = c["sha"].as_str().unwrap_or_default();
        let mut files = Vec::new();
        if i < COMMIT_DIFFS {
            let full: Result<Value> = async {
                Ok(github(
                    http,
                    &format!("https://api.github.com/repos/{repo}/commits/{sha}"),
                )
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?)
            }
            .await;
            if let Ok(full) = full {
                files = full["files"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|f| f["patch"].is_string())
                    .take(PATCH_FILES)
                    .map(|f| {
                        let patch: Vec<&str> = f["patch"]
                            .as_str()
                            .unwrap_or_default()
                            .lines()
                            .take(PATCH_LINES)
                            .collect();
                        json!({
                            "name": f["filename"],
                            "additions": f["additions"],
                            "deletions": f["deletions"],
                            "patch": patch.join("\n"),
                        })
                    })
                    .collect();
            }
        }
        let message = c["commit"]["message"].as_str().unwrap_or_default();
        out.push(json!({
            "repo": repo,
            "sha": &sha[..sha.len().min(7)],
            "message": message.lines().next().unwrap_or_default(),
            "author": c["commit"]["author"]["name"],
            "login": c["author"]["login"],
            "avatar": c["author"]["avatar_url"],
            "date": c["commit"]["author"]["date"],
            "url": c["html_url"],
            "files": files,
        }));
    }
    Ok(out)
}

/// Open pull requests, the most recently touched first.
async fn github_pulls(http: &reqwest::Client, repo: &str) -> Result<Vec<Value>> {
    let list: Vec<Value> = github(
        http,
        &format!("https://api.github.com/repos/{repo}/pulls?state=open&sort=updated&direction=desc&per_page=8"),
    )
    .send()
    .await?
    .error_for_status()?
    .json()
    .await?;
    Ok(list
        .iter()
        .map(|p| {
            json!({
                "repo": repo,
                "number": p["number"],
                "title": p["title"],
                "login": p["user"]["login"],
                "avatar": p["user"]["avatar_url"],
                "draft": p["draft"],
                "updated": p["updated_at"],
                "url": p["html_url"],
            })
        })
        .collect())
}

async fn profile(http: &reqwest::Client, handle: &str) -> Result<Value> {
    // fxtwitter answers browsers' user agents with a 404 at times: a plain one it always serves
    let v: Value = http
        .get(format!("https://api.fxtwitter.com/{handle}"))
        .header("User-Agent", "zscan")
        .timeout(Duration::from_secs(20))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let u = &v["user"];
    anyhow::ensure!(u.is_object(), "no user");
    Ok(json!({
        "name": u["name"],
        "handle": u["screen_name"],
        "description": u["description"],
        "followers": u["followers"],
        "following": u["following"],
        "tweets": u["tweets"],
        "avatar": u["avatar_url"].as_str().map(|s| s.replace("_normal.", "_400x400.")),
        "banner": u["banner_url"],
        "joined": u["joined"].as_str().and_then(date),
        "location": u["location"],
    }))
}

pub(crate) fn github(http: &reqwest::Client, url: &str) -> reqwest::RequestBuilder {
    let mut req = http
        .get(url)
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "zscan")
        .timeout(Duration::from_secs(20));
    if let Ok(token) = std::env::var("GITHUB_TOKEN") {
        req = req.bearer_auth(token);
    }
    req
}

async fn repo(http: &reqwest::Client, repo: &str) -> Result<Value> {
    let r: Value = github(http, &format!("https://api.github.com/repos/{repo}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(json!({
        "repo": repo,
        "url": r["html_url"],
        "description": r["description"],
        "stars": r["stargazers_count"],
        "forks": r["forks_count"],
        "issues": r["open_issues_count"],
        "language": r["language"],
        "pushed_at": r["pushed_at"],
    }))
}

async fn github_releases(http: &reqwest::Client, repo: &str) -> Result<Vec<Value>> {
    let list: Vec<Value> = github(
        http,
        &format!("https://api.github.com/repos/{repo}/releases?per_page=12"),
    )
    .send()
    .await?
    .error_for_status()?
    .json()
    .await?;
    Ok(list
        .iter()
        .filter(|r| {
            !r["prerelease"].as_bool().unwrap_or(false) && !r["draft"].as_bool().unwrap_or(false)
        })
        .take(6)
        .map(|r| {
            let tag = r["tag_name"].as_str().unwrap_or_default();
            let notes: String = r["body"]
                .as_str()
                .unwrap_or_default()
                .chars()
                .take(NOTES_MAX)
                .collect();
            json!({
                "repo": repo,
                "tag": tag.trim_start_matches("release/"),
                "name": r["name"],
                "date": r["published_at"],
                "url": r["html_url"],
                "notes": notes,
            })
        })
        .collect())
}

/// A Chrome Web Store listing: its rating, users and version, read off the public page.
async fn chrome_store(http: &reqwest::Client, id: &str) -> Result<Value> {
    let url = format!("https://chromewebstore.google.com/detail/{id}");
    let html = get(http, &url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let after = |marker: &str, take: fn(char) -> bool| -> Option<String> {
        let i = html.find(marker)? + marker.len();
        let v: String = html[i..]
            .chars()
            .skip_while(|c| !take(*c))
            .take_while(|c| take(*c))
            .collect();
        (!v.is_empty()).then_some(v)
    };
    // "4.0 out of 5", "50,000 users", "Version</div><div …>1.0.38"
    let rating = html
        .find(" out of 5")
        .and_then(|i| {
            html[..i]
                .rsplit(|c: char| !(c.is_ascii_digit() || c == '.'))
                .next()
        })
        .and_then(|v| v.parse::<f64>().ok());
    let users = html
        .find(" users")
        .map(|i| {
            html[..i]
                .chars()
                .rev()
                .take_while(|c| c.is_ascii_digit() || *c == ',' || *c == '+')
                .collect::<String>()
        })
        .map(|v| {
            v.chars()
                .rev()
                .filter(|c| c.is_ascii_digit())
                .collect::<String>()
        })
        .and_then(|v| v.parse::<u64>().ok());
    let version = after("Version</div>", |c| c.is_ascii_digit() || c == '.');
    anyhow::ensure!(rating.is_some() || version.is_some(), "unreadable listing");
    Ok(json!({
        "name": "Chrome Web Store",
        "rating": rating,
        "ratings": null,
        "users": users,
        "version": version,
        "released": null,
        "notes": null,
        "url": url,
        "chrome": true,
    }))
}

async fn app_store(http: &reqwest::Client, id: u64) -> Result<Value> {
    let v: Value = get(http, &format!("https://itunes.apple.com/lookup?id={id}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let a = v["results"].get(0).context("not on the store")?;
    Ok(json!({
        "name": a["trackName"],
        "rating": a["averageUserRating"],
        "ratings": a["userRatingCount"],
        "version": a["version"],
        "released": a["currentVersionReleaseDate"],
        "notes": a["releaseNotes"],
        "url": a["trackViewUrl"],
        "size": a["fileSizeBytes"],
        "seller": a["sellerName"],
        "genre": a["primaryGenreName"],
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retweets_become_the_original() {
        let raw = json!({
            "id_str": "1", "created_at": "Thu Oct 08 16:29:00 +0000 2026",
            "full_text": "RT @zodl_co: hello", "user": {"screen_name": "zodl_app", "name": "Zodl"},
            "retweeted_status": {
                "id": 0, "permalink": "/zodl_co/status/42", "created_at": "Thu Oct 08 16:15:30 +0000 2026",
                "full_text": "hello https://t.co/x &amp; more https://t.co/m",
                "entities": {"urls": [{"url": "https://t.co/x", "display_url": "zodl.com"}],
                             "media": [{"url": "https://t.co/m", "media_url_https": "https://pbs/1.jpg", "type": "photo"}]},
                "favorite_count": 7, "user": {"screen_name": "zodl_co", "name": "ZODL", "profile_image_url_https": "https://pbs/a_normal.jpg"}
            }
        });
        let t = tweet(&raw).unwrap();
        assert_eq!(t["id"], "42");
        assert_eq!(t["handle"], "zodl_co");
        assert_eq!(t["rt_by"], "zodl_app");
        assert_eq!(t["text"], "hello zodl.com & more");
        assert_eq!(t["likes"], 7);
        assert_eq!(t["avatar"], "https://pbs/a_200x200.jpg");
        assert_eq!(t["media"][0]["url"], "https://pbs/1.jpg");
        assert_eq!(t["at"], "2026-10-08T16:15:30+00:00");
    }
}
