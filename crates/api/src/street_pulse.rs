//! ZStreet's wallet towers, live. Every minute this reads what moved in the towers' repositories:
//! GitHub's public Atom feeds (commits on the default branch, releases: no key, no API quota) and,
//! when `GITHUB_TOKEN` is set, the repositories' events (pushes to any branch, pull requests),
//! asked with their ETag so an unchanged answer costs nothing. Every three minutes the wallets'
//! X timelines (FxTwitter, first page). What is new goes to `street_wallet_pulse`; a zkSNARK in the
//! tower announces it to whoever is inside (`GET /v1/street/wallets/pulse?since=`).

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::{
    extract::{Query, State},
    Json,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::ApiError;
use crate::street_wallets::{self, WALLETS};
use crate::AppState;

const TICK: Duration = Duration::from_secs(60);
/// Releases and tweets are read every this many ticks.
const SLOW_EVERY: u32 = 3;
/// Only what happened in the last hours is news (a feed seen for the first time brings its past).
const FRESH_HOURS: i64 = 6;
/// Without a token GitHub allows 60 API calls an hour: a repository's boards are read again at
/// most this often then.
const REFRESH_GAP: Duration = Duration::from_secs(10 * 60);

struct Item {
    uid: String,
    kind: &'static str,
    repo: Option<String>,
    actor: Option<String>,
    avatar: Option<String>,
    title: String,
    url: Option<String>,
    at: DateTime<Utc>,
}

pub async fn run(state: AppState) {
    let token = std::env::var("GITHUB_TOKEN").is_ok_and(|t| !t.is_empty());
    let mut etags: HashMap<String, String> = HashMap::new();
    let mut refreshed: HashMap<String, Instant> = HashMap::new();
    let mut cycle: u32 = 0;
    loop {
        let slow = cycle.is_multiple_of(SLOW_EVERY);
        cycle = cycle.wrapping_add(1);
        let http = &state.http;
        for w in WALLETS {
            let mut items = Vec::new();
            for r in w.repos {
                match commits_atom(http, r, None).await {
                    Ok(list) => items.extend(list),
                    Err(e) => tracing::debug!("pulse commits {r}: {e:#}"),
                }
                if slow {
                    match releases_atom(http, r).await {
                        Ok(list) => items.extend(list),
                        Err(e) => tracing::debug!("pulse releases {r}: {e:#}"),
                    }
                }
                if token {
                    match events(http, r, &mut etags).await {
                        Ok(list) => items.extend(list),
                        Err(e) => tracing::debug!("pulse events {r}: {e:#}"),
                    }
                }
            }
            if slow {
                for h in w.timelines {
                    match street_wallets::fx_timeline(http, h, 1).await {
                        Ok(list) => items.extend(list.iter().filter_map(tweet)),
                        Err(e) => tracing::debug!("pulse tweets {h}: {e:#}"),
                    }
                }
            }
            let fresh = Utc::now() - chrono::Duration::hours(FRESH_HOURS);
            let mut moved: HashSet<String> = HashSet::new();
            for it in items.into_iter().filter(|i| i.at > fresh) {
                match save(&state, w.key, &it).await {
                    Ok(true) => {
                        if let Some(r) = &it.repo {
                            moved.insert(r.clone());
                        }
                    }
                    Ok(false) => {}
                    Err(e) => tracing::warn!("pulse save: {e:#}"),
                }
            }
            for r in moved {
                let due = token
                    || refreshed
                        .get(&r)
                        .is_none_or(|t| t.elapsed() >= REFRESH_GAP);
                if due {
                    refreshed.insert(r.clone(), Instant::now());
                    if let Err(e) = street_wallets::refresh_repo(&state, w.key, &r).await {
                        tracing::warn!("pulse refresh {r}: {e:#}");
                    }
                }
            }
        }
        // a day of pulse is plenty: the walls keep the rest
        let _ = sqlx::query("DELETE FROM street_wallet_pulse WHERE at < now() - interval '2 days'")
            .execute(&state.db.pool)
            .await;
        tokio::time::sleep(TICK).await;
    }
}

/// True when the item is new.
async fn save(state: &AppState, wallet: &str, it: &Item) -> Result<bool> {
    let done = sqlx::query(
        "INSERT INTO street_wallet_pulse (uid, wallet, kind, repo, actor, avatar, title, url, at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (uid) DO NOTHING",
    )
    .bind(&it.uid)
    .bind(wallet)
    .bind(it.kind)
    .bind(&it.repo)
    .bind(&it.actor)
    .bind(&it.avatar)
    .bind(&it.title)
    .bind(&it.url)
    .bind(it.at)
    .execute(&state.db.pool)
    .await?;
    Ok(done.rows_affected() == 1)
}

async fn atom(http: &reqwest::Client, url: &str) -> Result<feed_rs::model::Feed> {
    let body = http
        .get(url)
        .header("User-Agent", "zscan")
        .timeout(Duration::from_secs(20))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    Ok(feed_rs::parser::parse(&body[..])?)
}

fn first_line(s: &str) -> String {
    let line = s.trim().lines().next().unwrap_or_default().trim();
    line.chars().take(200).collect()
}

/// The latest commits of a branch (the default one when `None`), from its Atom feed.
async fn commits_atom(http: &reqwest::Client, r: &str, branch: Option<&str>) -> Result<Vec<Item>> {
    let url = match branch {
        Some(b) => format!("https://github.com/{r}/commits/{b}.atom"),
        None => format!("https://github.com/{r}/commits.atom"),
    };
    let feed = atom(http, &url).await?;
    Ok(feed
        .entries
        .into_iter()
        .take(10)
        .filter_map(|e| {
            let sha = e.id.rsplit('/').next()?.to_string();
            let title = e
                .content
                .as_ref()
                .and_then(|c| c.body.as_deref())
                .map(strip_tags)
                .filter(|t| !t.is_empty())
                .or_else(|| e.title.as_ref().map(|t| t.content.clone()))?;
            Some(Item {
                uid: format!("c:{sha}"),
                kind: "commit",
                repo: Some(r.to_string()),
                actor: e.authors.first().and_then(|a| a.name.clone()),
                avatar: e
                    .media
                    .iter()
                    .flat_map(|m| m.thumbnails.iter())
                    .next()
                    .map(|t| t.image.uri.replace("s=30", "s=96")),
                title: first_line(&title),
                url: e.links.first().map(|l| l.href.clone()),
                at: e.updated.or(e.published)?,
            })
        })
        .collect())
}

async fn releases_atom(http: &reqwest::Client, r: &str) -> Result<Vec<Item>> {
    let feed = atom(http, &format!("https://github.com/{r}/releases.atom")).await?;
    Ok(feed
        .entries
        .into_iter()
        .take(5)
        .filter_map(|e| {
            let tag = e.id.rsplit('/').next()?.to_string();
            Some(Item {
                uid: format!("r:{r}:{tag}"),
                kind: "release",
                repo: Some(r.to_string()),
                actor: e.authors.first().and_then(|a| a.name.clone()),
                avatar: None,
                title: e
                    .title
                    .map(|t| first_line(&t.content))
                    .unwrap_or_else(|| tag.clone()),
                url: e.links.first().map(|l| l.href.clone()),
                at: e.updated.or(e.published)?,
            })
        })
        .collect())
}

/// The repository's events since the last answer (304 when nothing moved). Pushes to another
/// branch than the default one bring that branch's commits (its Atom feed); pull requests opened
/// or merged come as they are.
async fn events(
    http: &reqwest::Client,
    r: &str,
    etags: &mut HashMap<String, String>,
) -> Result<Vec<Item>> {
    let mut req = street_wallets::github(
        http,
        &format!("https://api.github.com/repos/{r}/events?per_page=20"),
    );
    if let Some(tag) = etags.get(r) {
        req = req.header("If-None-Match", tag);
    }
    let res = req.send().await?;
    if res.status() == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(Vec::new());
    }
    let res = res.error_for_status()?;
    if let Some(tag) = res.headers().get("etag").and_then(|v| v.to_str().ok()) {
        etags.insert(r.to_string(), tag.to_string());
    }
    let list: Vec<Value> = res.json().await?;
    let mut out = Vec::new();
    let mut branches = HashSet::new();
    for e in &list {
        let at = e["created_at"]
            .as_str()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&Utc));
        let Some(at) = at else { continue };
        let p = &e["payload"];
        match e["type"].as_str() {
            Some("PushEvent") => {
                if let Some(b) = p["ref"].as_str().and_then(|s| s.strip_prefix("refs/heads/")) {
                    branches.insert(b.to_string());
                }
            }
            Some("PullRequestEvent") => {
                let pr = &p["pull_request"];
                let merged = pr["merged"].as_bool().unwrap_or(false);
                let action = match (p["action"].as_str(), merged) {
                    (Some("opened"), _) => "opened",
                    (Some("closed"), true) => "merged",
                    _ => continue,
                };
                let n = pr["number"].as_u64().unwrap_or(0);
                out.push(Item {
                    uid: format!("p:{r}:{n}:{action}"),
                    kind: if action == "merged" { "merge" } else { "pull" },
                    repo: Some(r.to_string()),
                    actor: e["actor"]["login"].as_str().map(str::to_string),
                    avatar: e["actor"]["avatar_url"].as_str().map(str::to_string),
                    title: format!("#{n} {}", first_line(pr["title"].as_str().unwrap_or(""))),
                    url: pr["html_url"].as_str().map(str::to_string),
                    at,
                });
            }
            _ => {}
        }
    }
    // the default branch's commits come from its own feed every tick
    for b in branches.into_iter().filter(|b| b != "main" && b != "master") {
        if let Ok(list) = commits_atom(http, r, Some(&b)).await {
            out.extend(list.into_iter().map(|mut i| {
                i.title = format!("[{b}] {}", i.title);
                i
            }));
        }
    }
    Ok(out)
}

/// A repository's latest commits and releases in the wallet feed's shape, from the Atom feeds,
/// when GitHub's API says no (60 calls an hour without a token): no diffs, no pull requests, but
/// the boards aren't empty.
pub(crate) async fn atom_fallback(http: &reqwest::Client, r: &str) -> (Vec<Value>, Vec<Value>) {
    let commits = commits_atom(http, r, None)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|c| {
            let sha = c.uid.trim_start_matches("c:");
            json!({
                "repo": r, "sha": &sha[..sha.len().min(7)], "message": c.title,
                "author": c.actor, "login": c.actor, "avatar": c.avatar,
                "date": c.at, "url": c.url, "files": [],
            })
        })
        .collect();
    let releases = match atom(http, &format!("https://github.com/{r}/releases.atom")).await {
        Ok(feed) => feed
            .entries
            .into_iter()
            .take(6)
            .filter_map(|e| {
                let tag = e.id.rsplit('/').next()?.to_string();
                let notes: String = e
                    .content
                    .as_ref()
                    .and_then(|c| c.body.as_deref())
                    .map(strip_tags)
                    .unwrap_or_default()
                    .chars()
                    .take(1200)
                    .collect();
                Some(json!({
                    "repo": r, "tag": tag, "name": e.title.map(|t| t.content),
                    "date": e.updated.or(e.published)?, "url": e.links.first().map(|l| l.href.clone()),
                    "notes": notes,
                }))
            })
            .collect(),
        Err(_) => Vec::new(),
    };
    (commits, releases)
}

fn tweet(t: &Value) -> Option<Item> {
    let at = DateTime::parse_from_rfc3339(t["at"].as_str()?)
        .ok()?
        .with_timezone(&Utc);
    let text = t["text"].as_str().unwrap_or_default();
    Some(Item {
        uid: format!("x:{}", t["id"].as_str()?),
        kind: if t["rt_by"].is_string() {
            "retweet"
        } else {
            "tweet"
        },
        repo: None,
        actor: t["handle"].as_str().map(str::to_string),
        avatar: t["avatar"].as_str().map(str::to_string),
        title: text.chars().take(280).collect(),
        url: t["url"].as_str().map(str::to_string),
        at,
    })
}

fn strip_tags(html: &str) -> String {
    let mut out = String::new();
    let mut inside = false;
    for c in html.chars() {
        match c {
            '<' => inside = true,
            '>' => inside = false,
            _ if !inside => out.push(c),
            _ => {}
        }
    }
    out.replace("&#39;", "'")
        .replace("&quot;", "\"")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .trim()
        .to_string()
}

#[derive(Deserialize)]
pub struct PulseQuery {
    since: Option<i64>,
    wallet: Option<String>,
}

/// `GET /v1/street/wallets/pulse?since=<id>&wallet=<key>`: `{ last, items }`. Without `since`,
/// the newest id and the last few items (nothing to announce yet); with it, what came after,
/// oldest first.
pub async fn pulse(
    State(s): State<AppState>,
    Query(q): Query<PulseQuery>,
) -> Result<Json<Value>, ApiError> {
    type Row = (
        i64,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        Option<String>,
        DateTime<Utc>,
    );
    let rows: Vec<Row> = match q.since {
        Some(since) => {
            sqlx::query_as(
                "SELECT id, wallet, kind, repo, actor, avatar, title, url, at FROM street_wallet_pulse
                 WHERE id > $1 AND ($2::text IS NULL OR wallet = $2) ORDER BY id LIMIT 50",
            )
            .bind(since)
            .bind(&q.wallet)
            .fetch_all(&s.db.pool)
            .await?
        }
        None => {
            sqlx::query_as(
                "SELECT id, wallet, kind, repo, actor, avatar, title, url, at FROM street_wallet_pulse
                 WHERE ($1::text IS NULL OR wallet = $1) ORDER BY id DESC LIMIT 10",
            )
            .bind(&q.wallet)
            .fetch_all(&s.db.pool)
            .await?
        }
    };
    let last: Option<i64> = sqlx::query_scalar("SELECT max(id) FROM street_wallet_pulse")
        .fetch_one(&s.db.pool)
        .await?;
    let items: Vec<Value> = rows
        .into_iter()
        .map(|(id, wallet, kind, repo, actor, avatar, title, url, at)| {
            json!({ "id": id, "wallet": wallet, "kind": kind, "repo": repo, "actor": actor,
                    "avatar": avatar, "title": title, "url": url, "at": at })
        })
        .collect();
    Ok(Json(json!({ "last": last.unwrap_or(0), "items": items })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_commit_html() {
        assert_eq!(
            strip_tags("<pre style='x'>Fix &amp; ship: chip&#39;s USB</pre>"),
            "Fix & ship: chip's USB"
        );
    }

    #[test]
    fn reads_a_commit_feed() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:media="http://search.yahoo.com/mrss/">
  <id>tag:github.com,2008:/ZLabcash/zfirm/commits/main</id><title>t</title><updated>2026-10-12T00:57:21Z</updated>
  <entry>
    <id>tag:github.com,2008:Grit::Commit/d72c51e24c0c6d5a14999c3c6cb9646e5cceb192</id>
    <link type="text/html" rel="alternate" href="https://github.com/ZLabcash/zfirm/commit/d72c51e"/>
    <title>Wi-Fi board on any ESP32</title>
    <updated>2026-10-12T00:57:21Z</updated>
    <media:thumbnail height="30" width="30" url="https://avatars.githubusercontent.com/u/1?s=30&amp;v=4"/>
    <author><name>adamzkdev</name></author>
    <content type="html">&lt;pre&gt;Wi-Fi board on any ESP32 with Wi-Fi
second line&lt;/pre&gt;</content>
  </entry>
</feed>"#;
        let feed = feed_rs::parser::parse(xml.as_bytes()).unwrap();
        let e = &feed.entries[0];
        assert!(e.id.ends_with("d72c51e24c0c6d5a14999c3c6cb9646e5cceb192"));
        let body = strip_tags(e.content.as_ref().unwrap().body.as_deref().unwrap());
        assert_eq!(first_line(&body), "Wi-Fi board on any ESP32 with Wi-Fi");
        assert_eq!(e.authors[0].name.as_deref(), Some("adamzkdev"));
    }
}
