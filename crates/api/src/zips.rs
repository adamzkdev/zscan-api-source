//! Zcash Improvement Proposals, read from github.com/zcash/zips (the source of zips.z.cash) every six
//! hours and kept in memory. The preamble of each file (`ZIP:`, `Title:`, `Status:` …) reads the same
//! in .rst and .md; the first paragraph of the Abstract gives a short summary. Served by `/v1/zips`.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

const REPO: &str = "zcash/zips";
const REFRESH: Duration = Duration::from_secs(6 * 3600);
const RETRY: Duration = Duration::from_secs(600);
const ABSTRACT_MAX: usize = 420;

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Zip {
    /// File name without extension: `zip-0317`, `draft-arya-deploy-nu7`. Also the zips.z.cash path.
    pub slug: String,
    /// ZIP number, null for drafts that have none yet.
    pub number: Option<u32>,
    pub title: String,
    /// First status in the preamble, e.g. `Final` for "[Revision 0: Canopy, Revision 1: NU6] Final, …".
    pub status: String,
    /// The full status when revisions differ.
    pub status_detail: Option<String>,
    /// Process, Standards, Informational, Consensus, Network, Wallet …
    pub categories: Vec<String>,
    pub owners: Vec<String>,
    pub created: Option<String>,
    /// First paragraph of the Abstract, plain text, shortened.
    pub summary: Option<String>,
    pub url: String,
    /// The ZScan page where this ZIP can be watched at work on chain.
    pub live: Option<Live>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Live {
    pub href: String,
    pub label: String,
}

fn store() -> &'static RwLock<Arc<Vec<Zip>>> {
    static S: OnceLock<RwLock<Arc<Vec<Zip>>>> = OnceLock::new();
    S.get_or_init(|| RwLock::new(Arc::new(Vec::new())))
}

/// Every ZIP loaded so far (empty until the first refresh has finished).
pub fn all() -> Arc<Vec<Zip>> {
    store().read().expect("zips lock").clone()
}

fn live_pages() -> &'static HashMap<u32, Live> {
    static L: OnceLock<HashMap<u32, Live>> = OnceLock::new();
    L.get_or_init(|| {
        let raw: HashMap<String, Live> = serde_json::from_str(include_str!("../../../labels/zips_live.json")).expect("labels/zips_live.json is valid");
        raw.into_iter().filter_map(|(k, v)| Some((k.parse().ok()?, v))).collect()
    })
}

pub async fn run(http: reqwest::Client) {
    loop {
        let wait = match load(&http).await {
            Ok(zips) => {
                tracing::info!(count = zips.len(), "zips refreshed");
                *store().write().expect("zips lock") = Arc::new(zips);
                REFRESH
            }
            Err(e) => {
                tracing::warn!("zips refresh: {e:#}");
                RETRY
            }
        };
        tokio::time::sleep(wait).await;
    }
}

#[derive(Deserialize)]
struct Entry {
    name: String,
}

async fn load(http: &reqwest::Client) -> Result<Vec<Zip>> {
    let mut req = http
        .get(format!("https://api.github.com/repos/{REPO}/contents/zips"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "zscan");
    if let Ok(token) = std::env::var("GITHUB_TOKEN") {
        req = req.bearer_auth(token);
    }
    let listing: Vec<Entry> = req.send().await?.error_for_status().context("GitHub listing")?.json().await?;
    let files: Vec<String> = listing
        .into_iter()
        .map(|e| e.name)
        .filter(|n| (n.starts_with("zip-") || n.starts_with("draft-")) && (n.ends_with(".md") || n.ends_with(".rst")))
        .collect();
    let mut zips = Vec::new();
    // raw files are not rate limited like the API; 16 at a time keeps a refresh quick
    for chunk in files.chunks(16) {
        let texts = join_all(chunk.iter().map(|name| async move {
            let url = format!("https://raw.githubusercontent.com/{REPO}/main/zips/{name}");
            let text = match http.get(&url).timeout(Duration::from_secs(15)).send().await {
                Ok(r) if r.status().is_success() => r.text().await.unwrap_or_default(),
                _ => String::new(),
            };
            (name, text)
        }))
        .await;
        for (name, text) in texts {
            let slug = name.trim_end_matches(".md").trim_end_matches(".rst");
            if let Some(z) = to_zip(slug, &text) {
                zips.push(z);
            }
        }
    }
    anyhow::ensure!(!zips.is_empty(), "no ZIP could be read");
    zips.sort_by(|a, b| a.number.unwrap_or(u32::MAX).cmp(&b.number.unwrap_or(u32::MAX)).then_with(|| a.slug.cmp(&b.slug)));
    Ok(zips)
}

/// The `Key: value` preamble; indented lines without a key continue the previous value.
fn preamble(text: &str) -> HashMap<String, Vec<String>> {
    let mut out: HashMap<String, Vec<String>> = HashMap::new();
    let lines: Vec<&str> = text.lines().collect();
    let Some(start) = lines.iter().position(|l| l.trim_start().starts_with("ZIP:")) else {
        return out;
    };
    let mut key: Option<String> = None;
    for line in &lines[start..] {
        if line.trim().is_empty() {
            break;
        }
        let t = line.trim();
        let kv = t.split_once(':').filter(|(k, _)| !k.is_empty() && k.chars().next().is_some_and(|c| c.is_ascii_alphabetic()) && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
        match kv {
            Some((k, v)) => {
                let v = v.trim();
                out.insert(k.to_string(), if v.is_empty() { vec![] } else { vec![v.to_string()] });
                key = Some(k.to_string());
            }
            _ => {
                if let Some(k) = &key {
                    out.entry(k.clone()).or_default().push(t.to_string());
                }
            }
        }
    }
    out
}

/// First paragraph under the "Abstract" heading (rst underline or markdown #), as plain text.
fn summary(text: &str) -> Option<String> {
    let lines: Vec<&str> = text.lines().collect();
    let at = lines.iter().position(|l| l.trim().trim_start_matches('#').trim() == "Abstract")?;
    let mut para: Vec<String> = Vec::new();
    for l in &lines[at + 1..] {
        let t = l.trim();
        let rule = !t.is_empty() && t.chars().all(|c| "=-~^*".contains(c));
        if rule || (t.is_empty() && para.is_empty()) {
            continue;
        }
        if t.is_empty() || t.starts_with('#') || t.starts_with(".. ") {
            break;
        }
        para.push(t.to_string());
    }
    if para.is_empty() {
        return None;
    }
    // inline maths ($\mathsf{MAX\_MONEY}$) reads as its name
    let joined = para.join(" ").replace("\\_", "_").replace("\\mathsf{", "").replace("\\mathrm{", "").replace("\\text{", "").replace(['$', '{', '}'], "");
    let mut s = plain(&joined);
    if s.chars().count() > ABSTRACT_MAX {
        let cut: String = s.chars().take(ABSTRACT_MAX).collect();
        s = format!("{}…", cut.rsplit_once(' ').map_or(cut.as_str(), |(a, _)| a));
    }
    Some(s)
}

/// Drop rst/markdown markup: `links <url>`_, **bold**, ``code``, [text](url), :role:`x`.
fn plain(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            // <url> inside an rst link: skip it
            '<' => {
                for d in chars.by_ref() {
                    if d == '>' {
                        break;
                    }
                }
            }
            // (url) right after a markdown link's ]
            '(' if out.ends_with(']') => {
                out.pop();
                for d in chars.by_ref() {
                    if d == ')' {
                        break;
                    }
                }
            }
            '`' | '*' | '[' => {}
            ']' => out.push(']'),
            '_' if matches!(chars.peek(), None | Some(' ' | '.' | ',' | ';' | ')')) && out.ends_with(|ch: char| ch.is_alphanumeric() || ch == ' ') => {}
            _ => out.push(c),
        }
    }
    // citation references (`#bip-0032_`, `^zip-0229`) go, their trailing punctuation stays
    let mut words: Vec<String> = Vec::new();
    for w in out.replace(']', "").split_whitespace() {
        let core = w.trim_end_matches(|c| ",.;:)".contains(c));
        if core.starts_with('^') || core.starts_with('#') {
            if let Some(prev) = words.last_mut() {
                prev.push_str(&w[core.len()..]);
            }
        } else {
            words.push(w.to_string());
        }
    }
    words.join(" ")
}

fn to_zip(slug: &str, text: &str) -> Option<Zip> {
    let p = preamble(text);
    let title = p.get("Title")?.join(" ");
    // templates and guides ("{Something Short and To the Point}") are not proposals
    if title.is_empty() || title.starts_with('{') {
        return None;
    }
    let raw = p.get("Status").map(|v| v.join(" ")).unwrap_or_else(|| "Unknown".into());
    // the first capitalised word, after the first "[Revision …]" prefix if any
    let after = raw.split_once(']').map_or(raw.as_str(), |(_, b)| b).trim();
    let status = after.split(|c: char| !c.is_ascii_alphabetic()).find(|w| w.chars().next().is_some_and(|c| c.is_ascii_uppercase())).unwrap_or("Unknown").to_string();
    let number = slug.strip_prefix("zip-").and_then(|n| n.parse::<u32>().ok());
    let categories = p
        .get("Category")
        .map(|v| v.join(" "))
        .unwrap_or_default()
        .split('/')
        .map(|c| match c.trim() {
            "Consensus Process" => "Process".to_string(),
            "Standards Track" => "Standards".to_string(),
            o => o.to_string(),
        })
        .filter(|c| !c.is_empty())
        .collect();
    let owners = p
        .get("Owners")
        .map(|v| v.iter().map(|o| o.split('<').next().unwrap_or("").trim().trim_end_matches(',').to_string()).filter(|o| !o.is_empty()).collect())
        .unwrap_or_default();
    Some(Zip {
        slug: slug.to_string(),
        number,
        title,
        status,
        status_detail: raw.contains('[').then_some(raw.clone()),
        categories,
        owners,
        created: p.get("Created").and_then(|v| v.first().cloned()),
        summary: summary(text),
        url: format!("https://zips.z.cash/{slug}"),
        live: number.and_then(|n| live_pages().get(&n).cloned()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const RST: &str = "::\n\n  ZIP: 317\n  Title: Proportional Transfer Fee Mechanism\n  Owners: Aditya Bharadwaj <nighthawk24@gmail.com>\n          Daira-Emma Hopwood <daira@jacaranda.org>\n  Status: [Revision 0] Active, [Revision 1] Final\n  Category: Standards / Wallet\n  Created: 2022-08-15\n\n\nAbstract\n========\n\nThe goal of this ZIP is to change the conventional fees for transactions\nby making them `proportional <https://example.org>`_ to the number of **outputs** [#zip-0244]_.\n\nMotivation\n==========\n";

    #[test]
    fn parses_rst() {
        let z = to_zip("zip-0317", RST).unwrap();
        assert_eq!(z.number, Some(317));
        assert_eq!(z.title, "Proportional Transfer Fee Mechanism");
        assert_eq!(z.status, "Active");
        assert_eq!(z.categories, vec!["Standards", "Wallet"]);
        assert_eq!(z.owners, vec!["Aditya Bharadwaj", "Daira-Emma Hopwood"]);
        assert_eq!(z.created.as_deref(), Some("2022-08-15"));
        assert_eq!(
            z.summary.as_deref(),
            Some("The goal of this ZIP is to change the conventional fees for transactions by making them proportional to the number of outputs.")
        );
    }

    #[test]
    fn parses_md() {
        let md = "    ZIP: Unassigned\n    Title: Deploy NU7\n    Status: Draft\n    Category: Consensus / Network\n\n# Abstract\n\nThis proposal [defines](https://x.y) the NU7 upgrade.\n\n# Motivation\n";
        let z = to_zip("draft-arya-deploy-nu7", md).unwrap();
        assert_eq!(z.number, None);
        assert_eq!(z.status, "Draft");
        assert_eq!(z.summary.as_deref(), Some("This proposal defines the NU7 upgrade."));
    }

    /// `cargo test -p zscan-api zips::tests::live -- --ignored --nocapture`: every ZIP, as JSON.
    #[tokio::test]
    #[ignore]
    async fn live() {
        let zips = load(&reqwest::Client::new()).await.unwrap();
        println!("ZIPS_JSON {}", serde_json::to_string(&zips).unwrap());
    }

    #[test]
    fn skips_templates() {
        assert!(to_zip("zip-guide", "  ZIP: XXX\n  Title: {Something Short and To the Point}\n").is_none());
    }
}
