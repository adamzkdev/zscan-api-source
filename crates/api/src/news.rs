//! Zcash news: the curated feeds of `labels/news_sources.json` (blogs, forum, digests, releases),
//! polled in the background, plus network events ZScan detects in its own daily rollup (records
//! and activity spikes). Served by `/v1/news`.

use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;

const POLL_EVERY: Duration = Duration::from_secs(600);
const SUMMARY_MAX: usize = 280;

#[derive(Debug, Clone, Deserialize)]
pub struct Source {
    pub id: String,
    pub name: String,
    pub category: String,
    pub homepage: String,
    pub feed: String,
    /// Prepended to entry titles that don't name the project (release tags like "0.10.1").
    #[serde(default)]
    pub title_prefix: String,
    /// Entries whose title contains any of these are skipped (release candidates, crate tags).
    #[serde(default)]
    pub exclude: Vec<String>,
}

#[derive(Deserialize)]
struct SourcesFile {
    sources: Vec<Source>,
}

pub fn sources() -> &'static [Source] {
    static S: OnceLock<Vec<Source>> = OnceLock::new();
    S.get_or_init(|| {
        serde_json::from_str::<SourcesFile>(include_str!("../../../labels/news_sources.json"))
            .expect("labels/news_sources.json is valid")
            .sources
    })
}

/// Name of a source id, including ZScan's own network events.
pub fn source_name(id: &str) -> &str {
    if id == "zscan" {
        return "ZScan";
    }
    sources()
        .iter()
        .find(|s| s.id == id)
        .map_or(id, |s| s.name.as_str())
}

pub async fn run(db: PgPool) {
    let http = zscan_chain::http_client_builder()
        .timeout(Duration::from_secs(20))
        .user_agent("ZScan/1.0 (+https://zscan.cash)")
        .build()
        .expect("http client");
    loop {
        for s in sources() {
            match poll(&db, &http, s).await {
                Ok(n) if n > 0 => tracing::info!(source = %s.id, new = n, "news items"),
                Ok(_) => {}
                Err(e) => tracing::warn!(source = %s.id, "news feed: {e:#}"),
            }
        }
        if let Err(e) = network_events(&db).await {
            tracing::warn!("news network events: {e:#}");
        }
        tokio::time::sleep(POLL_EVERY).await;
    }
}

struct Item {
    id: String,
    title: String,
    url: String,
    summary: Option<String>,
    published: DateTime<Utc>,
}

async fn poll(db: &PgPool, http: &reqwest::Client, s: &Source) -> Result<u64> {
    let body = http
        .get(&s.feed)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    let feed = feed_rs::parser::parse(&body[..]).context("parse feed")?;
    let mut new = 0;
    for item in feed.entries.into_iter().filter_map(|e| entry(s, e)) {
        new += sqlx::query(
            "INSERT INTO news_items (id, source, category, title, url, summary, published_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING",
        )
        .bind(&item.id)
        .bind(&s.id)
        .bind(&s.category)
        .bind(&item.title)
        .bind(&item.url)
        .bind(&item.summary)
        .bind(item.published)
        .execute(db)
        .await?
        .rows_affected();
    }
    Ok(new)
}

fn entry(s: &Source, e: feed_rs::model::Entry) -> Option<Item> {
    let raw_title = clean(&e.title?.content);
    if raw_title.is_empty() || s.exclude.iter().any(|x| raw_title.contains(x.as_str())) {
        return None;
    }
    let url = e
        .links
        .iter()
        .find(|l| l.rel.as_deref().is_none_or(|r| r == "alternate"))
        .or(e.links.first())?
        .href
        .clone();
    let published = e.published.or(e.updated)?;
    let summary = e
        .summary
        .map(|t| t.content)
        .or_else(|| e.content.and_then(|c| c.body))
        .map(|h| excerpt(&clean(&h)))
        .filter(|t| !t.is_empty() && *t != raw_title);
    Some(Item {
        id: format!("{}:{url}", s.id),
        title: format!("{}{raw_title}", s.title_prefix),
        url,
        summary,
        published,
    })
}

/// Plain text from feed HTML: tags dropped, common entities decoded, whitespace collapsed.
fn clean(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    for c in html.chars() {
        match c {
            '<' => {
                in_tag = true;
                out.push(' ');
            }
            '>' if in_tag => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    let out = out
        .replace("[&#8230;]", "…")
        .replace("[…]", "…")
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&#8217;", "’")
        .replace("&#8216;", "‘")
        .replace("&#8220;", "“")
        .replace("&#8221;", "”")
        .replace("&#8211;", "–")
        .replace("&#8212;", "—")
        .replace("&#8230;", "…");
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// First sentences up to `SUMMARY_MAX` characters, cut at a word.
fn excerpt(text: &str) -> String {
    if text.chars().count() <= SUMMARY_MAX {
        return text.to_string();
    }
    let cut: String = text.chars().take(SUMMARY_MAX).collect();
    let end = cut.rfind(' ').unwrap_or(cut.len());
    format!(
        "{}…",
        cut[..end].trim_end_matches([',', ';', ':', '.', ' '])
    )
}

/// A daily figure worth a headline when it sets a record or jumps far above its weekly average.
struct Signal {
    metric: &'static str,
    /// SQL over `daily_stats` columns (constant, not user input).
    expr: &'static str,
    unit: &'static str,
    record: &'static str,
    /// Spike threshold (× the previous 7-day average); none for shares.
    spike: Option<f64>,
    spike_title: &'static str,
}

const SIGNALS: &[Signal] = &[
    Signal {
        metric: "transactions",
        expr: "txs::float8",
        unit: "int",
        record: "New high: {v} Zcash transactions in a day",
        spike: Some(2.0),
        spike_title: "Transaction spike: {v} transactions, {x}× the weekly average",
    },
    Signal {
        metric: "shielded-share",
        expr: "(txs - transparent)::float8 / NULLIF(txs, 0)",
        unit: "pct",
        record: "New high: {v} of Zcash transactions used a shielded pool",
        spike: None,
        spike_title: "",
    },
    Signal {
        metric: "fully-shielded-share",
        expr: "shielded::float8 / NULLIF(txs, 0)",
        unit: "pct",
        record: "New high: {v} of Zcash transactions stayed fully shielded",
        spike: None,
        spike_title: "",
    },
    Signal {
        metric: "shielding-flows",
        expr: "shielded_in::float8 / 1e8",
        unit: "zec",
        record: "New high: {v} moved into Zcash's shielded pools in a day",
        spike: Some(3.0),
        spike_title: "Shielding surge: {v} moved into the shielded pools, {x}× the weekly average",
    },
    Signal {
        metric: "shielded-actions",
        expr: "(sapling_spends + sapling_outputs + orchard_actions + ironwood_actions)::float8",
        unit: "int",
        record: "New high: {v} shielded actions in a day",
        spike: None,
        spike_title: "",
    },
];

/// Needs this many earlier complete days before a record or spike means anything.
const WARMUP_DAYS: i64 = 30;
const STATS_URL: &str = "https://stats.zscan.cash";

fn fmt_value(v: f64, unit: &str) -> String {
    match unit {
        "pct" => format!("{:.1}%", v * 100.0),
        "zec" => format!("{} ZEC", thousands(v.round() as i64)),
        _ => thousands(v.round() as i64),
    }
}

fn thousands(n: i64) -> String {
    let s = n.abs().to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    if n < 0 {
        format!("-{out}")
    } else {
        out
    }
}

/// Stores records and spikes of complete days as news items (idempotent, keyed by day).
async fn network_events(db: &PgPool) -> Result<()> {
    let Some(first): Option<DateTime<Utc>> = sqlx::query_scalar("SELECT min(day) FROM daily_stats")
        .fetch_one(db)
        .await?
    else {
        return Ok(());
    };
    let since = first.format("%b %-d, %Y").to_string();
    let mut ids = Vec::new();
    for s in SIGNALS {
        let rows: Vec<(DateTime<Utc>, f64, Option<f64>, Option<f64>)> = sqlx::query_as(&format!(
            "WITH d AS (
                SELECT day, {} AS v FROM daily_stats WHERE day < date_trunc('day', now())
             ), w AS (
                SELECT day, v,
                       max(v) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) prev_max,
                       avg(v) OVER (ORDER BY day ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) avg7,
                       row_number() OVER (ORDER BY day) n
                FROM d WHERE v IS NOT NULL
             )
             SELECT day, v, prev_max, avg7 FROM w WHERE n > $1",
            s.expr
        ))
        .bind(WARMUP_DAYS)
        .fetch_all(db)
        .await?;
        for (day, v, prev_max, avg7) in rows {
            let ratio = avg7.filter(|a| *a > 0.0).map(|a| v / a);
            let (kind, title) = if prev_max.is_some_and(|m| v > m) {
                ("record", s.record.replace("{v}", &fmt_value(v, s.unit)))
            } else if let (Some(k), Some(r)) = (s.spike, ratio) {
                if r < k {
                    continue;
                }
                (
                    "spike",
                    s.spike_title
                        .replace("{v}", &fmt_value(v, s.unit))
                        .replace("{x}", &format!("{r:.1}")),
                )
            } else {
                continue;
            };
            let date = day.format("%Y-%m-%d").to_string();
            let id = format!("zscan:{kind}:{}:{date}", s.metric);
            let summary = match (kind, prev_max, avg7) {
                ("record", Some(m), _) => format!(
                    "{} on {}: the highest since ZScan's daily history began on {since} (previous high {}).",
                    fmt_value(v, s.unit),
                    day.format("%b %-d, %Y"),
                    fmt_value(m, s.unit)
                ),
                (_, _, Some(a)) => format!(
                    "{} on {} against a 7-day average of {}.",
                    fmt_value(v, s.unit),
                    day.format("%b %-d, %Y"),
                    fmt_value(a, s.unit)
                ),
                _ => String::new(),
            };
            sqlx::query(
                "INSERT INTO news_items (id, source, category, title, url, summary, published_at, data)
                 VALUES ($1, 'zscan', 'network', $2, $3, $4, $5, $6)
                 ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, summary = EXCLUDED.summary,
                    data = EXCLUDED.data",
            )
            .bind(&id)
            .bind(&title)
            .bind(format!("{STATS_URL}/{}#{date}", s.metric))
            .bind(&summary)
            .bind(day + chrono::Duration::days(1))
            .bind(json!({ "kind": kind, "metric": s.metric, "day": date, "value": v,
                          "previous_high": prev_max, "avg7": avg7 }))
            .execute(db)
            .await?;
            ids.push(id);
        }
    }
    // Events that no longer qualify (rules changed, history re-seeded) are withdrawn.
    sqlx::query("DELETE FROM news_items WHERE source = 'zscan' AND NOT (id = ANY($1))")
        .bind(&ids)
        .execute(db)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sources_parse() {
        assert!(sources().len() >= 5);
        assert!(sources().iter().all(|s| s.feed.starts_with("https://")));
    }

    #[test]
    fn cleans_feed_html() {
        assert_eq!(
            clean("<p>Zebra&nbsp;6.4.2 &amp; <b>more</b> [&#8230;]</p>"),
            "Zebra 6.4.2 & more …"
        );
        let long = "word ".repeat(100);
        let e = excerpt(&long);
        assert!(e.chars().count() <= SUMMARY_MAX + 1 && e.ends_with('…'));
    }

    #[test]
    fn formats_values() {
        assert_eq!(fmt_value(35145.0, "int"), "35,145");
        assert_eq!(fmt_value(0.4771, "pct"), "47.7%");
        assert_eq!(fmt_value(156952.4, "zec"), "156,952 ZEC");
    }
}
