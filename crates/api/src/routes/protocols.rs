//! Protocols page: holdings and flows of every protocol and entity that touches ZEC, in the
//! spirit of DefiLlama. Shielded pools and the lockbox come from consensus; everything else from
//! labeled transparent addresses (see `/v1/labels`).

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use axum::{
    extract::{Path, Query, State},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use utoipa::{IntoParams, ToSchema};
use zscan_chain::labels as registry;

use super::labels::LabeledAddress;
use crate::error::{ApiError, ApiResult};
use crate::AppState;

/// How a protocol's holdings are measured.
#[derive(Debug, Clone)]
enum Source {
    /// A shielded value pool; the column prefix in `transactions` (`<pool>_vb`) and snapshots.
    Pool(&'static str),
    Lockbox,
    /// Labeled transparent addresses with this protocol slug.
    Addresses,
    /// Daily holdings reported by the bridge itself (`bridge_snapshots`), e.g. ZEC minted on NEAR.
    Snapshots,
}

#[derive(Debug, Clone)]
struct Proto {
    slug: String,
    name: String,
    category: String,
    website: Option<String>,
    description: Option<String>,
    source: Source,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ProtocolSummary {
    pub slug: String,
    pub name: String,
    /// shielded_pool | treasury | bridge | exchange | fund | mining_pool
    pub category: String,
    pub website: Option<String>,
    pub description: Option<String>,
    /// ZEC held now, zatoshis (node balances / consensus pool values).
    pub holdings: Option<i64>,
    pub change_1d: Option<i64>,
    pub change_7d: Option<i64>,
    pub change_30d: Option<i64>,
    pub inflow_7d: Option<i64>,
    pub outflow_7d: Option<i64>,
    /// Labeled addresses (0 for consensus pools).
    pub addresses: i64,
    /// End-of-day holdings for the last 30 days, oldest first.
    pub sparkline: Vec<i64>,
    /// Extra figures, from the protocol's own API or the index (e.g. mining share).
    #[schema(value_type = Object)]
    pub stats: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ProtocolDay {
    pub day: DateTime<Utc>,
    /// End-of-day holdings, zatoshis.
    pub holdings: i64,
    pub inflow: i64,
    pub outflow: i64,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct ProtocolTx {
    pub txid: String,
    pub height: i32,
    pub time: DateTime<Utc>,
    pub kind: String,
    /// Net change of the protocol's holdings in this transaction, zatoshis.
    pub delta: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProtocolDetail {
    pub protocol: ProtocolSummary,
    pub daily: Vec<ProtocolDay>,
    pub addresses: Vec<LabeledAddress>,
    pub recent_txs: Vec<ProtocolTx>,
    /// Flows are known from this height on; holdings before it are not reconstructed.
    pub indexed_from: Option<i32>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct DetailQuery {
    /// Days of history (default 90, max 365).
    pub days: Option<i32>,
}

/// Registry protocols plus every other protocol slug found in the labels (mining pools,
/// curated exchanges…).
async fn all_protocols(s: &AppState) -> anyhow::Result<Vec<Proto>> {
    let mut out: Vec<Proto> = registry::protocols()
        .iter()
        .map(|p| Proto {
            source: match p.slug.as_str() {
                "ironwood" => Source::Pool("ironwood"),
                "orchard" => Source::Pool("orchard"),
                "sapling" => Source::Pool("sapling"),
                "sprout" => Source::Pool("sprout"),
                "lockbox" => Source::Lockbox,
                "near-intents" => Source::Snapshots,
                _ => Source::Addresses,
            },
            slug: p.slug.clone(),
            name: p.name.clone(),
            category: p.category.clone(),
            website: p.website.clone(),
            description: p.description.clone(),
        })
        .collect();
    let dynamic: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT protocol, mode() WITHIN GROUP (ORDER BY name), mode() WITHIN GROUP (ORDER BY category)
         FROM labels WHERE protocol IS NOT NULL GROUP BY protocol",
    )
    .fetch_all(&s.db.pool)
    .await?;
    for (slug, name, category) in dynamic {
        if out.iter().any(|p| p.slug == slug) {
            continue;
        }
        out.push(Proto {
            slug,
            name,
            category,
            website: None,
            description: None,
            source: Source::Addresses,
        });
    }
    Ok(out)
}

/// Daily inflow/outflow over the indexed window (at most `days`), plus current holdings.
async fn series(
    s: &AppState,
    p: &Proto,
    days: i32,
) -> anyhow::Result<(Option<i64>, Vec<ProtocolDay>)> {
    // Continuous days from max(latest - days, first indexed day) to the latest block's day.
    const DAYS: &str = "bounds AS (
            SELECT date_trunc('day', max(time)) AS last,
                   GREATEST(date_trunc('day', max(time)) - make_interval(days => $1 - 1),
                            date_trunc('day', min(time))) AS first
            FROM blocks
         ), days AS (SELECT generate_series(first, last, interval '1 day') AS day FROM bounds)";
    let (flows, current): (String, Option<i64>) = match &p.source {
        Source::Pool(pool) => {
            let current: Option<i64> = sqlx::query_scalar(&format!(
                "SELECT {pool} FROM pool_snapshots ORDER BY height DESC LIMIT 1"
            ))
            .fetch_optional(&s.db.pool)
            .await?;
            (
                // A negative value balance moves value into the pool.
                format!(
                    "SELECT date_trunc('day', time) AS day,
                            sum(GREATEST(-{pool}_vb, 0))::bigint AS inflow,
                            sum(GREATEST({pool}_vb, 0))::bigint AS outflow
                     FROM transactions WHERE time >= (SELECT first FROM bounds) GROUP BY 1"
                ),
                current,
            )
        }
        Source::Lockbox => {
            let current: Option<i64> = sqlx::query_scalar(
                "SELECT lockbox FROM pool_snapshots ORDER BY height DESC LIMIT 1",
            )
            .fetch_optional(&s.db.pool)
            .await?;
            (
                "SELECT day, deferred AS inflow, 0::bigint AS outflow FROM daily_blocks
                 WHERE day >= (SELECT first FROM bounds)"
                    .to_string(),
                current,
            )
        }
        Source::Addresses => {
            let current: Option<i64> =
                sqlx::query_scalar("SELECT sum(balance)::bigint FROM labels WHERE protocol = $1")
                    .bind(&p.slug)
                    .fetch_one(&s.db.pool)
                    .await?;
            (
                // Net per transaction, so transfers between the protocol's own addresses cancel.
                "SELECT date_trunc('day', tr.time) AS day,
                        sum(GREATEST(t.d, 0))::bigint AS inflow,
                        sum(GREATEST(-t.d, 0))::bigint AS outflow
                 FROM (
                    SELECT txid, sum(v) AS d FROM (
                        SELECT o.txid, o.value AS v FROM tx_outputs o
                        JOIN labels l ON l.address = o.address AND l.protocol = $2
                        UNION ALL
                        SELECT i.txid, -COALESCE(i.value, 0) FROM tx_inputs i
                        JOIN labels l ON l.address = i.address AND l.protocol = $2
                    ) x GROUP BY txid
                 ) t JOIN transactions tr USING (txid)
                 WHERE tr.time >= (SELECT first FROM bounds) GROUP BY 1"
                    .to_string(),
                current,
            )
        }
        Source::Snapshots => {
            let current: Option<i64> = sqlx::query_scalar(
                "SELECT amount FROM bridge_snapshots WHERE protocol = $1 ORDER BY day DESC LIMIT 1",
            )
            .bind(&p.slug)
            .fetch_optional(&s.db.pool)
            .await?;
            (
                // Day-over-day change of the reported holdings.
                "SELECT (day::timestamp AT TIME ZONE 'UTC') AS day,
                        GREATEST(amount - prev, 0)::bigint AS inflow,
                        GREATEST(prev - amount, 0)::bigint AS outflow
                 FROM (SELECT day, amount, lag(amount) OVER (ORDER BY day) AS prev
                       FROM bridge_snapshots WHERE protocol = $2) x
                 WHERE prev IS NOT NULL"
                    .to_string(),
                current,
            )
        }
    };
    let sql = format!(
        "WITH {DAYS}, f AS ({flows})
         SELECT d.day, COALESCE(f.inflow, 0)::bigint, COALESCE(f.outflow, 0)::bigint
         FROM days d LEFT JOIN f USING (day) ORDER BY d.day"
    );
    let mut q = sqlx::query_as::<_, (DateTime<Utc>, i64, i64)>(&sql).bind(days);
    if matches!(p.source, Source::Addresses | Source::Snapshots) {
        q = q.bind(&p.slug);
    }
    let rows = q.fetch_all(&s.db.pool).await?;

    // Walk back from current holdings: end of day D = current − net flows after D.
    let mut holdings = current.unwrap_or(0);
    let mut out: Vec<ProtocolDay> = Vec::with_capacity(rows.len());
    for (day, inflow, outflow) in rows.into_iter().rev() {
        out.push(ProtocolDay {
            day,
            holdings,
            inflow,
            outflow,
        });
        holdings -= inflow - outflow;
    }
    out.reverse();
    Ok((current, out))
}

fn summarize(
    p: &Proto,
    current: Option<i64>,
    daily: &[ProtocolDay],
    addresses: i64,
) -> ProtocolSummary {
    let n = daily.len();
    let ago = |d: usize| {
        current
            .zip((n > d).then(|| daily[n - 1 - d].holdings))
            .map(|(c, h)| c - h)
    };
    let last7 = &daily[n.saturating_sub(7)..];
    ProtocolSummary {
        slug: p.slug.clone(),
        name: p.name.clone(),
        category: p.category.clone(),
        website: p.website.clone(),
        description: p.description.clone(),
        holdings: current,
        change_1d: ago(1),
        change_7d: ago(7),
        change_30d: ago(30),
        inflow_7d: Some(last7.iter().map(|d| d.inflow).sum()),
        // Lockbox disbursements aren't tracked yet.
        outflow_7d: (!matches!(p.source, Source::Lockbox))
            .then(|| last7.iter().map(|d| d.outflow).sum()),
        addresses,
        sparkline: daily[n.saturating_sub(30)..]
            .iter()
            .map(|d| d.holdings)
            .collect(),
        stats: None,
    }
}

/// Per-protocol extras: API stats (bridges) and mining share (pools). Cached for a minute:
/// the mining share scans a month of blocks.
async fn stats(s: &AppState) -> anyhow::Result<HashMap<String, serde_json::Value>> {
    type Stats = Option<(Instant, HashMap<String, serde_json::Value>)>;
    static CACHE: OnceLock<Mutex<Stats>> = OnceLock::new();
    let mut cache = CACHE.get_or_init(|| Mutex::new(None)).lock().await;
    if let Some((at, m)) = cache.as_ref() {
        if at.elapsed() < Duration::from_secs(60) {
            return Ok(m.clone());
        }
    }
    let mut out: HashMap<String, serde_json::Value> =
        sqlx::query_as::<_, (String, serde_json::Value)>("SELECT slug, data FROM protocol_stats")
            .fetch_all(&s.db.pool)
            .await?
            .into_iter()
            .collect();
    // Untagged blocks count for the pool known to use the same payout address. Blocks without
    // a known pool count toward the total but get no entry.
    let mining: Vec<(Option<String>, i64, i64)> = sqlx::query_as(
        "WITH recent AS (
            SELECT miner, miner_pool FROM blocks
            WHERE time > (SELECT max(time) FROM blocks) - interval '30 days'
         ), payout AS (
            SELECT miner, mode() WITHIN GROUP (ORDER BY miner_pool) AS pool FROM blocks
            WHERE miner_pool IS NOT NULL AND miner IN (SELECT miner FROM recent) GROUP BY miner
         )
         SELECT COALESCE(r.miner_pool, p.pool), count(*), sum(count(*)) OVER ()::bigint
         FROM recent r LEFT JOIN payout p USING (miner) GROUP BY 1",
    )
    .fetch_all(&s.db.pool)
    .await?;
    for (pool, blocks, total) in mining {
        let Some(pool) = pool else { continue };
        out.entry(registry::slug(&pool)).or_insert_with(|| {
            serde_json::json!({
                "blocks_30d": blocks,
                "share_30d": if total > 0 { blocks as f64 / total as f64 } else { 0.0 },
            })
        });
    }
    *cache = Some((Instant::now(), out.clone()));
    Ok(out)
}

async fn address_counts(s: &AppState) -> anyhow::Result<HashMap<String, i64>> {
    Ok(sqlx::query_as::<_, (String, i64)>(
        "SELECT protocol, count(*) FROM labels WHERE protocol IS NOT NULL GROUP BY protocol",
    )
    .fetch_all(&s.db.pool)
    .await?
    .into_iter()
    .collect())
}

type Cached = Option<(Instant, Vec<ProtocolSummary>)>;

/// All protocols ranked by holdings, with 30-day sparklines and 1d/7d/30d changes.
#[utoipa::path(get, path = "/v1/protocols",
    responses((status = 200, body = [ProtocolSummary])), tag = "protocols")]
pub async fn list(State(s): State<AppState>) -> ApiResult<Vec<ProtocolSummary>> {
    static CACHE: OnceLock<Mutex<Cached>> = OnceLock::new();
    let mut cache = CACHE.get_or_init(|| Mutex::new(None)).lock().await;
    if let Some((at, rows)) = cache.as_ref() {
        if at.elapsed() < Duration::from_secs(60) {
            return Ok(Json(rows.clone()));
        }
    }
    let protos = all_protocols(&s).await?;
    let counts = address_counts(&s).await?;
    let mut extra = stats(&s).await?;
    let mut rows = Vec::with_capacity(protos.len());
    for p in &protos {
        let (current, daily) = series(&s, p, 31).await?;
        let mut sum = summarize(
            p,
            current,
            &daily,
            counts.get(&p.slug).copied().unwrap_or(0),
        );
        sum.stats = extra.remove(&p.slug);
        rows.push(sum);
    }
    rows.sort_by_key(|r| std::cmp::Reverse(r.holdings.unwrap_or(0)));
    *cache = Some((Instant::now(), rows.clone()));
    Ok(Json(rows))
}

/// One protocol: daily holdings and flows, labeled addresses and recent transactions.
#[utoipa::path(get, path = "/v1/protocols/{slug}", params(("slug" = String, Path), DetailQuery),
    responses((status = 200, body = ProtocolDetail), (status = 404)), tag = "protocols")]
pub async fn detail(
    State(s): State<AppState>,
    Path(slug): Path<String>,
    Query(q): Query<DetailQuery>,
) -> ApiResult<ProtocolDetail> {
    let p = all_protocols(&s)
        .await?
        .into_iter()
        .find(|p| p.slug == slug)
        .ok_or_else(|| ApiError::NotFound(format!("No protocol `{slug}`")))?;
    let (current, daily) = series(&s, &p, q.days.unwrap_or(90).clamp(1, 365)).await?;
    let addresses = sqlx::query_as::<_, LabeledAddress>(
        "SELECT address, name, category, protocol, source, origin, active, balance, updated_at
         FROM labels WHERE protocol = $1 ORDER BY active DESC, balance DESC NULLS LAST",
    )
    .bind(&slug)
    .fetch_all(&s.db.pool)
    .await?;
    let recent_txs = if matches!(p.source, Source::Addresses) {
        sqlx::query_as::<_, ProtocolTx>(
            "SELECT tr.txid, tr.height, tr.time, tr.kind, t.d AS delta FROM (
                SELECT txid, sum(v)::bigint AS d FROM (
                    SELECT o.txid, o.value AS v FROM tx_outputs o
                    JOIN labels l ON l.address = o.address AND l.protocol = $1
                    UNION ALL
                    SELECT i.txid, -COALESCE(i.value, 0) FROM tx_inputs i
                    JOIN labels l ON l.address = i.address AND l.protocol = $1
                ) x GROUP BY txid
             ) t JOIN transactions tr USING (txid)
             ORDER BY tr.height DESC, tr.block_index DESC LIMIT 25",
        )
        .bind(&slug)
        .fetch_all(&s.db.pool)
        .await?
    } else {
        Vec::new()
    };
    let mut summary = summarize(&p, current, &daily, addresses.len() as i64);
    summary.stats = stats(&s).await?.remove(&slug);
    let indexed_from: Option<i32> =
        sqlx::query_scalar("SELECT GREATEST(low, tx_low) FROM sync_state")
            .fetch_one(&s.db.pool)
            .await?;
    Ok(Json(ProtocolDetail {
        protocol: summary,
        daily,
        addresses,
        recent_txs,
        indexed_from,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_changes_from_daily_holdings() {
        let day = |h, i, o| ProtocolDay {
            day: Utc::now(),
            holdings: h,
            inflow: i,
            outflow: o,
        };
        let p = Proto {
            slug: "x".into(),
            name: "X".into(),
            category: "bridge".into(),
            website: None,
            description: None,
            source: Source::Addresses,
        };
        let daily = vec![day(100, 0, 0), day(150, 60, 10), day(120, 0, 30)];
        let s = summarize(&p, Some(120), &daily, 2);
        assert_eq!(s.change_1d, Some(-30));
        assert_eq!(s.change_7d, None);
        assert_eq!(s.inflow_7d, Some(60));
        assert_eq!(s.outflow_7d, Some(40));
        assert_eq!(s.sparkline, vec![100, 150, 120]);
    }
}
