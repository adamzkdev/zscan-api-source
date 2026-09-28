use axum::{
    extract::{Query, State},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use zscan_chain::consts::nu6_3_height;

use crate::error::ApiResult;
use crate::AppState;

#[derive(Debug, Serialize, ToSchema)]
pub struct Status {
    pub chain_tip: u32,
    pub indexed_high: Option<i32>,
    pub indexed_low: Option<i32>,
    pub target_low: u32,
    /// Backfill progress, 0–1.
    pub backfill_progress: f64,
    /// Lowest height whose transactions are stored, when older ones are pruned
    /// (TX_RETENTION_DAYS); blocks below it keep their header-level data.
    pub transactions_from: Option<i32>,
}

/// Indexer status.
#[utoipa::path(get, path = "/v1/status", responses((status = 200, body = Status)), tag = "network")]
pub async fn status(State(s): State<AppState>) -> ApiResult<Status> {
    let (low, high, target, tx_low): (Option<i32>, Option<i32>, Option<i32>, Option<i32>) =
        sqlx::query_as("SELECT low, high, target, tx_low FROM sync_state")
            .fetch_one(&s.db.pool)
            .await?;
    let target = target.unwrap_or(nu6_3_height() as i32);
    // The index tip is a fine fallback if lightwalletd is briefly unreachable.
    let tip = match s.lwd.tip().await {
        Ok((t, _)) => t,
        Err(_) => high.unwrap_or(0) as u32,
    };
    let progress = match (low, high) {
        (Some(l), Some(h)) if h > target => {
            ((h - l.max(target)) as f64 / (h - target) as f64).clamp(0.0, 1.0)
        }
        _ => 0.0,
    };
    Ok(Json(Status {
        chain_tip: tip,
        indexed_high: high,
        indexed_low: low,
        target_low: target as u32,
        backfill_progress: progress,
        transactions_from: tx_low.max(low),
    }))
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct MempoolTx {
    pub txid: String,
    pub seen_at: DateTime<Utc>,
    pub size: i32,
    pub kind: String,
    pub fee: Option<i64>,
    pub transparent_out: i64,
    /// ZIP-317 logical actions (the conventional fee is 5000 zat × max(2, n_actions)).
    pub n_actions: i32,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Mempool {
    pub count: usize,
    pub total_size: i64,
    pub total_fees: i64,
    pub txs: Vec<MempoolTx>,
}

/// Unconfirmed transactions.
#[utoipa::path(get, path = "/v1/mempool", responses((status = 200, body = Mempool)), tag = "network")]
pub async fn mempool(State(s): State<AppState>) -> ApiResult<Mempool> {
    let txs = sqlx::query_as::<_, MempoolTx>(
        "SELECT m.* FROM mempool m
         WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.txid = m.txid)
         ORDER BY seen_at DESC LIMIT 500",
    )
    .fetch_all(&s.db.pool)
    .await?;
    Ok(Json(Mempool {
        count: txs.len(),
        total_size: txs.iter().map(|t| t.size as i64).sum(),
        total_fees: txs.iter().filter_map(|t| t.fee).sum(),
        txs,
    }))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Network {
    pub height: Option<i32>,
    pub difficulty: Option<f64>,
    /// Estimated network solutions per second.
    pub solps: Option<f64>,
    /// Average block interval over the last 100 blocks, seconds.
    pub avg_block_time: Option<f64>,
    /// Consensus target block interval at the tip, seconds (75, or 25 after NU7).
    pub target_block_time: u32,
    pub txs_24h: i64,
    pub fees_24h: i64,
    /// Share of 24h transactions with shielded components, 0–1.
    pub shielded_tx_ratio_24h: f64,
    pub migrations_24h: i64,
}

/// Network statistics.
#[utoipa::path(get, path = "/v1/network", responses((status = 200, body = Network)), tag = "network")]
pub async fn network(State(s): State<AppState>) -> ApiResult<Network> {
    let snap: Option<(i32, f64)> = sqlx::query_as(
        "SELECT height, difficulty FROM network_snapshots ORDER BY time DESC LIMIT 1",
    )
    .fetch_optional(&s.db.pool)
    .await?;
    let avg_block_time: Option<f64> = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM (max(time) - min(time)))::float8 / NULLIF(count(*) - 1, 0)
         FROM (SELECT time FROM blocks ORDER BY height DESC LIMIT 100) b",
    )
    .fetch_one(&s.db.pool)
    .await?;
    let (txs, fees, shielded, migrations): (i64, Option<i64>, i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE kind <> 'coinbase'),
                sum(fee)::bigint,
                count(*) FILTER (WHERE kind IN ('shielded', 'shielding', 'deshielding', 'migration', 'mixed')),
                count(*) FILTER (WHERE kind = 'migration')
         FROM transactions WHERE time > (SELECT max(time) FROM blocks) - interval '24 hours'",
    )
    .fetch_one(&s.db.pool)
    .await?;
    let height = super::tip(&s.db).await?;
    Ok(Json(Network {
        height,
        difficulty: snap.map(|s| s.1),
        // Equihash (200, 9): difficulty 1 corresponds to 2^13 expected solutions per block.
        solps: snap
            .zip(avg_block_time)
            .filter(|(_, t)| *t > 0.0)
            .map(|((_, d), t)| d * 8192.0 / t),
        avg_block_time,
        target_block_time: zscan_chain::upgrades::target_spacing(height.unwrap_or(0) as u32 + 1),
        txs_24h: txs,
        fees_24h: fees.unwrap_or(0),
        shielded_tx_ratio_24h: if txs > 0 {
            shielded as f64 / txs as f64
        } else {
            0.0
        },
        migrations_24h: migrations,
    }))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct DailyQuery {
    /// Number of days (default 30, max 3650).
    pub days: Option<i32>,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct DailyStats {
    pub day: DateTime<Utc>,
    pub blocks: i64,
    /// Whether transaction figures are known for this day (days before the indexed history, or
    /// partially indexed, report zeros).
    pub has_txs: bool,
    /// Non-coinbase transactions, and their split by kind.
    pub txs: i64,
    pub transparent: i64,
    pub shielding: i64,
    pub deshielding: i64,
    pub shielded: i64,
    pub migration: i64,
    pub mixed: i64,
    /// Fees paid by the day's transactions (zatoshis).
    pub fees: i64,
    pub avg_block_size: f64,
    pub tx_bytes: i64,
    pub transparent_inputs: i64,
    pub transparent_outputs: i64,
    pub sapling_spends: i64,
    pub sapling_outputs: i64,
    pub orchard_actions: i64,
    pub ironwood_actions: i64,
    /// Zatoshis moved into / out of the shielded pools (summed per transaction).
    pub shielded_in: i64,
    pub shielded_out: i64,
    /// New coins issued and fees burned (zatoshis; zero where economics are not known yet).
    pub issued: i64,
    pub burned: i64,
    /// Value pool balances at the end of the day (zatoshis), derived from the latest node
    /// snapshot and the daily pool changes. Null before any snapshot exists.
    pub pool_transparent: Option<i64>,
    pub pool_sprout: Option<i64>,
    pub pool_sapling: Option<i64>,
    pub pool_orchard: Option<i64>,
    pub pool_ironwood: Option<i64>,
}

/// Daily network statistics: blocks, transactions by kind, fees, shielded actions and flows,
/// issuance, end-of-day pool balances. Rolled up by the indexer and kept after old
/// transactions are pruned.
#[utoipa::path(get, path = "/v1/stats/daily", params(DailyQuery),
    responses((status = 200, body = [DailyStats])), tag = "network")]
pub async fn daily(
    State(s): State<AppState>,
    Query(q): Query<DailyQuery>,
) -> ApiResult<Vec<DailyStats>> {
    let anchor = super::pools::anchor(&s).await.ok();
    let rows = sqlx::query_as::<_, DailyStats>(
        // The window ends at the latest indexed block, not the wall clock, like the other stats.
        // Days come from the durable rollup, and from `blocks` for days it doesn't cover yet.
        // Pool balances walk back from the anchor snapshot: balance at the end of day D =
        // anchor - changes of the days after D + changes of the blocks after the anchor.
        "WITH since AS (
            SELECT date_trunc('day', max(time)) - make_interval(days => $1 - 1) AS t FROM blocks
         ), b AS (
            SELECT date_trunc('day', time) AS day, count(*) AS blocks, sum(size) AS bytes,
                   sum(d_transparent) dt, sum(d_sprout) dsp, sum(d_sapling) dsa,
                   sum(d_orchard) dor, sum(d_ironwood) diw
            FROM blocks WHERE time >= (SELECT t FROM since) GROUP BY 1
         ), after_anchor AS (
            SELECT COALESCE(sum(d_transparent), 0) dt, COALESCE(sum(d_sprout), 0) dsp,
                   COALESCE(sum(d_sapling), 0) dsa, COALESCE(sum(d_orchard), 0) dor,
                   COALESCE(sum(d_ironwood), 0) diw
            FROM blocks WHERE height > $2
         ), d AS (
            SELECT day, s.day IS NOT NULL AS has_txs, s.txs, s.transparent, s.shielding,
                   s.deshielding, s.shielded, s.migration, s.mixed, s.fees, s.tx_bytes,
                   s.transparent_inputs, s.transparent_outputs, s.sapling_spends, s.sapling_outputs,
                   s.orchard_actions, s.ironwood_actions, s.shielded_in, s.shielded_out,
                   COALESCE(NULLIF(s.blocks, 0), b.blocks) AS nblocks,
                   COALESCE(NULLIF(s.block_bytes, 0), b.bytes) AS nbytes,
                   CASE WHEN s.blocks > 0 THEN s.d_transparent ELSE b.dt END AS xt,
                   CASE WHEN s.blocks > 0 THEN s.d_sprout ELSE b.dsp END AS xsp,
                   CASE WHEN s.blocks > 0 THEN s.d_sapling ELSE b.dsa END AS xsa,
                   CASE WHEN s.blocks > 0 THEN s.d_orchard ELSE b.dor END AS xor,
                   CASE WHEN s.blocks > 0 THEN s.d_ironwood ELSE b.diw END AS xiw
            FROM (SELECT * FROM daily_stats WHERE day >= (SELECT t FROM since)) s
            FULL JOIN b USING (day)
         ), w AS (
            SELECT d.*,
                   COALESCE(sum(xt) OVER later, 0) lt, COALESCE(sum(xsp) OVER later, 0) lsp,
                   COALESCE(sum(xsa) OVER later, 0) lsa, COALESCE(sum(xor) OVER later, 0) lor,
                   COALESCE(sum(xiw) OVER later, 0) liw
            FROM d WINDOW later AS (ORDER BY day DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
         )
         SELECT w.day, COALESCE(nblocks, 0)::int8 AS blocks, has_txs,
                COALESCE(txs, 0) txs, COALESCE(transparent, 0) transparent,
                COALESCE(shielding, 0) shielding, COALESCE(deshielding, 0) deshielding,
                COALESCE(shielded, 0) shielded, COALESCE(migration, 0) migration,
                COALESCE(mixed, 0) mixed, COALESCE(w.fees, 0) fees,
                COALESCE(nbytes::float8 / NULLIF(nblocks, 0), 0) AS avg_block_size,
                COALESCE(tx_bytes, 0) tx_bytes,
                COALESCE(transparent_inputs, 0) transparent_inputs,
                COALESCE(transparent_outputs, 0) transparent_outputs,
                COALESCE(sapling_spends, 0) sapling_spends,
                COALESCE(sapling_outputs, 0) sapling_outputs,
                COALESCE(orchard_actions, 0) orchard_actions,
                COALESCE(ironwood_actions, 0) ironwood_actions,
                COALESCE(shielded_in, 0) shielded_in, COALESCE(shielded_out, 0) shielded_out,
                COALESCE(e.issued, 0) issued, COALESCE(e.burned, 0) burned,
                ($3 - lt + a.dt)::int8 AS pool_transparent, ($4 - lsp + a.dsp)::int8 AS pool_sprout,
                ($5 - lsa + a.dsa)::int8 AS pool_sapling, ($6 - lor + a.dor)::int8 AS pool_orchard,
                ($7 - liw + a.diw)::int8 AS pool_ironwood
         FROM w CROSS JOIN after_anchor a LEFT JOIN daily_blocks e ON e.day = w.day
         ORDER BY w.day",
    )
    .bind(q.days.unwrap_or(30).clamp(1, 3650))
    .bind(anchor.as_ref().map(|a| a.height))
    .bind(anchor.as_ref().map(|a| a.transparent))
    .bind(anchor.as_ref().map(|a| a.sprout))
    .bind(anchor.as_ref().map(|a| a.sapling))
    .bind(anchor.as_ref().map(|a| a.orchard))
    .bind(anchor.as_ref().map(|a| a.ironwood))
    .fetch_all(&s.db.pool)
    .await?;
    Ok(Json(rows))
}
