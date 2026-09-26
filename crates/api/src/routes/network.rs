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
}

/// Indexer status.
#[utoipa::path(get, path = "/v1/status", responses((status = 200, body = Status)), tag = "network")]
pub async fn status(State(s): State<AppState>) -> ApiResult<Status> {
    let (low, high, target): (Option<i32>, Option<i32>, Option<i32>) =
        sqlx::query_as("SELECT low, high, target FROM sync_state")
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
    /// Number of days (default 30, max 365).
    pub days: Option<i32>,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct DailyStats {
    pub day: DateTime<Utc>,
    pub blocks: i64,
    pub txs: i64,
    pub transparent: i64,
    pub shielding: i64,
    pub deshielding: i64,
    pub shielded: i64,
    pub migration: i64,
    pub mixed: i64,
    pub fees: i64,
    pub avg_block_size: f64,
}

/// Daily transaction statistics by kind.
#[utoipa::path(get, path = "/v1/stats/daily", params(DailyQuery),
    responses((status = 200, body = [DailyStats])), tag = "network")]
pub async fn daily(
    State(s): State<AppState>,
    Query(q): Query<DailyQuery>,
) -> ApiResult<Vec<DailyStats>> {
    let rows = sqlx::query_as::<_, DailyStats>(
        // The window ends at the latest indexed block, not the wall clock, like the other stats.
        "WITH since AS (
            SELECT date_trunc('day', max(time)) - make_interval(days => $1 - 1) AS t FROM blocks
         ), b AS (
            SELECT date_trunc('day', time) AS day, count(*) AS blocks, avg(size)::float8 AS avg_block_size
            FROM blocks WHERE time >= (SELECT t FROM since) GROUP BY 1
         ), t AS (
            SELECT date_trunc('day', time) AS day,
                   count(*) FILTER (WHERE kind <> 'coinbase') AS txs,
                   count(*) FILTER (WHERE kind = 'transparent') AS transparent,
                   count(*) FILTER (WHERE kind = 'shielding') AS shielding,
                   count(*) FILTER (WHERE kind = 'deshielding') AS deshielding,
                   count(*) FILTER (WHERE kind = 'shielded') AS shielded,
                   count(*) FILTER (WHERE kind = 'migration') AS migration,
                   count(*) FILTER (WHERE kind = 'mixed') AS mixed,
                   COALESCE(sum(fee), 0)::bigint AS fees
            FROM transactions WHERE time >= (SELECT t FROM since) GROUP BY 1
         )
         SELECT b.day, b.blocks, COALESCE(t.txs, 0) txs, COALESCE(t.transparent, 0) transparent,
                COALESCE(t.shielding, 0) shielding, COALESCE(t.deshielding, 0) deshielding,
                COALESCE(t.shielded, 0) shielded, COALESCE(t.migration, 0) migration,
                COALESCE(t.mixed, 0) mixed, COALESCE(t.fees, 0) fees, b.avg_block_size
         FROM b LEFT JOIN t USING (day)
         ORDER BY b.day",
    )
    .bind(q.days.unwrap_or(30).clamp(1, 365))
    .fetch_all(&s.db.pool)
    .await?;
    Ok(Json(rows))
}
