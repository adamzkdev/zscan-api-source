//! Service status and chain reorganizations.

use axum::{extract::State, Json};
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

use crate::error::ApiResult;
use crate::AppState;

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct Reorg {
    pub detected_at: DateTime<Utc>,
    pub height: i32,
    pub old_hash: String,
    pub new_hash: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Health {
    pub network: String,
    pub node_tip: Option<u32>,
    pub indexed_tip: Option<i32>,
    /// Blocks the index is behind the node.
    pub lag_blocks: Option<i64>,
    pub last_block_time: Option<DateTime<Utc>>,
    pub last_snapshot_time: Option<DateTime<Utc>>,
    pub lightwalletd_ok: bool,
    pub audit_checks: i64,
    pub audit_failed: i64,
    pub reorgs: Vec<Reorg>,
}

/// Live health of the indexer, data sources, supply audit, and recent reorgs.
#[utoipa::path(get, path = "/v1/health", responses((status = 200, body = Health)), tag = "network")]
pub async fn health(State(s): State<AppState>) -> ApiResult<Health> {
    let node_tip = s.lwd.tip().await.ok().map(|(h, _)| h);
    let (indexed_tip, last_block_time): (Option<i32>, Option<DateTime<Utc>>) =
        sqlx::query_as("SELECT max(height), max(time) FROM blocks")
            .fetch_one(&s.db.pool)
            .await?;
    let last_snapshot_time: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT max(time) FROM pool_snapshots")
            .fetch_one(&s.db.pool)
            .await?;
    let (audit_checks, audit_failed): (i64, i64) =
        sqlx::query_as("SELECT count(*), count(*) FILTER (WHERE NOT ok) FROM audit_checks")
            .fetch_one(&s.db.pool)
            .await?;
    let reorgs = sqlx::query_as::<_, Reorg>(
        "SELECT detected_at, height, old_hash, new_hash FROM reorgs ORDER BY id DESC LIMIT 50",
    )
    .fetch_all(&s.db.pool)
    .await?;
    Ok(Json(Health {
        network: if zscan_chain::is_testnet() {
            "testnet"
        } else {
            "mainnet"
        }
        .into(),
        lag_blocks: node_tip.zip(indexed_tip).map(|(n, i)| n as i64 - i as i64),
        lightwalletd_ok: node_tip.is_some(),
        node_tip,
        indexed_tip,
        last_block_time,
        last_snapshot_time,
        audit_checks,
        audit_failed,
        reorgs,
    }))
}
