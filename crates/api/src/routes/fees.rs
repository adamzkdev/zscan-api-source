//! ZIP-317 fee tracker (Zcash's equivalent of a gas tracker).

use axum::{extract::State, Json};
use serde::Serialize;
use utoipa::ToSchema;

use crate::error::ApiResult;
use crate::AppState;

/// ZIP-317 parameters.
const MARGINAL_FEE: i64 = 5_000;
const GRACE_ACTIONS: i64 = 2;
const MAX_BLOCK_SIZE: i64 = 2_000_000;

/// SQL for a transaction's ZIP-317 logical action count.
const LOGICAL_ACTIONS: &str =
    "(GREATEST(n_tin, n_tout) + 2 * n_joinsplits + GREATEST(n_sapling_spends, n_sapling_outputs) \
    + n_orchard_actions + n_ironwood_actions)";

#[derive(Debug, Serialize, ToSchema)]
pub struct FeeExample {
    pub label: &'static str,
    pub logical_actions: i64,
    /// ZIP-317 conventional fee (zatoshis).
    pub fee: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Fees {
    pub marginal_fee: i64,
    pub grace_actions: i64,
    /// Fee for the most common shielded transfer (2 actions).
    pub standard_fee: i64,
    pub examples: Vec<FeeExample>,
    /// Over the last 100 blocks.
    pub median_fee: Option<i64>,
    pub median_fee_per_action: Option<f64>,
    /// Share of transactions paying at least the conventional fee, 0–1.
    pub conventional_share: f64,
    pub avg_block_fullness: f64,
    pub mempool_count: i64,
    pub mempool_fees: i64,
    pub mempool_bytes: i64,
    /// Whether the mempool fits in the next block.
    pub fits_next_block: bool,
}

fn conventional(actions: i64) -> i64 {
    MARGINAL_FEE * actions.max(GRACE_ACTIONS)
}

/// Current fee conditions and ZIP-317 conventional fees.
#[utoipa::path(get, path = "/v1/fees", responses((status = 200, body = Fees)), tag = "network")]
pub async fn fees(State(s): State<AppState>) -> ApiResult<Fees> {
    let (median_fee, median_per_action, conventional_share): (Option<f64>, Option<f64>, Option<f64>) = sqlx::query_as(&format!(
        "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY fee)::float8,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY fee::float8 / GREATEST({LOGICAL_ACTIONS}, 1))::float8,
                avg(CASE WHEN fee >= {MARGINAL_FEE} * GREATEST({LOGICAL_ACTIONS}, {GRACE_ACTIONS}) THEN 1.0 ELSE 0.0 END)::float8
         FROM transactions
         WHERE kind <> 'coinbase' AND fee IS NOT NULL
           AND height > (SELECT max(height) FROM blocks) - 100"
    ))
    .fetch_one(&s.db.pool)
    .await?;
    let fullness: Option<f64> = sqlx::query_scalar(&format!(
        "SELECT avg(size::float8 / {MAX_BLOCK_SIZE}) FROM (SELECT size FROM blocks ORDER BY height DESC LIMIT 100) b"
    ))
    .fetch_one(&s.db.pool)
    .await?;
    let (mempool_count, mempool_fees, mempool_bytes): (i64, Option<i64>, Option<i64>) =
        sqlx::query_as(
            "SELECT count(*), sum(fee)::bigint, sum(size)::bigint FROM mempool m
         WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.txid = m.txid)",
        )
        .fetch_one(&s.db.pool)
        .await?;
    let examples = vec![
        ("Shielded transfer (2 actions)", 2),
        ("Shielded transfer with change to 3 recipients", 4),
        ("Shielding 5 transparent inputs", 5),
        ("Consolidating 20 transparent inputs", 20),
    ]
    .into_iter()
    .map(|(label, n)| FeeExample {
        label,
        logical_actions: n,
        fee: conventional(n),
    })
    .collect();
    Ok(Json(Fees {
        marginal_fee: MARGINAL_FEE,
        grace_actions: GRACE_ACTIONS,
        standard_fee: conventional(2),
        examples,
        median_fee: median_fee.map(|v| v as i64),
        median_fee_per_action: median_per_action,
        conventional_share: conventional_share.unwrap_or(0.0),
        avg_block_fullness: fullness.unwrap_or(0.0),
        mempool_count,
        mempool_fees: mempool_fees.unwrap_or(0),
        mempool_bytes: mempool_bytes.unwrap_or(0),
        fits_next_block: mempool_bytes.unwrap_or(0) < MAX_BLOCK_SIZE,
    }))
}
