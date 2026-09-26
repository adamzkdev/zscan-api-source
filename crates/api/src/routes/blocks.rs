use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use zscan_db::build::build_block;

use crate::error::{ApiError, ApiResult};
use crate::models::{BlockSummary, TxRow, TX_ROW_COLUMNS};
use crate::routes::rankings::MINER_POOL_SQL;
use crate::AppState;

#[derive(Debug, Deserialize, IntoParams)]
pub struct ListQuery {
    /// Max items (default 25, max 100).
    pub limit: Option<i64>,
    /// Only blocks below this height (for pagination).
    pub before: Option<i32>,
}

/// Latest blocks.
#[utoipa::path(get, path = "/v1/blocks", params(ListQuery), responses((status = 200, body = [BlockSummary])), tag = "blocks")]
pub async fn list(
    State(s): State<AppState>,
    Query(q): Query<ListQuery>,
) -> ApiResult<Vec<BlockSummary>> {
    let rows = sqlx::query_as::<_, BlockSummary>(&format!(
        "SELECT height, hash, time, tx_count, size, fees, miner, {MINER_POOL_SQL} AS miner_pool, coinbase_value FROM blocks
         WHERE ($1::int IS NULL OR height < $1) ORDER BY height DESC LIMIT $2"
    ))
    .bind(q.before)
    .bind(q.limit.unwrap_or(25).clamp(1, 100))
    .fetch_all(&s.db.pool)
    .await?;
    Ok(Json(rows))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PoolDeltas {
    pub transparent: i64,
    pub sprout: i64,
    pub sapling: i64,
    pub orchard: i64,
    pub ironwood: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct BlockDetail {
    #[serde(flatten)]
    pub summary: BlockSummary,
    pub prev_hash: String,
    pub next_hash: Option<String>,
    pub confirmations: Option<i64>,
    pub coinbase_value: i64,
    pub sapling_tree_size: Option<i32>,
    pub orchard_tree_size: Option<i32>,
    pub ironwood_tree_size: Option<i32>,
    /// Net change of each value pool caused by this block.
    pub pool_deltas: PoolDeltas,
    pub txs: Vec<TxRow>,
    /// `false` when decoded live because the block is outside the indexed range.
    pub indexed: bool,
}

#[derive(sqlx::FromRow)]
struct BlockRow {
    height: i32,
    hash: String,
    prev_hash: String,
    time: chrono::DateTime<chrono::Utc>,
    tx_count: i32,
    size: i32,
    fees: Option<i64>,
    miner: Option<String>,
    miner_pool: Option<String>,
    coinbase_value: i64,
    sapling_tree_size: Option<i32>,
    orchard_tree_size: Option<i32>,
    ironwood_tree_size: Option<i32>,
    d_transparent: i64,
    d_sprout: i64,
    d_sapling: i64,
    d_orchard: i64,
    d_ironwood: i64,
}

/// Block by height or hash, with its transactions.
#[utoipa::path(get, path = "/v1/blocks/{id}", params(("id" = String, Path, description = "Height or block hash")),
    responses((status = 200, body = BlockDetail), (status = 404)), tag = "blocks")]
pub async fn detail(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult<BlockDetail> {
    let height: Option<i32> = id.parse().ok();
    if height.is_none() && !zscan_chain::hash::is_hash(&id) {
        return Err(ApiError::BadRequest(
            "invalid_id",
            "Expected a height or a block hash".into(),
        ));
    }
    let tip = super::tip(&s.db).await?;
    let row = sqlx::query_as::<_, BlockRow>(&format!(
        "SELECT height, hash, prev_hash, time, tx_count, size, fees, miner, {MINER_POOL_SQL} AS miner_pool,
                coinbase_value, sapling_tree_size, orchard_tree_size, ironwood_tree_size,
                d_transparent, d_sprout, d_sapling, d_orchard, d_ironwood
         FROM blocks WHERE height = $1 OR hash = $2"
    ))
        .bind(height.unwrap_or(-1))
        .bind(id.to_lowercase())
        .fetch_optional(&s.db.pool)
        .await?;

    if let Some(b) = row {
        let txs = sqlx::query_as::<_, TxRow>(&format!(
            "SELECT {TX_ROW_COLUMNS} FROM transactions WHERE height = $1 ORDER BY block_index"
        ))
        .bind(b.height)
        .fetch_all(&s.db.pool)
        .await?;
        let next_hash = sqlx::query_scalar("SELECT hash FROM blocks WHERE height = $1")
            .bind(b.height + 1)
            .fetch_optional(&s.db.pool)
            .await?;
        return Ok(Json(BlockDetail {
            summary: BlockSummary {
                height: b.height,
                hash: b.hash,
                time: b.time,
                tx_count: b.tx_count,
                size: b.size,
                fees: b.fees,
                miner: b.miner,
                miner_pool: b.miner_pool,
                coinbase_value: b.coinbase_value,
            },
            prev_hash: b.prev_hash,
            next_hash,
            confirmations: tip.map(|t| (t - b.height + 1) as i64),
            coinbase_value: b.coinbase_value,
            sapling_tree_size: b.sapling_tree_size,
            orchard_tree_size: b.orchard_tree_size,
            ironwood_tree_size: b.ironwood_tree_size,
            pool_deltas: PoolDeltas {
                transparent: b.d_transparent,
                sprout: b.d_sprout,
                sapling: b.d_sapling,
                orchard: b.d_orchard,
                ironwood: b.d_ironwood,
            },
            txs,
            indexed: true,
        }));
    }

    // Outside the indexed range: decode live from lightwalletd (heights only).
    let Some(h) = height.filter(|h| *h >= 0) else {
        return Err(ApiError::NotFound(format!("Block {id} not found")));
    };
    let Some(cb) = s.lwd.block(h as u32).await? else {
        return Err(ApiError::NotFound(format!("Block {h} not found")));
    };
    let b = build_block(&s.db, &s.lwd, &s.resolver, cb).await?;
    let txs = b
        .txs
        .iter()
        .map(|t| TxRow::from_indexed(t, b.height, b.time))
        .collect();
    Ok(Json(BlockDetail {
        summary: BlockSummary {
            height: b.height as i32,
            hash: b.hash,
            time: b.time,
            tx_count: b.txs.len() as i32,
            size: b.size as i32,
            fees: b.fees,
            miner_pool: b.miner_pool.map(str::to_string),
            miner: b.miner,
            coinbase_value: b.coinbase_value,
        },
        prev_hash: b.prev_hash,
        next_hash: None,
        confirmations: tip.map(|t| (t - h + 1) as i64),
        coinbase_value: b.coinbase_value,
        sapling_tree_size: b.sapling_tree_size.map(|v| v as i32),
        orchard_tree_size: b.orchard_tree_size.map(|v| v as i32),
        ironwood_tree_size: b.ironwood_tree_size.map(|v| v as i32),
        pool_deltas: PoolDeltas {
            transparent: b.deltas.transparent,
            sprout: b.deltas.sprout,
            sapling: b.deltas.sapling,
            orchard: b.deltas.orchard,
            ironwood: b.deltas.ironwood,
        },
        txs,
        indexed: false,
    }))
}
