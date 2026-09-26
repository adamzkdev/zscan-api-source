use std::collections::HashMap;

use axum::{
    extract::{Path, Query, State},
    Json,
};
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use zscan_chain::{hash, parse_tx, summarize, PrevOut};

use crate::error::{ApiError, ApiResult};
use crate::models::{TxRow, TX_ROW_COLUMNS};
use crate::AppState;

#[derive(Debug, Deserialize, IntoParams)]
pub struct ListQuery {
    /// Filter by kind (coinbase, transparent, shielding, deshielding, shielded, migration, mixed).
    pub kind: Option<String>,
    /// Max items (default 25, max 100).
    pub limit: Option<i64>,
    /// Only transactions below this height (for pagination).
    pub before: Option<i32>,
    /// Sort by `recent` (default) or `value` (largest shielded value moved first).
    pub sort: Option<String>,
}

/// Latest transactions, optionally filtered by kind.
#[utoipa::path(get, path = "/v1/txs", params(ListQuery), responses((status = 200, body = [TxRow])), tag = "transactions")]
pub async fn list(State(s): State<AppState>, Query(q): Query<ListQuery>) -> ApiResult<Vec<TxRow>> {
    let order = match q.sort.as_deref() {
        Some("value") => "GREATEST(abs(orchard_vb), abs(ironwood_vb), abs(sapling_vb)) DESC",
        _ => "height DESC, block_index DESC",
    };
    let rows = sqlx::query_as::<_, TxRow>(&format!(
        "SELECT {TX_ROW_COLUMNS} FROM transactions
         WHERE ($1::text IS NULL OR kind = $1) AND ($2::int IS NULL OR height < $2)
         ORDER BY {order} LIMIT $3"
    ))
    .bind(&q.kind)
    .bind(q.before)
    .bind(q.limit.unwrap_or(25).clamp(1, 100))
    .fetch_all(&s.db.pool)
    .await?;
    Ok(Json(rows))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct InputView {
    pub prev_txid: Option<String>,
    pub prev_n: Option<u32>,
    pub value: Option<i64>,
    pub address: Option<String>,
    /// Coinbase data (hex) for coinbase inputs.
    pub coinbase: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SpentBy {
    pub txid: String,
    pub n: i32,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OutputView {
    pub n: u32,
    pub value: i64,
    pub address: Option<String>,
    pub script: String,
    pub spent_by: Option<SpentBy>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ActionView {
    pub nullifier: String,
    pub cmx: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ValueBalances {
    /// Positive = value leaves the pool.
    pub sprout: i64,
    pub sapling: i64,
    pub orchard: i64,
    pub ironwood: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TxDetail {
    pub txid: String,
    /// mined | mempool
    pub status: String,
    pub height: Option<i64>,
    pub block_hash: Option<String>,
    pub time: Option<DateTime<Utc>>,
    pub confirmations: Option<i64>,
    pub version: u32,
    pub size: usize,
    pub kind: String,
    pub fee: Option<i64>,
    pub transparent_in: i64,
    pub transparent_out: i64,
    pub value_balances: ValueBalances,
    pub lock_time: u32,
    pub expiry_height: Option<u32>,
    pub inputs: Vec<InputView>,
    pub outputs: Vec<OutputView>,
    pub joinsplits: usize,
    /// Sapling spend nullifiers.
    pub sapling_spends: Vec<String>,
    /// Sapling output note commitments.
    pub sapling_outputs: Vec<String>,
    pub orchard_actions: Vec<ActionView>,
    pub ironwood_actions: Vec<ActionView>,
}

/// Transaction details, decoded from the raw transaction. Works for any mined or mempool tx.
#[utoipa::path(get, path = "/v1/tx/{txid}", params(("txid" = String, Path)),
    responses((status = 200, body = TxDetail), (status = 404)), tag = "transactions")]
pub async fn detail(State(s): State<AppState>, Path(txid): Path<String>) -> ApiResult<TxDetail> {
    let txid = txid.to_lowercase();
    if !hash::is_hash(&txid) {
        return Err(ApiError::BadRequest(
            "invalid_txid",
            "Expected a 64-character hex txid".into(),
        ));
    }
    let Some(raw) = s.lwd.raw_tx(&txid).await? else {
        return Err(ApiError::NotFound(format!("Transaction {txid} not found")));
    };
    let tx = parse_tx(&raw.data)?;
    let mined = raw.height != 0 && raw.height != u64::MAX;

    // Spent outputs: the index stores them for indexed txs; otherwise resolve.
    let stored: Vec<(i32, Option<i64>, Option<String>)> =
        sqlx::query_as("SELECT n, value, address FROM tx_inputs WHERE txid = $1")
            .bind(&txid)
            .fetch_all(&s.db.pool)
            .await?;
    let prevouts: Vec<Option<PrevOut>> =
        if !stored.is_empty() || tx.inputs.is_empty() || tx.is_coinbase {
            let by_n: HashMap<i32, PrevOut> = stored
                .into_iter()
                .filter_map(|(n, v, a)| {
                    Some((
                        n,
                        PrevOut {
                            value: v?,
                            address: a,
                        },
                    ))
                })
                .collect();
            (0..tx.inputs.len())
                .map(|n| by_n.get(&(n as i32)).cloned())
                .collect()
        } else {
            let outpoints: Vec<_> = tx
                .inputs
                .iter()
                .filter_map(|i| Some((i.prev_txid.clone()?, i.prev_n?)))
                .collect();
            let known = s.db.known_outputs(&outpoints).await?;
            s.resolver.resolve(&tx.inputs, &known).await?
        };
    let values: Vec<_> = prevouts
        .iter()
        .map(|p| p.as_ref().map(|p| p.value))
        .collect();
    let summary = summarize(&tx, &values);

    let spent: HashMap<i32, SpentBy> = sqlx::query_as::<_, (i32, String, i32)>(
        "SELECT prev_n, txid, n FROM tx_inputs WHERE prev_txid = $1",
    )
    .bind(&txid)
    .fetch_all(&s.db.pool)
    .await?
    .into_iter()
    .map(|(pn, t, n)| (pn, SpentBy { txid: t, n }))
    .collect();
    let mut spent = spent;

    let (block_hash, time) = if mined {
        let h = raw.height as i32;
        match sqlx::query_as::<_, (String, DateTime<Utc>)>(
            "SELECT hash, time FROM blocks WHERE height = $1",
        )
        .bind(h)
        .fetch_optional(&s.db.pool)
        .await?
        {
            Some((bh, t)) => (Some(bh), Some(t)),
            None => match s.lwd.block(h as u32).await? {
                Some(cb) => (
                    Some(hash::to_display(&cb.hash)),
                    Utc.timestamp_opt(cb.time as i64, 0).single(),
                ),
                None => (None, None),
            },
        }
    } else {
        (None, None)
    };
    let tip = super::tip(&s.db).await?;

    Ok(Json(TxDetail {
        status: if mined { "mined" } else { "mempool" }.into(),
        height: mined.then_some(raw.height as i64),
        block_hash,
        time,
        confirmations: mined
            .then(|| tip.map(|t| t as i64 - raw.height as i64 + 1))
            .flatten(),
        version: tx.version,
        size: tx.size,
        kind: summary.kind.as_str().into(),
        fee: summary.fee,
        transparent_in: summary.transparent_in,
        transparent_out: summary.transparent_out,
        value_balances: ValueBalances {
            sprout: tx.sprout_vb,
            sapling: tx.sapling_vb,
            orchard: tx.orchard_vb,
            ironwood: tx.ironwood_vb,
        },
        lock_time: tx.lock_time,
        expiry_height: tx.expiry_height,
        inputs: tx
            .inputs
            .iter()
            .zip(prevouts)
            .map(|(i, p)| InputView {
                prev_txid: i.prev_txid.clone(),
                prev_n: i.prev_n,
                value: p.as_ref().map(|p| p.value),
                address: p.and_then(|p| p.address),
                coinbase: i.coinbase_data.clone(),
            })
            .collect(),
        outputs: tx
            .outputs
            .iter()
            .map(|o| OutputView {
                n: o.n,
                value: o.value,
                address: o.address.clone(),
                script: o.script.clone(),
                spent_by: spent.remove(&(o.n as i32)),
            })
            .collect(),
        joinsplits: tx.n_joinsplits,
        sapling_spends: tx.sapling_spends,
        sapling_outputs: tx.sapling_outputs,
        orchard_actions: tx
            .orchard_actions
            .into_iter()
            .map(|a| ActionView {
                nullifier: a.nullifier,
                cmx: a.cmx,
            })
            .collect(),
        ironwood_actions: tx
            .ironwood_actions
            .into_iter()
            .map(|a| ActionView {
                nullifier: a.nullifier,
                cmx: a.cmx,
            })
            .collect(),
        txid,
    }))
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct BroadcastRequest {
    /// Raw transaction, hex-encoded.
    pub hex: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct BroadcastResponse {
    pub txid: Option<String>,
    pub error_code: i32,
    pub error_message: String,
}

/// Broadcasts a signed raw transaction to the network.
#[utoipa::path(post, path = "/v1/tx/broadcast", request_body = BroadcastRequest,
    responses((status = 200, body = BroadcastResponse)), tag = "transactions")]
pub async fn broadcast(
    State(s): State<AppState>,
    Json(req): Json<BroadcastRequest>,
) -> ApiResult<BroadcastResponse> {
    let raw = hex::decode(req.hex.trim()).map_err(|_| {
        ApiError::BadRequest("invalid_hex", "Transaction must be hex-encoded".into())
    })?;
    let txid = parse_tx(&raw)
        .map_err(|e| ApiError::BadRequest("invalid_tx", format!("Cannot decode transaction: {e}")))?
        .txid;
    let r = s.lwd.send(raw).await?;
    Ok(Json(BroadcastResponse {
        txid: (r.error_code == 0).then_some(txid),
        error_code: r.error_code,
        error_message: r.error_message,
    }))
}
