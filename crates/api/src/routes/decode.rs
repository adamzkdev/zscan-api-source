//! Developer tools: decode a raw transaction or any address without touching the chain.

use axum::{
    extract::{Path, State},
    Json,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use zscan_chain::{address::AddressInfo, parse::ParsedTx, parse_tx};

use crate::error::{ApiError, ApiResult};
use crate::AppState;

#[derive(Debug, Deserialize, ToSchema)]
pub struct DecodeRequest {
    /// Raw transaction, hex-encoded.
    pub hex: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DecodedTx {
    /// Version, expiry, transparent inputs and outputs, value balances per pool, Sapling spends
    /// and outputs, Orchard and Ironwood actions (nullifiers and commitments).
    #[schema(value_type = Object)]
    pub tx: ParsedTx,
    /// Pools the transaction touches: transparent, sprout, sapling, orchard, ironwood.
    pub pools: Vec<String>,
    /// ZIP-317 logical actions and the conventional fee they call for, zatoshis.
    pub logical_actions: usize,
    pub conventional_fee: i64,
    /// Block height when the transaction is already indexed as mined.
    pub mined_height: Option<i32>,
    pub in_mempool: bool,
}

/// Decodes a raw transaction (v1–v6, including Ironwood) without broadcasting it.
#[utoipa::path(post, path = "/v1/decode/tx", request_body = DecodeRequest,
    responses((status = 200, body = DecodedTx), (status = 400)), tag = "transactions")]
pub async fn tx(State(s): State<AppState>, Json(req): Json<DecodeRequest>) -> ApiResult<DecodedTx> {
    let hex: String = req.hex.split_whitespace().collect();
    let raw = hex::decode(hex.trim_start_matches("0x")).map_err(|_| {
        ApiError::BadRequest("invalid_hex", "Transaction must be hex-encoded".into())
    })?;
    let tx = parse_tx(&raw).map_err(|e| {
        ApiError::BadRequest("invalid_tx", format!("Cannot decode transaction: {e:#}"))
    })?;
    let pools = [
        (
            "transparent",
            !tx.inputs.is_empty() || !tx.outputs.is_empty(),
        ),
        ("sprout", tx.n_joinsplits > 0),
        (
            "sapling",
            !tx.sapling_spends.is_empty() || !tx.sapling_outputs.is_empty(),
        ),
        ("orchard", !tx.orchard_actions.is_empty()),
        ("ironwood", !tx.ironwood_actions.is_empty()),
    ]
    .into_iter()
    .filter(|(_, used)| *used)
    .map(|(p, _)| p.to_string())
    .collect();
    let logical_actions = tx.logical_actions();
    let mined_height: Option<i32> =
        sqlx::query_scalar("SELECT height FROM transactions WHERE txid = $1")
            .bind(&tx.txid)
            .fetch_optional(&s.db.pool)
            .await?;
    let in_mempool: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM mempool WHERE txid = $1)")
            .bind(&tx.txid)
            .fetch_one(&s.db.pool)
            .await?;
    Ok(Json(DecodedTx {
        pools,
        conventional_fee: if tx.is_coinbase {
            0
        } else {
            5_000 * logical_actions.max(2) as i64
        },
        logical_actions,
        mined_height,
        in_mempool,
        tx,
    }))
}

/// Decodes any Zcash address (transparent, TEX, Sapling, Sprout, Unified) on any network: type,
/// pools it can receive in, memo support and receivers.
#[utoipa::path(get, path = "/v1/decode/address/{address}", params(("address" = String, Path)),
    responses((status = 200, body = Object), (status = 400)), tag = "addresses")]
pub async fn address(Path(address): Path<String>) -> ApiResult<AddressInfo> {
    zscan_chain::address::inspect_any(&address)
        .map(Json)
        .ok_or_else(|| ApiError::BadRequest("invalid_address", "Not a valid Zcash address".into()))
}
