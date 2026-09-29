//! Shielded data for client-side scanning: the viewing-key explorer decrypts it in the browser,
//! so viewing keys never reach the server. The server only sees which block range or
//! transaction is requested, like any lightwalletd.

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::header,
    response::Response,
    Json,
};
use serde::Deserialize;
use utoipa::IntoParams;
use zscan_chain::parse::{shielded_outputs, ShieldedOutputs};

use crate::error::{ApiError, ApiResult};
use crate::AppState;

/// Most blocks per request: keeps each response a few MB at most.
pub const MAX_BLOCKS: u32 = 500;

#[derive(Debug, Deserialize, IntoParams)]
pub struct RangeQuery {
    /// First block height (inclusive).
    pub start: u32,
    /// Last block height (inclusive), at most 500 blocks after `start`.
    pub end: u32,
}

/// Compact shielded data of a block range, in a binary layout (all integers little-endian):
///
/// ```text
/// "ZSC1"
/// per block:  u32 height, u32 time, u32 sapling_tree_size (end of block), u32 n_tx
///   per tx with shielded data:  [32] txid (protocol order), u32 index,
///     u16 n_sapling_spends, u16 n_sapling_outputs, u16 n_orchard_actions, u16 n_ironwood_actions,
///     spends:  [32] nf
///     outputs: [32] cmu, [32] epk, [52] ciphertext
///     actions (Orchard, then Ironwood): [32] nf, [32] cmx, [32] epk, [52] ciphertext
/// ```
///
/// Every block of the range is present, including the ones with no shielded data.
#[utoipa::path(get, path = "/v1/compact", params(RangeQuery),
    responses((status = 200, description = "Binary compact blocks", content_type = "application/octet-stream")),
    tag = "shielded")]
pub async fn range(
    State(s): State<AppState>,
    Query(q): Query<RangeQuery>,
) -> Result<Response, ApiError> {
    if q.end < q.start || q.end - q.start >= MAX_BLOCKS {
        return Err(ApiError::BadRequest(
            "invalid_range",
            format!(
                "`end` must be between `start` and `start` + {}",
                MAX_BLOCKS - 1
            ),
        ));
    }
    let (tip, _) = s.lwd.tip().await?;
    if q.start > tip {
        return Err(ApiError::BadRequest(
            "invalid_range",
            format!("`start` is above the tip ({tip})"),
        ));
    }
    let end = q.end.min(tip);

    let out = zscan_chain::compact::compact_range(&s.lwd, q.start, end).await?;

    // Blocks well below the tip never change; the last ones can still be reorged.
    let cache = if end + 100 < tip {
        "public, max-age=86400, immutable"
    } else {
        "public, max-age=30"
    };
    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CACHE_CONTROL, cache)
        .body(Body::from(out))
        .expect("valid response"))
}

/// Full note ciphertexts of one transaction, to decrypt memos and outgoing notes with a viewing
/// key (compact blocks only carry the first 52 bytes of each note).
#[utoipa::path(get, path = "/v1/tx/{txid}/shielded", params(("txid" = String, Path)),
    responses((status = 200, body = Object)), tag = "shielded")]
pub async fn tx_outputs(
    State(s): State<AppState>,
    Path(txid): Path<String>,
) -> ApiResult<ShieldedOutputs> {
    if txid.len() != 64 || !txid.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err(ApiError::BadRequest(
            "invalid_txid",
            "Expected a 64-character hex txid".into(),
        ));
    }
    let Some(raw) = s.lwd.raw_tx(&txid).await? else {
        return Err(ApiError::NotFound(format!("Transaction {txid} not found")));
    };
    Ok(Json(shielded_outputs(&raw.data)?))
}
