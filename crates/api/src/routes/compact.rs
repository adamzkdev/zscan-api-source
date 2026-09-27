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

    let mut out = Vec::with_capacity(64 * 1024);
    out.extend_from_slice(b"ZSC1");
    let mut stream = s.lwd.block_range(q.start, end).await?;
    while let Some(b) = stream.message().await? {
        let txs: Vec<_> = b
            .vtx
            .iter()
            .filter(|t| {
                !t.spends.is_empty()
                    || !t.outputs.is_empty()
                    || !t.actions.is_empty()
                    || !t.ironwood_actions.is_empty()
            })
            .collect();
        out.extend_from_slice(&(b.height as u32).to_le_bytes());
        out.extend_from_slice(&b.time.to_le_bytes());
        let sapling_size = b
            .chain_metadata
            .as_ref()
            .map_or(0, |m| m.sapling_commitment_tree_size);
        out.extend_from_slice(&sapling_size.to_le_bytes());
        out.extend_from_slice(&(txs.len() as u32).to_le_bytes());
        for t in txs {
            put(&mut out, &t.txid, 32);
            out.extend_from_slice(&(t.index as u32).to_le_bytes());
            for n in [
                t.spends.len(),
                t.outputs.len(),
                t.actions.len(),
                t.ironwood_actions.len(),
            ] {
                out.extend_from_slice(&(n as u16).to_le_bytes());
            }
            for sp in &t.spends {
                put(&mut out, &sp.nf, 32);
            }
            for o in &t.outputs {
                put(&mut out, &o.cmu, 32);
                put(&mut out, &o.ephemeral_key, 32);
                put(&mut out, &o.ciphertext, 52);
            }
            for a in t.actions.iter().chain(&t.ironwood_actions) {
                put(&mut out, &a.nullifier, 32);
                put(&mut out, &a.cmx, 32);
                put(&mut out, &a.ephemeral_key, 32);
                put(&mut out, &a.ciphertext, 52);
            }
        }
    }

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

/// Writes exactly `len` bytes: the field, truncated or zero-padded (fields are fixed-size in
/// valid blocks; this only keeps the layout aligned if a server sends something odd).
fn put(out: &mut Vec<u8>, bytes: &[u8], len: usize) {
    let n = bytes.len().min(len);
    out.extend_from_slice(&bytes[..n]);
    out.resize(out.len() + (len - n), 0);
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
