use axum::{
    extract::{Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use zscan_chain::{
    hash,
    parse::{address_kind, AddressKind},
};

use crate::error::ApiResult;
use crate::AppState;

#[derive(Debug, Deserialize, IntoParams)]
pub struct SearchQuery {
    /// Block height, block hash, txid, or address.
    pub q: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SearchResult {
    /// block | tx | address | shielded_address | not_found
    #[serde(rename = "type")]
    pub kind: &'static str,
    /// Canonical identifier to navigate to.
    pub id: String,
}

/// Resolves a search string to a block, transaction, or address.
#[utoipa::path(get, path = "/v1/search", params(SearchQuery),
    responses((status = 200, body = SearchResult)), tag = "search")]
pub async fn search(
    State(s): State<AppState>,
    Query(q): Query<SearchQuery>,
) -> ApiResult<SearchResult> {
    let q = q.q.trim().to_string();
    let found = |kind, id: String| Ok(Json(SearchResult { kind, id }));

    if let Ok(h) = q.replace([',', '_', ' '], "").parse::<u32>() {
        let tip = match s.lwd.tip().await {
            Ok((t, _)) => t,
            Err(_) => super::tip(&s.db).await?.unwrap_or(0) as u32,
        };
        if h <= tip {
            return found("block", h.to_string());
        }
    }
    match address_kind(&q) {
        Some(AddressKind::Transparent) => return found("address", q),
        Some(AddressKind::Shielded) => return found("shielded_address", q),
        None => {}
    }
    let lower = q.to_lowercase();
    if hash::is_hash(&lower) {
        let block: Option<i32> = sqlx::query_scalar("SELECT height FROM blocks WHERE hash = $1")
            .bind(&lower)
            .fetch_optional(&s.db.pool)
            .await?;
        if let Some(h) = block {
            return found("block", h.to_string());
        }
        // Block hashes start with leading zeros; txids almost never do.
        if s.lwd.raw_tx(&lower).await?.is_some() {
            return found("tx", lower);
        }
    }
    found("not_found", q)
}
