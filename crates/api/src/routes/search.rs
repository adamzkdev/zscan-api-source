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
    /// Block height, block hash, txid, address, inscription or ZRC-20 ticker.
    pub q: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SearchResult {
    /// block | tx | address | shielded_address | inscription | token | not_found
    #[serde(rename = "type")]
    pub kind: &'static str,
    /// Canonical identifier to navigate to.
    pub id: String,
}

/// Resolves a search string to a block, transaction, address, inscription (`#123`, `<txid>i0`)
/// or ZRC-20 ticker.
#[utoipa::path(get, path = "/v1/search", params(SearchQuery),
    responses((status = 200, body = SearchResult)), tag = "search")]
pub async fn search(
    State(s): State<AppState>,
    Query(q): Query<SearchQuery>,
) -> ApiResult<SearchResult> {
    let q = q.q.trim().to_string();
    let found = |kind, id: String| Ok(Json(SearchResult { kind, id }));

    // Inscriptions: `#123` or `<txid>i0`.
    let lower = q.to_lowercase();
    let inscription: Option<String> = if let Some(n) = lower.strip_prefix('#') {
        match n.replace(',', "").parse::<i64>() {
            Ok(n) => {
                sqlx::query_scalar("SELECT id FROM inscriptions WHERE number = $1")
                    .bind(n)
                    .fetch_optional(&s.db.pool)
                    .await?
            }
            Err(_) => None,
        }
    } else if lower.len() == 66 && lower.ends_with("i0") {
        sqlx::query_scalar("SELECT id FROM inscriptions WHERE id = $1")
            .bind(&lower)
            .fetch_optional(&s.db.pool)
            .await?
    } else {
        None
    };
    if let Some(id) = inscription {
        return found("inscription", id);
    }

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
    // ZRC-20 tickers are 4-5 bytes; numbers were handled as heights above.
    if (4..=5).contains(&lower.len()) {
        let tick: Option<String> =
            sqlx::query_scalar("SELECT tick FROM zrc20_tokens WHERE tick = $1")
                .bind(&lower)
                .fetch_optional(&s.db.pool)
                .await?;
        if let Some(t) = tick {
            return found("token", t);
        }
    }
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
