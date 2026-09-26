//! Address name tags, with the source of every label.

use std::collections::HashMap;

use axum::{
    extract::{Query, State},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::error::ApiResult;
use crate::AppState;

/// A name tag attached to an address.
#[derive(Debug, Clone, Serialize, sqlx::FromRow, ToSchema)]
pub struct Tag {
    pub name: String,
    /// shielded_pool | treasury | bridge | exchange | fund | mining_pool
    pub category: String,
    /// Protocol page slug (`/v1/protocols/{slug}`).
    pub protocol: Option<String>,
    /// Where the label comes from, so it can be checked.
    pub source: String,
    /// curated | consensus | coinbase | api
    pub origin: String,
    /// False once the protocol stopped using this address.
    pub active: bool,
}

/// Tags for `addresses`, keyed by address.
pub async fn lookup(
    db: &zscan_db::Db,
    addresses: &[String],
) -> anyhow::Result<HashMap<String, Tag>> {
    if addresses.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(String, String, String, Option<String>, String, String, bool)> = sqlx::query_as(
        "SELECT address, name, category, protocol, source, origin, active
         FROM labels WHERE address = ANY($1)",
    )
    .bind(addresses)
    .fetch_all(&db.pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(a, name, category, protocol, source, origin, active)| {
            (
                a,
                Tag {
                    name,
                    category,
                    protocol,
                    source,
                    origin,
                    active,
                },
            )
        })
        .collect())
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct LabeledAddress {
    pub address: String,
    pub name: String,
    pub category: String,
    pub protocol: Option<String>,
    pub source: String,
    pub origin: String,
    pub active: bool,
    /// Balance from the node at `updated_at`, zatoshis.
    pub balance: Option<i64>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct LabelsQuery {
    /// Filter by category (e.g. `bridge`, `mining_pool`, `treasury`, `exchange`).
    pub category: Option<String>,
    /// Filter by protocol slug.
    pub protocol: Option<String>,
}

/// Every labeled address with its source. Contributions: `labels/addresses.json` on GitHub.
#[utoipa::path(get, path = "/v1/labels", params(LabelsQuery),
    responses((status = 200, body = [LabeledAddress])), tag = "labels")]
pub async fn list(
    State(s): State<AppState>,
    Query(q): Query<LabelsQuery>,
) -> ApiResult<Vec<LabeledAddress>> {
    let rows = sqlx::query_as::<_, LabeledAddress>(
        "SELECT address, name, category, protocol, source, origin, active, balance, updated_at
         FROM labels
         WHERE ($1::text IS NULL OR category = $1) AND ($2::text IS NULL OR protocol = $2)
         ORDER BY balance DESC NULLS LAST, name",
    )
    .bind(&q.category)
    .bind(&q.protocol)
    .fetch_all(&s.db.pool)
    .await?;
    Ok(Json(rows))
}
