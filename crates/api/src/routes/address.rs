use axum::{
    extract::{Path, Query, State},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use zscan_chain::parse::{address_kind, tex_to_p2pkh, AddressKind};

use crate::error::{ApiError, ApiResult};
use crate::AppState;

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct AddressTx {
    pub txid: String,
    pub height: i32,
    pub time: DateTime<Utc>,
    pub kind: String,
    /// Net change of this address's balance in the transaction (zatoshis).
    pub delta: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AddressDetail {
    pub address: String,
    /// Name tag, e.g. "Maya Protocol: Vault" or "Miner: Luxor".
    pub label: Option<String>,
    /// Details of the name tag, when the address is in the label set.
    pub tag: Option<super::labels::Tag>,
    /// Blocks mined by this address within the indexed range.
    pub blocks_mined: i64,
    /// Current balance from the node (zatoshis), covering the full chain history.
    pub balance: i64,
    /// Totals over the indexed range only.
    pub received: i64,
    pub sent: i64,
    pub tx_count: i64,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_seen: Option<DateTime<Utc>>,
    /// First height covered by the index; older history is not listed.
    pub indexed_from: Option<i32>,
    /// Transactions matching the history filters (all of `tx_count` when unfiltered).
    pub filtered_count: i64,
    pub txs: Vec<AddressTx>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct HistoryQuery {
    /// Max items (default 25, max 100).
    pub limit: Option<i64>,
    /// Zero-based page number.
    pub page: Option<i64>,
    /// Filter by method (transaction kind), e.g. `shielding`.
    pub kind: Option<String>,
    /// `in` (balance increased) or `out` (decreased).
    pub direction: Option<String>,
    /// `time` (default) or `amount` (absolute balance change).
    pub sort: Option<String>,
    /// `desc` (default) or `asc`.
    pub order: Option<String>,
}

/// Transparent address balance and activity.
#[utoipa::path(get, path = "/v1/address/{address}", params(("address" = String, Path), HistoryQuery),
    responses((status = 200, body = AddressDetail), (status = 400)), tag = "addresses")]
pub async fn detail(
    State(s): State<AppState>,
    Path(address): Path<String>,
    Query(q): Query<HistoryQuery>,
) -> ApiResult<AddressDetail> {
    let limit = q.limit.unwrap_or(25).clamp(1, 100);
    let offset = q.page.unwrap_or(0).clamp(0, 10_000) * limit;
    if let Some(k) = q.kind.as_deref().filter(|k| !super::tx::KINDS.contains(k)) {
        return Err(ApiError::BadRequest(
            "invalid_kind",
            format!(
                "Unknown kind `{k}`; expected one of: {}",
                super::tx::KINDS.join(", ")
            ),
        ));
    }
    let direction = match q.direction.as_deref() {
        None | Some("") | Some("all") => None,
        Some(d @ ("in" | "out")) => Some(d),
        Some(d) => {
            return Err(ApiError::BadRequest(
                "invalid_direction",
                format!("Unknown direction `{d}`; expected `in` or `out`"),
            ))
        }
    };
    // Whitelisted fragments only: nothing user-supplied is interpolated.
    let dir = if q.order.as_deref() == Some("asc") {
        "ASC"
    } else {
        "DESC"
    };
    let order = match q.sort.as_deref() {
        Some("amount") => format!("abs(delta) {dir}, height DESC, block_index DESC"),
        _ => format!("height {dir}, block_index {dir}"),
    };
    // TEX addresses are indexed under the P2PKH address they pay to.
    let address = tex_to_p2pkh(&address).unwrap_or(address);
    match address_kind(&address) {
        Some(AddressKind::Transparent) => {}
        Some(AddressKind::Shielded) => return Err(ApiError::BadRequest(
            "shielded_address",
            "Shielded addresses are private: their balance and history are not visible on-chain."
                .into(),
        )),
        None => {
            return Err(ApiError::BadRequest(
                "invalid_address",
                "Not a valid Zcash address".into(),
            ))
        }
    }

    let balance = s.lwd.taddr_balance(&address).await?;
    let (received, sent, tx_count, first_seen, last_seen): (
        Option<i64>,
        Option<i64>,
        i64,
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
    ) = sqlx::query_as(
        "WITH x AS (
            SELECT txid, value AS recv, 0::bigint AS sent FROM tx_outputs WHERE address = $1
            UNION ALL
            SELECT txid, 0, value FROM tx_inputs WHERE address = $1)
         SELECT sum(recv)::bigint, sum(sent)::bigint, count(DISTINCT x.txid), min(t.time), max(t.time)
         FROM x JOIN transactions t USING (txid)",
    )
    .bind(&address)
    .fetch_one(&s.db.pool)
    .await?;

    const HISTORY: &str = "WITH h AS (
            SELECT t.txid, t.height, t.time, t.kind, t.block_index, sum(x.delta)::bigint AS delta FROM (
                SELECT txid, value AS delta FROM tx_outputs WHERE address = $1
                UNION ALL
                SELECT txid, -value FROM tx_inputs WHERE address = $1) x
            JOIN transactions t USING (txid)
            GROUP BY t.txid, t.height, t.time, t.kind, t.block_index
         ), f AS (
            SELECT * FROM h
            WHERE ($2::text IS NULL OR kind = $2)
              AND ($3::text IS NULL OR ($3 = 'in' AND delta >= 0) OR ($3 = 'out' AND delta < 0))
         )";
    let txs = sqlx::query_as::<_, AddressTx>(&format!(
        "{HISTORY} SELECT txid, height, time, kind, delta FROM f ORDER BY {order} LIMIT $4 OFFSET $5"
    ))
    .bind(&address)
    .bind(&q.kind)
    .bind(direction)
    .bind(limit)
    .bind(offset)
    .fetch_all(&s.db.pool)
    .await?;
    let filtered_count: i64 = sqlx::query_scalar(&format!("{HISTORY} SELECT count(*) FROM f"))
        .bind(&address)
        .bind(&q.kind)
        .bind(direction)
        .fetch_one(&s.db.pool)
        .await?;

    let (blocks_mined, pool): (i64, Option<String>) = sqlx::query_as(
        "SELECT count(*), mode() WITHIN GROUP (ORDER BY miner_pool) FILTER (WHERE miner_pool IS NOT NULL)
         FROM blocks WHERE miner = $1",
    )
    .bind(&address)
    .fetch_one(&s.db.pool)
    .await?;
    let tag = super::labels::lookup(&s.db, std::slice::from_ref(&address))
        .await?
        .remove(&address);
    let label = tag.as_ref().map(|t| t.name.clone()).or_else(|| {
        (blocks_mined > 0).then(|| {
            format!(
                "Miner{}",
                pool.map(|p| format!(": {p}")).unwrap_or_default()
            )
        })
    });

    let indexed_from: Option<i32> = sqlx::query_scalar("SELECT low FROM sync_state")
        .fetch_one(&s.db.pool)
        .await?;

    Ok(Json(AddressDetail {
        address,
        label,
        tag,
        blocks_mined,
        balance,
        received: received.unwrap_or(0),
        sent: sent.unwrap_or(0),
        tx_count,
        first_seen,
        last_seen,
        indexed_from,
        filtered_count,
        txs,
    }))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct UnifiedAddress {
    pub address: String,
    pub network: String,
    pub receivers: Vec<UnifiedReceiver>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct UnifiedReceiver {
    /// transparent_p2pkh | transparent_p2sh | sapling | orchard | unknown
    pub kind: String,
    pub pools: String,
    /// Encoded address for transparent receivers.
    pub address: Option<String>,
    pub raw: String,
}

/// Decodes a Unified Address into its receivers (which pools it can be paid in).
#[utoipa::path(get, path = "/v1/unified/{address}", params(("address" = String, Path)),
    responses((status = 200, body = UnifiedAddress), (status = 400)), tag = "addresses")]
pub async fn unified(Path(address): Path<String>) -> ApiResult<UnifiedAddress> {
    let info = zscan_chain::unified::inspect(&address).ok_or_else(|| {
        ApiError::BadRequest(
            "invalid_unified_address",
            "Not a valid Unified Address".into(),
        )
    })?;
    Ok(Json(UnifiedAddress {
        address,
        network: info.network,
        receivers: info
            .receivers
            .into_iter()
            .map(|r| UnifiedReceiver {
                kind: r.kind,
                pools: r.pools,
                address: r.address,
                raw: r.raw,
            })
            .collect(),
    }))
}

/// Transaction history of a transparent address as CSV (indexed range, up to 10,000 rows).
#[utoipa::path(get, path = "/v1/address/{address}/csv", params(("address" = String, Path)),
    responses((status = 200, content_type = "text/csv")), tag = "addresses")]
pub async fn csv(
    State(s): State<AppState>,
    Path(address): Path<String>,
) -> Result<axum::response::Response, ApiError> {
    use axum::response::IntoResponse;
    let address = tex_to_p2pkh(&address).unwrap_or(address);
    if address_kind(&address) != Some(AddressKind::Transparent) {
        return Err(ApiError::BadRequest(
            "invalid_address",
            "Transparent address required".into(),
        ));
    }
    let rows: Vec<(String, i32, DateTime<Utc>, String, i64)> = sqlx::query_as(
        "SELECT t.txid, t.height, t.time, t.kind, sum(x.delta)::bigint FROM (
            SELECT txid, value AS delta FROM tx_outputs WHERE address = $1
            UNION ALL SELECT txid, -value FROM tx_inputs WHERE address = $1) x
         JOIN transactions t USING (txid)
         GROUP BY t.txid, t.height, t.time, t.kind, t.block_index
         ORDER BY t.height DESC, t.block_index DESC LIMIT 10000",
    )
    .bind(&address)
    .fetch_all(&s.db.pool)
    .await?;
    let mut out = String::from("txid,block,timestamp_utc,type,direction,amount_zec\n");
    for (txid, h, t, kind, delta) in rows {
        out.push_str(&format!(
            "{txid},{h},{},{kind},{},{}\n",
            t.format("%Y-%m-%d %H:%M:%S"),
            if delta >= 0 { "IN" } else { "OUT" },
            format_zec(delta.abs())
        ));
    }
    Ok((
        [
            (
                axum::http::header::CONTENT_TYPE,
                "text/csv; charset=utf-8".to_string(),
            ),
            (
                axum::http::header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"zscan-{address}.csv\""),
            ),
        ],
        out,
    )
        .into_response())
}

fn format_zec(zat: i64) -> String {
    format!("{}.{:08}", zat / 100_000_000, zat % 100_000_000)
}
