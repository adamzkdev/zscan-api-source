//! Inscriptions (Zerdinals / Zordinals): content inscribed in transparent scriptSigs, owned by
//! transparent outputs. Projected by the indexer's `inscriptions` scan.

use std::collections::HashSet;
use std::sync::OnceLock;

use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use zscan_chain::inscriptions::{format_amount, Engine, FIRST_HEIGHT};

use crate::error::{ApiError, ApiResult};
use crate::AppState;

const ROW_COLUMNS: &str = "id, number, content_type, media, content_len, txid, height, time,
    owner, state, zrc20, transfers, hidden,
    CASE WHEN media IN ('text', 'json') THEN substring(content FROM 1 FOR 280) END AS preview_bytes";

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct InscriptionRow {
    /// Genesis txid + `i0`.
    pub id: String,
    /// Sequence number, by completion block then position in the block (from 1).
    pub number: i64,
    pub content_type: String,
    /// image, svg, html, text, json, audio, video, other.
    pub media: String,
    pub content_len: i32,
    /// Completion transaction.
    pub txid: String,
    pub height: i32,
    pub time: DateTime<Utc>,
    /// Current owner; `None` once shielded or burned.
    pub owner: Option<String>,
    /// transparent | shielded | burned
    pub state: String,
    /// ZRC-20 op carried by the body (deploy, mint, transfer).
    pub zrc20: Option<String>,
    pub transfers: i32,
    /// Moderated: content is not served.
    pub hidden: bool,
    /// Start of the body, for text and JSON.
    #[sqlx(skip)]
    pub preview: Option<String>,
    #[serde(skip)]
    #[schema(ignore)]
    preview_bytes: Option<Vec<u8>>,
}

/// Ids hidden by the operator (`INSCRIPTION_BLOCKLIST`, comma-separated), besides the
/// `hidden` column.
fn blocklist() -> &'static HashSet<String> {
    static B: OnceLock<HashSet<String>> = OnceLock::new();
    B.get_or_init(|| {
        std::env::var("INSCRIPTION_BLOCKLIST")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect()
    })
}

/// Applies the blocklist and fills the text preview.
fn moderate(mut r: InscriptionRow) -> InscriptionRow {
    r.hidden |= blocklist().contains(&r.id);
    let bytes = r.preview_bytes.take();
    if !r.hidden {
        r.preview = bytes.map(|b| String::from_utf8_lossy(&b).into_owned());
    }
    r
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ListQuery {
    /// image, svg, html, text, json, audio, video, other; `art` = image + svg + html.
    pub media: Option<String>,
    /// transparent | shielded | burned
    pub state: Option<String>,
    /// `desc` (newest first, default) or `asc`.
    pub order: Option<String>,
    /// Continue after this inscription number (the previous page's `next`).
    pub cursor: Option<i64>,
    /// Max items (default 48, max 200).
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct InscriptionPage {
    pub items: Vec<InscriptionRow>,
    /// Cursor for the next page, if any.
    pub next: Option<i64>,
}

const MEDIA: [&str; 8] = [
    "image", "svg", "html", "text", "json", "audio", "video", "other",
];

/// Inscriptions, newest first, filterable by media family and state.
#[utoipa::path(get, path = "/v1/inscriptions", params(ListQuery),
    responses((status = 200, body = InscriptionPage)), tag = "inscriptions")]
pub async fn list(
    State(s): State<AppState>,
    Query(q): Query<ListQuery>,
) -> ApiResult<InscriptionPage> {
    let limit = q.limit.unwrap_or(48).clamp(1, 200);
    let media: Vec<&str> = match q.media.as_deref() {
        None | Some("") | Some("all") => vec![],
        Some("art") => vec!["image", "svg", "html"],
        Some(m) if MEDIA.contains(&m) => vec![m],
        Some(m) => {
            return Err(ApiError::BadRequest(
                "invalid_media",
                format!(
                    "Unknown media `{m}`; expected art or one of: {}",
                    MEDIA.join(", ")
                ),
            ))
        }
    };
    if let Some(st) = q.state.as_deref() {
        if !["transparent", "shielded", "burned"].contains(&st) {
            return Err(ApiError::BadRequest(
                "invalid_state",
                "state must be transparent, shielded or burned".into(),
            ));
        }
    }
    let asc = q.order.as_deref() == Some("asc");
    let sql = format!(
        "SELECT {ROW_COLUMNS} FROM inscriptions
         WHERE (cardinality($1::text[]) = 0 OR media = ANY($1))
           AND ($2::text IS NULL OR state = $2)
           AND ($3::bigint IS NULL OR number {cmp} $3)
         ORDER BY number {dir} LIMIT $4",
        cmp = if asc { ">" } else { "<" },
        dir = if asc { "ASC" } else { "DESC" },
    );
    let mut items: Vec<InscriptionRow> = sqlx::query_as(&sql)
        .bind(&media)
        .bind(&q.state)
        .bind(q.cursor)
        .bind(limit + 1)
        .fetch_all(&s.db.pool)
        .await?;
    let next = (items.len() as i64 > limit).then(|| {
        items.truncate(limit as usize);
        items.last().map(|r| r.number)
    });
    Ok(Json(InscriptionPage {
        items: items.into_iter().map(moderate).collect(),
        next: next.flatten(),
    }))
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct CountBy {
    pub key: String,
    pub count: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct InscriptionStats {
    pub total: i64,
    pub by_media: Vec<CountBy>,
    pub by_state: Vec<CountBy>,
    /// Distinct current owners.
    pub holders: i64,
    pub zrc20_tokens: i64,
    /// Reveal chains waiting for their next transaction.
    pub pending: i64,
    /// Next height the scan reads (it trails the tip by a few blocks).
    pub scan_height: Option<i32>,
    pub first_height: u32,
    pub tip: Option<i32>,
}

/// Inscription totals by media family and state, and the scan's progress.
#[utoipa::path(get, path = "/v1/inscriptions/stats",
    responses((status = 200, body = InscriptionStats)), tag = "inscriptions")]
pub async fn stats(State(s): State<AppState>) -> ApiResult<InscriptionStats> {
    let db = &s.db.pool;
    let by_media: Vec<CountBy> = sqlx::query_as(
        "SELECT media AS key, count(*) AS count FROM inscriptions GROUP BY 1 ORDER BY 2 DESC",
    )
    .fetch_all(db)
    .await?;
    let by_state: Vec<CountBy> = sqlx::query_as(
        "SELECT state AS key, count(*) AS count FROM inscriptions GROUP BY 1 ORDER BY 2 DESC",
    )
    .fetch_all(db)
    .await?;
    let (holders, zrc20_tokens, pending, scan_height): (i64, i64, i64, Option<i32>) =
        sqlx::query_as(
            "SELECT (SELECT count(DISTINCT owner) FROM inscriptions WHERE owner IS NOT NULL),
                    (SELECT count(*) FROM zrc20_tokens),
                    (SELECT count(*) FROM inscription_pending),
                    (SELECT height FROM inscription_sync)",
        )
        .fetch_one(db)
        .await?;
    Ok(Json(InscriptionStats {
        total: by_media.iter().map(|c| c.count).sum(),
        by_media,
        by_state,
        holders,
        zrc20_tokens,
        pending,
        scan_height,
        first_height: FIRST_HEIGHT,
        tip: super::tip(&s.db).await?,
    }))
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct InscriptionEvent {
    /// inscribed | transfer | shielded | burned
    pub kind: String,
    pub txid: String,
    pub height: i32,
    pub time: DateTime<Utc>,
    pub from_addr: Option<String>,
    pub to_addr: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct InscriptionDetail {
    #[serde(flatten)]
    pub row: InscriptionRow,
    pub genesis_height: i32,
    pub sha256: String,
    /// Reveal transactions, genesis first; the last one completes the inscription.
    pub reveal_txids: Vec<String>,
    /// Current carrying output `txid:vout`, while transparent.
    pub location: Option<String>,
    /// UTF-8 body for text and JSON inscriptions up to 16 KB.
    pub text: Option<String>,
    pub events: Vec<InscriptionEvent>,
    /// ZRC-20 operations tied to this inscription.
    pub zrc20_events: Vec<super::zrc20::ZrcEvent>,
    pub prev: Option<String>,
    pub next: Option<String>,
}

/// The inscription with this id (`<txid>i0`) or number.
async fn find(s: &AppState, id: &str) -> Result<InscriptionRow, ApiError> {
    let id = id.trim().to_lowercase();
    let row: Option<InscriptionRow> = if let Ok(n) = id.trim_start_matches('#').parse::<i64>() {
        sqlx::query_as(&format!(
            "SELECT {ROW_COLUMNS} FROM inscriptions WHERE number = $1"
        ))
        .bind(n)
        .fetch_optional(&s.db.pool)
        .await?
    } else {
        let full = if id.len() == 64 {
            format!("{id}i0")
        } else {
            id.clone()
        };
        sqlx::query_as(&format!(
            "SELECT {ROW_COLUMNS} FROM inscriptions WHERE id = $1"
        ))
        .bind(&full)
        .fetch_optional(&s.db.pool)
        .await?
    };
    row.map(moderate)
        .ok_or_else(|| ApiError::NotFound(format!("Inscription {id} not found")))
}

/// One inscription: metadata, provenance, and ZRC-20 effects.
#[utoipa::path(get, path = "/v1/inscriptions/{id}", params(("id" = String, Path, description = "`<txid>i0`, txid or number")),
    responses((status = 200, body = InscriptionDetail), (status = 404)), tag = "inscriptions")]
pub async fn detail(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<InscriptionDetail> {
    let row = find(&s, &id).await?;
    let db = &s.db.pool;
    type Extra = (
        i32,
        String,
        Vec<String>,
        Option<String>,
        Option<i32>,
        Option<Vec<u8>>,
    );
    let (genesis_height, sha256, reveal_txids, loc_txid, loc_vout, content): Extra =
        sqlx::query_as(
            "SELECT genesis_height, sha256, reveal_txids, location_txid, location_vout,
                CASE WHEN media IN ('text', 'json') AND content_len <= 16384 THEN content END
             FROM inscriptions WHERE id = $1",
        )
        .bind(&row.id)
        .fetch_one(db)
        .await?;
    let events: Vec<InscriptionEvent> = sqlx::query_as(
        "SELECT kind, txid, height, time, from_addr, to_addr FROM inscription_events
         WHERE inscription_id = $1 ORDER BY height, kind <> 'inscribed'",
    )
    .bind(&row.id)
    .fetch_all(db)
    .await?;
    let zrc20_events = super::zrc20::events_where(&s, "e.inscription_id = $1", &row.id, 50).await?;
    let (prev, next): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT (SELECT id FROM inscriptions WHERE number = $1 - 1),
                (SELECT id FROM inscriptions WHERE number = $1 + 1)",
    )
    .bind(row.number)
    .fetch_one(db)
    .await?;
    let text = content
        .filter(|_| !row.hidden)
        .and_then(|c| String::from_utf8(c).ok());
    Ok(Json(InscriptionDetail {
        genesis_height,
        sha256,
        reveal_txids,
        location: loc_txid.zip(loc_vout).map(|(t, v)| format!("{t}:{v}")),
        text,
        events,
        zrc20_events,
        prev,
        next,
        row,
    }))
}

/// Content types are served as inscribed, unless they could smuggle header syntax.
fn safe_content_type(ct: &str) -> HeaderValue {
    let ok = ct.len() <= 96 && ct.bytes().all(|b| b.is_ascii_graphic() || b == b' ');
    ok.then(|| HeaderValue::from_str(ct).ok())
        .flatten()
        .unwrap_or(HeaderValue::from_static("application/octet-stream"))
}

/// Inscribed bytes, served with their content type in a sandbox: an opaque origin with no
/// network access, so HTML and SVG can't reach ZScan cookies, storage or the outside.
#[utoipa::path(get, path = "/v1/inscriptions/{id}/content", params(("id" = String, Path)),
    responses((status = 200, description = "Raw content"), (status = 404), (status = 451, description = "Hidden by moderation")),
    tag = "inscriptions")]
pub async fn content(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let row = find(&s, &id).await?;
    if row.hidden {
        return Ok((
            StatusCode::UNAVAILABLE_FOR_LEGAL_REASONS,
            "This inscription is hidden on ZScan.",
        )
            .into_response());
    }
    let (content, reveal_txids, sha256): (Option<Vec<u8>>, Vec<String>, String) =
        sqlx::query_as("SELECT content, reveal_txids, sha256 FROM inscriptions WHERE id = $1")
            .bind(&row.id)
            .fetch_one(&s.db.pool)
            .await?;
    let body = match content {
        Some(c) => c,
        None => rebuild(&s, &reveal_txids, &sha256).await?,
    };
    let headers = [
        (header::CONTENT_TYPE, safe_content_type(&row.content_type)),
        (
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(
                "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; \
                 img-src data: blob:; font-src data:; media-src data: blob:; sandbox allow-scripts",
            ),
        ),
        (
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ),
        (
            header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=31536000, immutable"),
        ),
        (
            header::HeaderName::from_static("cross-origin-resource-policy"),
            HeaderValue::from_static("cross-origin"),
        ),
    ];
    Ok((headers, body).into_response())
}

/// Reassembles content that wasn't stored, by replaying its reveal transactions.
async fn rebuild(s: &AppState, reveal_txids: &[String], sha256: &str) -> Result<Vec<u8>, ApiError> {
    let mut engine = Engine::new();
    for (k, txid) in reveal_txids.iter().enumerate() {
        let raw =
            s.lwd.raw_tx(txid).await?.ok_or_else(|| {
                ApiError::NotFound(format!("Reveal transaction {txid} not found"))
            })?;
        let tx = zscan_chain::parse_tx(&raw.data)?;
        engine.apply_tx(raw.height as u32, 0, k as u32, &tx);
    }
    let insc = engine
        .out
        .inscriptions
        .pop()
        .filter(|i| i.sha256 == sha256)
        .ok_or_else(|| anyhow::anyhow!("content rebuild mismatch"))?;
    Ok(insc.content)
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AddressInscriptions {
    pub address: String,
    /// Inscriptions currently owned, newest first (up to 200).
    pub inscriptions: Vec<InscriptionRow>,
    pub inscription_count: i64,
    pub zrc20: Vec<super::zrc20::Holding>,
}

/// Inscriptions and ZRC-20 balances held by a transparent address.
#[utoipa::path(get, path = "/v1/address/{address}/inscriptions", params(("address" = String, Path)),
    responses((status = 200, body = AddressInscriptions)), tag = "inscriptions")]
pub async fn by_address(
    State(s): State<AppState>,
    Path(address): Path<String>,
) -> ApiResult<AddressInscriptions> {
    let address = zscan_chain::parse::tex_to_p2pkh(&address).unwrap_or(address);
    let db = &s.db.pool;
    let inscriptions: Vec<InscriptionRow> = sqlx::query_as(&format!(
        "SELECT {ROW_COLUMNS} FROM inscriptions WHERE owner = $1 ORDER BY number DESC LIMIT 200"
    ))
    .bind(&address)
    .fetch_all(db)
    .await?;
    let inscription_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM inscriptions WHERE owner = $1")
            .bind(&address)
            .fetch_one(db)
            .await?;
    let rows: Vec<(String, i16, String, String)> = sqlx::query_as(
        "SELECT b.tick, t.dec, b.available::text, b.transferable::text
         FROM zrc20_balances b JOIN zrc20_tokens t USING (tick)
         WHERE b.address = $1 ORDER BY b.tick",
    )
    .bind(&address)
    .fetch_all(db)
    .await?;
    let zrc20 = rows
        .into_iter()
        .map(|(tick, dec, a, t)| {
            let (a, t): (u128, u128) = (a.parse().unwrap_or(0), t.parse().unwrap_or(0));
            super::zrc20::Holding {
                tick,
                available: format_amount(a, dec as u8),
                transferable: format_amount(t, dec as u8),
                balance: format_amount(a + t, dec as u8),
            }
        })
        .collect();
    Ok(Json(AddressInscriptions {
        address,
        inscriptions: inscriptions.into_iter().map(moderate).collect(),
        inscription_count,
        zrc20,
    }))
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct TxInscription {
    pub id: String,
    pub number: i64,
    pub content_type: String,
    pub media: String,
    /// inscribed | transfer | shielded | burned
    pub kind: String,
    pub from_addr: Option<String>,
    pub to_addr: Option<String>,
}

/// Inscriptions created or moved by a transaction.
pub async fn for_tx(s: &AppState, txid: &str) -> anyhow::Result<Vec<TxInscription>> {
    Ok(sqlx::query_as(
        "SELECT i.id, i.number, i.content_type, i.media, e.kind, e.from_addr, e.to_addr
         FROM inscription_events e JOIN inscriptions i ON i.id = e.inscription_id
         WHERE e.txid = $1 ORDER BY i.number",
    )
    .bind(txid)
    .fetch_all(&s.db.pool)
    .await?)
}
