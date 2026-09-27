//! Inscriptions (Zerdinals / Zordinals): content inscribed in transparent scriptSigs, owned by
//! transparent outputs, and the ZRC-721 collections built on them. Projected by the indexer's
//! `inscriptions` scan; token bookkeeping documents (ZRC-20) are numbered but not indexed.

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
use zscan_chain::inscriptions::{Engine, FIRST_HEIGHT};

use crate::error::{ApiError, ApiResult};
use crate::{ipfs, AppState};

const ROW_COLUMNS: &str = "id, number, content_type, media, content_len, txid, height, time,
    owner, state, transfers, hidden, collection, item,
    CASE WHEN media IN ('text', 'json') THEN substring(content FROM 1 FOR 280) END AS preview_bytes";

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct InscriptionRow {
    /// Genesis txid + `i0`.
    pub id: String,
    /// Sequence number, by completion block then position in the block (from 1). Token
    /// documents take numbers too, so indexed numbers have gaps.
    pub number: i64,
    pub content_type: String,
    /// On chain: image, svg, html, text, json, audio, video, other. NFTs with an IPFS image:
    /// collection (ZRC-721 item), ipfs (IPFS pointer).
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
    pub transfers: i32,
    /// Moderated: content is not served.
    pub hidden: bool,
    /// ZRC-721 collection (items and deploys).
    pub collection: Option<String>,
    /// ZRC-721 item id within the collection.
    pub item: Option<String>,
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
    /// `nft` (default: images, SVG, HTML, collection items, IPFS NFTs), `all`, or one family:
    /// image, svg, html, collection, ipfs, text, json, audio, video, other.
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

const MEDIA: [&str; 10] = [
    "image",
    "svg",
    "html",
    "collection",
    "ipfs",
    "text",
    "json",
    "audio",
    "video",
    "other",
];
const NFT: [&str; 5] = ["image", "svg", "html", "collection", "ipfs"];

/// Inscriptions, newest first. NFTs by default.
#[utoipa::path(get, path = "/v1/inscriptions", params(ListQuery),
    responses((status = 200, body = InscriptionPage)), tag = "inscriptions")]
pub async fn list(
    State(s): State<AppState>,
    Query(q): Query<ListQuery>,
) -> ApiResult<InscriptionPage> {
    let limit = q.limit.unwrap_or(48).clamp(1, 200);
    let media: Vec<&str> = match q.media.as_deref() {
        None | Some("") | Some("nft") => NFT.to_vec(),
        Some("all") => vec![],
        Some(m) if MEDIA.contains(&m) => vec![m],
        Some(m) => {
            return Err(ApiError::BadRequest(
                "invalid_media",
                format!(
                    "Unknown media `{m}`; expected nft, all or one of: {}",
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
    /// Indexed inscriptions (everything but token documents).
    pub total: i64,
    /// All inscriptions numbered so far, token documents included.
    pub numbered: i64,
    pub by_media: Vec<CountBy>,
    pub by_state: Vec<CountBy>,
    /// Distinct current owners.
    pub holders: i64,
    pub collections: i64,
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
    let (holders, collections, pending, scan_height, next): (
        i64,
        i64,
        i64,
        Option<i32>,
        Option<i64>,
    ) = sqlx::query_as(
        "SELECT (SELECT count(DISTINCT owner) FROM inscriptions WHERE owner IS NOT NULL),
                (SELECT count(DISTINCT collection) FROM inscriptions WHERE media = 'collection'),
                (SELECT count(*) FROM inscription_pending),
                (SELECT height FROM inscription_sync),
                (SELECT next_number FROM inscription_sync)",
    )
    .fetch_one(db)
    .await?;
    Ok(Json(InscriptionStats {
        total: by_media.iter().map(|c| c.count).sum(),
        numbered: next.unwrap_or(1) - 1,
        by_media,
        by_state,
        holders,
        collections,
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
pub struct Attribute {
    pub trait_type: String,
    pub value: String,
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
    /// IPFS location of the image, for collection items and IPFS NFTs.
    pub ipfs: Option<String>,
    /// From the collection's IPFS metadata, when it can be read.
    pub name: Option<String>,
    pub description: Option<String>,
    pub attributes: Vec<Attribute>,
    pub events: Vec<InscriptionEvent>,
    /// Neighbouring indexed inscriptions.
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

/// Where the image of a collection item or IPFS NFT lives, and its IPFS metadata if any.
async fn ipfs_source(
    s: &AppState,
    row: &InscriptionRow,
) -> Result<(Option<String>, Option<String>, Option<serde_json::Value>), ApiError> {
    match row.media.as_str() {
        "ipfs" => {
            let (cid, mime): (Option<String>, Option<String>) =
                sqlx::query_as("SELECT ipfs, ipfs_mime FROM inscriptions WHERE id = $1")
                    .bind(&row.id)
                    .fetch_one(&s.db.pool)
                    .await?;
            Ok((cid, mime, None))
        }
        "collection" => {
            let meta: Option<String> =
                sqlx::query_scalar("SELECT meta FROM collections WHERE name = $1")
                    .bind(&row.collection)
                    .fetch_optional(&s.db.pool)
                    .await?
                    .flatten();
            let (Some(meta), Some(item)) = (meta, row.item.as_deref()) else {
                return Ok((None, None, None));
            };
            let md = ipfs::metadata(&meta, item).await;
            let image = md.as_ref().and_then(ipfs::image_of);
            Ok((image, None, md))
        }
        _ => Ok((None, None, None)),
    }
}

/// One inscription: metadata, provenance and, for NFTs with an IPFS image, their metadata.
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
                CASE WHEN media IN ('text', 'json', 'collection', 'ipfs')
                     AND content_len <= 16384 THEN content END
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
    let (prev, next): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT (SELECT id FROM inscriptions WHERE number < $1 ORDER BY number DESC LIMIT 1),
                (SELECT id FROM inscriptions WHERE number > $1 ORDER BY number LIMIT 1)",
    )
    .bind(row.number)
    .fetch_one(db)
    .await?;
    let (image, _, md) = ipfs_source(&s, &row).await?;
    let md_str = |k: &str| {
        md.as_ref()
            .and_then(|m| m.get(k)?.as_str())
            .map(|v| v.chars().take(1000).collect::<String>())
    };
    let attributes = md
        .as_ref()
        .and_then(|m| m.get("attributes")?.as_array().cloned())
        .unwrap_or_default()
        .iter()
        .take(50)
        .filter_map(|a| {
            let value = match a.get("value")? {
                serde_json::Value::String(s) => s.clone(),
                v => v.to_string(),
            };
            Some(Attribute {
                trait_type: a.get("trait_type")?.as_str()?.chars().take(100).collect(),
                value: value.chars().take(200).collect(),
            })
        })
        .collect();
    let text = content
        .filter(|_| !row.hidden)
        .and_then(|c| String::from_utf8(c).ok());
    Ok(Json(InscriptionDetail {
        genesis_height,
        sha256,
        reveal_txids,
        location: loc_txid.zip(loc_vout).map(|(t, v)| format!("{t}:{v}")),
        text,
        ipfs: image,
        name: md_str("name"),
        description: md_str("description"),
        attributes,
        events,
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

/// Serves untrusted bytes in a sandbox: an opaque origin with no network access, so HTML and
/// SVG can't reach ZScan cookies, storage or the outside. Immutable, so cached for a year.
fn sandboxed(content_type: HeaderValue, body: Vec<u8>) -> Response {
    let headers = [
        (header::CONTENT_TYPE, content_type),
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
    (headers, body).into_response()
}

fn hidden() -> Response {
    (
        StatusCode::UNAVAILABLE_FOR_LEGAL_REASONS,
        "This inscription is hidden on ZScan.",
    )
        .into_response()
}

/// Inscribed bytes, served with their content type in a sandbox.
#[utoipa::path(get, path = "/v1/inscriptions/{id}/content", params(("id" = String, Path)),
    responses((status = 200, description = "Raw content"), (status = 404), (status = 451, description = "Hidden by moderation")),
    tag = "inscriptions")]
pub async fn content(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let row = find(&s, &id).await?;
    if row.hidden {
        return Ok(hidden());
    }
    let body = inscribed_bytes(&s, &row.id).await?;
    Ok(sandboxed(safe_content_type(&row.content_type), body))
}

async fn inscribed_bytes(s: &AppState, id: &str) -> Result<Vec<u8>, ApiError> {
    let (content, reveal_txids, sha256): (Option<Vec<u8>>, Vec<String>, String) =
        sqlx::query_as("SELECT content, reveal_txids, sha256 FROM inscriptions WHERE id = $1")
            .bind(id)
            .fetch_one(&s.db.pool)
            .await?;
    match content {
        Some(c) => Ok(c),
        None => rebuild(s, &reveal_txids, &sha256).await,
    }
}

/// The picture of an NFT: the inscribed image, or for collection items and IPFS NFTs the image
/// on IPFS, relayed so visitors never contact a gateway.
#[utoipa::path(get, path = "/v1/inscriptions/{id}/image", params(("id" = String, Path)),
    responses((status = 200, description = "Image"), (status = 404), (status = 451), (status = 502, description = "IPFS unavailable")),
    tag = "inscriptions")]
pub async fn image(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let row = find(&s, &id).await?;
    if row.hidden {
        return Ok(hidden());
    }
    if matches!(row.media.as_str(), "image" | "svg") {
        let body = inscribed_bytes(&s, &row.id).await?;
        return Ok(sandboxed(safe_content_type(&row.content_type), body));
    }
    let (Some(path), declared, _) = ipfs_source(&s, &row).await? else {
        return Err(ApiError::NotFound(format!(
            "Inscription {} has no image",
            row.id
        )));
    };
    match ipfs::image(&path, declared.as_deref()).await {
        Ok((ctype, body)) => Ok(sandboxed(safe_content_type(&ctype), body)),
        Err(e) => {
            tracing::warn!("ipfs image {path}: {e:#}");
            Ok((StatusCode::BAD_GATEWAY, "IPFS image unavailable").into_response())
        }
    }
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
}

/// Inscriptions held by a transparent address.
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
    Ok(Json(AddressInscriptions {
        address,
        inscriptions: inscriptions.into_iter().map(moderate).collect(),
        inscription_count,
    }))
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct TxInscription {
    pub id: String,
    pub number: i64,
    pub content_type: String,
    pub media: String,
    pub collection: Option<String>,
    pub item: Option<String>,
    /// inscribed | transfer | shielded | burned
    pub kind: String,
    pub from_addr: Option<String>,
    pub to_addr: Option<String>,
}

/// Inscriptions created or moved by a transaction.
pub async fn for_tx(s: &AppState, txid: &str) -> anyhow::Result<Vec<TxInscription>> {
    Ok(sqlx::query_as(
        "SELECT i.id, i.number, i.content_type, i.media, i.collection, i.item, e.kind,
                e.from_addr, e.to_addr
         FROM inscription_events e JOIN inscriptions i ON i.id = e.inscription_id
         WHERE e.txid = $1 ORDER BY i.number",
    )
    .bind(txid)
    .fetch_all(&s.db.pool)
    .await?)
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct CollectionRow {
    /// Uppercase name.
    pub name: String,
    /// Declared by the deploy.
    pub supply: Option<i64>,
    /// Items inscribed (every mint counts, duplicates included).
    pub items: i64,
    /// Distinct current owners.
    pub holders: i64,
    /// Items whose tracking ended in a shielded transaction.
    pub shielded: i64,
    /// Whether a deploy defines the collection (its IPFS metadata, supply).
    pub deployed: bool,
    pub deploy_inscription: Option<String>,
    pub deployer: Option<String>,
    /// First item, for a cover image.
    pub cover: Option<String>,
    pub first_time: DateTime<Utc>,
    pub last_time: DateTime<Utc>,
}

const COLLECTION_SQL: &str = "
    WITH items AS (
        SELECT collection AS name, count(*) AS items,
               count(DISTINCT owner) AS holders,
               count(*) FILTER (WHERE state = 'shielded') AS shielded,
               (array_agg(id ORDER BY number))[1] AS cover,
               min(time) AS first_time, max(time) AS last_time
        FROM inscriptions WHERE media = 'collection' GROUP BY collection)
    SELECT i.name, c.supply, i.items, i.holders, i.shielded, c.name IS NOT NULL AS deployed,
           c.deploy_inscription, c.deployer, i.cover, i.first_time, i.last_time
    FROM items i LEFT JOIN collections c ON c.name = i.name";

/// ZRC-721 collections, largest first.
#[utoipa::path(get, path = "/v1/collections",
    responses((status = 200, body = [CollectionRow])), tag = "inscriptions")]
pub async fn collections(State(s): State<AppState>) -> ApiResult<Vec<CollectionRow>> {
    let rows = sqlx::query_as(&format!(
        "{COLLECTION_SQL} ORDER BY i.items DESC, i.name LIMIT 500"
    ))
    .fetch_all(&s.db.pool)
    .await?;
    Ok(Json(rows))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct CollectionQuery {
    /// Items after this inscription number (the previous page's `next`).
    pub cursor: Option<i64>,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct Holder {
    pub address: String,
    pub count: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CollectionDetail {
    #[serde(flatten)]
    pub collection: CollectionRow,
    /// IPFS directory of the item metadata.
    pub meta: Option<String>,
    /// Items in inscription order, 48 per page.
    pub items_page: Vec<InscriptionRow>,
    pub next: Option<i64>,
    /// Top 25 holders.
    pub holders_list: Vec<Holder>,
}

/// One ZRC-721 collection: items, holders and its deploy.
#[utoipa::path(get, path = "/v1/collections/{name}", params(("name" = String, Path), CollectionQuery),
    responses((status = 200, body = CollectionDetail), (status = 404)), tag = "inscriptions")]
pub async fn collection(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Query(q): Query<CollectionQuery>,
) -> ApiResult<CollectionDetail> {
    let name = name.trim().to_uppercase();
    let db = &s.db.pool;
    let collection: CollectionRow = sqlx::query_as(&format!("{COLLECTION_SQL} WHERE i.name = $1"))
        .bind(&name)
        .fetch_optional(db)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("Collection {name} not found")))?;
    let meta: Option<String> = sqlx::query_scalar("SELECT meta FROM collections WHERE name = $1")
        .bind(&name)
        .fetch_optional(db)
        .await?
        .flatten();
    let mut items: Vec<InscriptionRow> = sqlx::query_as(&format!(
        "SELECT {ROW_COLUMNS} FROM inscriptions
         WHERE media = 'collection' AND collection = $1 AND ($2::bigint IS NULL OR number > $2)
         ORDER BY number LIMIT 49"
    ))
    .bind(&name)
    .bind(q.cursor)
    .fetch_all(db)
    .await?;
    let next = (items.len() > 48).then(|| {
        items.truncate(48);
        items.last().map(|r| r.number)
    });
    let holders_list = sqlx::query_as(
        "SELECT owner AS address, count(*) AS count FROM inscriptions
         WHERE media = 'collection' AND collection = $1 AND owner IS NOT NULL
         GROUP BY owner ORDER BY 2 DESC, 1 LIMIT 25",
    )
    .bind(&name)
    .fetch_all(db)
    .await?;
    Ok(Json(CollectionDetail {
        collection,
        meta,
        items_page: items.into_iter().map(moderate).collect(),
        next: next.flatten(),
        holders_list,
    }))
}
