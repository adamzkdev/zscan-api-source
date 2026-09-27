//! NFT collections: projects from the registry (`labels/nft_projects.json`) joined with what
//! ZScan detects on chain (ZRC-721 collections and launchpad drops), marketplace figures and
//! on-chain sales. Detected collections that no project claims are listed too.

use axum::{
    extract::{Path, Query, State},
    response::Response,
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use zscan_chain::nft_projects::{self, Project};

use super::inscriptions::{moderate, InscriptionRow, ROW_COLUMNS};
use crate::error::{ApiError, ApiResult};
use crate::nft_markets::{self, VenueItem, VenueItemDetail, VenueSale, VenueStats};
use crate::{ipfs, AppState};

/// Fewest items for a launchpad drop to be listed as a collection.
pub const MIN_DROP: i64 = 20;
/// Fewest items for a ZRC-721 collection no project claims to be listed (skips test deploys).
const MIN_UNCLAIMED_ZRC721: i64 = 5;

/// Display name of a drop no project claims.
pub fn drop_name(key: &str) -> String {
    format!("Drop by {}…{}", &key[..6], &key[key.len() - 4..])
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Link {
    pub label: String,
    pub url: String,
}

fn links(l: &[nft_projects::Link]) -> Vec<Link> {
    l.iter()
        .map(|l| Link {
            label: l.label.clone(),
            url: l.url.clone(),
        })
        .collect()
}

/// A registry project, as shown with its collection.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ProjectInfo {
    pub description: String,
    pub website: Option<String>,
    pub x: Option<String>,
    pub discord: Option<String>,
    pub telegram: Option<String>,
    pub minted_at: Option<String>,
    pub mint_price_zec: Option<f64>,
    /// Places it trades without a supported API.
    pub marketplaces: Vec<Link>,
    /// verified | likely
    pub confidence: String,
    pub sources: Vec<Link>,
}

impl From<&Project> for ProjectInfo {
    fn from(p: &Project) -> Self {
        let mut marketplaces = links(&p.marketplaces);
        if let Some(m) = &p.market {
            if !marketplaces.iter().any(|l| l.url == m.url) {
                let label = reqwest::Url::parse(&m.url)
                    .ok()
                    .and_then(|u| {
                        u.host_str()
                            .map(|h| h.trim_start_matches("www.").to_string())
                    })
                    .unwrap_or_else(|| m.url.clone());
                marketplaces.insert(
                    0,
                    Link {
                        label,
                        url: m.url.clone(),
                    },
                );
            }
        }
        Self {
            description: p.description.clone(),
            website: p.website.clone(),
            x: p.x.clone(),
            discord: p.discord.clone(),
            telegram: p.telegram.clone(),
            minted_at: p.minted_at.clone(),
            mint_price_zec: p.mint_price_zec,
            marketplaces,
            confidence: p.confidence.clone(),
            sources: links(&p.sources),
        }
    }
}

/// A collection detected on chain.
#[derive(Debug, Clone, sqlx::FromRow)]
struct Group {
    kind: String,
    slug: String,
    supply: Option<i64>,
    items: i64,
    unique_items: i64,
    holders: i64,
    shielded: i64,
    deployed: bool,
    deploy_inscription: Option<String>,
    deployer: Option<String>,
    cover: Option<String>,
    first_time: DateTime<Utc>,
    last_time: DateTime<Utc>,
    sales: i64,
    volume: i64,
    last_price: Option<i64>,
}

/// On-chain figures of a collection.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Onchain {
    /// zrc721 (declared by a deploy inscription) | drop (launchpad mints grouped by the address
    /// their commit transactions paid)
    pub kind: String,
    /// The ZRC-721 name or the drop's creator address.
    pub key: String,
    /// Items inscribed; every mint counts, duplicates included.
    pub items: i64,
    /// Distinct item ids (ZRC-721) or images (drops).
    pub unique_items: i64,
    /// Distinct current owners.
    pub holders: i64,
    /// Items whose tracking ended in a shielded transaction.
    pub shielded: i64,
    /// ZRC-721: whether a deploy inscription defines it.
    pub deployed: bool,
    pub deploy_inscription: Option<String>,
    /// ZRC-721 deployer, or the drop's creator address.
    pub creator: Option<String>,
    pub first_time: DateTime<Utc>,
    pub last_time: DateTime<Utc>,
    /// Marketplace sales detected on chain.
    pub sales: i64,
    /// Sum of on-chain sale prices, zatoshis.
    pub volume: i64,
    pub last_price: Option<i64>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CollectionRow {
    /// URL key: the project slug, or for unclaimed on-chain collections the ZRC-721 name or
    /// drop address.
    pub slug: String,
    pub name: String,
    /// Where items live: inscription | marketplace | zsa-testnet | other-chain | upcoming.
    pub kind: String,
    /// Supply the project declares.
    pub supply: Option<i64>,
    /// When the collection launched (YYYY-MM-DD): the project's mint date, else its first
    /// on-chain mint.
    pub launched: Option<String>,
    /// Picture for the collection: path of an image served by this API.
    pub image: Option<String>,
    /// Registry facts; `None` for on-chain collections no project claims.
    pub project: Option<ProjectInfo>,
    pub onchain: Option<Onchain>,
    /// Marketplace figures, as the venue reports them.
    pub market: Option<VenueStats>,
}

fn onchain_of(g: &Group) -> Onchain {
    Onchain {
        kind: g.kind.clone(),
        key: g.slug.clone(),
        items: g.items,
        unique_items: g.unique_items,
        holders: g.holders,
        shielded: g.shielded,
        deployed: g.deployed,
        deploy_inscription: g.deploy_inscription.clone(),
        creator: g.deployer.clone(),
        first_time: g.first_time,
        last_time: g.last_time,
        sales: g.sales,
        volume: g.volume,
        last_price: g.last_price,
    }
}

fn project_of(g: &Group) -> Option<&'static Project> {
    match g.kind.as_str() {
        "zrc721" => nft_projects::for_zrc721(&g.slug),
        _ => nft_projects::for_drop(&g.slug),
    }
}

async fn row(p: Option<&'static Project>, g: Option<&Group>) -> CollectionRow {
    let cover = g
        .and_then(|g| g.cover.as_ref())
        .map(|id| format!("/v1/inscriptions/{id}/image"));
    match p {
        Some(p) => CollectionRow {
            slug: p.slug.clone(),
            name: p.name.clone(),
            kind: p.kind.clone(),
            supply: p.supply.or(g.and_then(|g| g.supply)),
            launched: p
                .minted_at
                .as_ref()
                .filter(|d| d.len() >= 10 && d.as_bytes()[4] == b'-')
                .map(|d| d[..10].to_string())
                .or_else(|| g.map(|g| g.first_time.format("%Y-%m-%d").to_string())),
            image: p
                .logo
                .as_ref()
                .map(|_| format!("/v1/collections/{}/logo", p.slug))
                .or(cover),
            project: Some(p.into()),
            onchain: g.map(onchain_of),
            market: nft_markets::stats(p).await,
        },
        None => {
            let g = g.expect("an unclaimed collection is on chain");
            CollectionRow {
                slug: g.slug.clone(),
                name: if g.kind == "drop" {
                    drop_name(&g.slug)
                } else {
                    g.slug.clone()
                },
                kind: "inscription".into(),
                supply: g.supply,
                launched: Some(g.first_time.format("%Y-%m-%d").to_string()),
                image: cover,
                project: None,
                onchain: Some(onchain_of(g)),
                market: None,
            }
        }
    }
}

/// On-chain collections with item, holder and sale aggregates; `$1` filters when given.
fn group_sql(filter: &str) -> String {
    format!(
        "WITH members AS (
            SELECT 'zrc721' AS kind, collection AS slug, i.* FROM inscriptions i
            WHERE media = 'collection'
            UNION ALL
            SELECT 'drop', drop_key, i.* FROM inscriptions i
            WHERE drop_key IS NOT NULL AND drop_key <> ''),
        groups AS (
            SELECT kind, slug, count(*) AS items,
                   count(DISTINCT coalesce(item, ipfs, sha256)) AS unique_items,
                   count(DISTINCT owner) AS holders,
                   count(*) FILTER (WHERE state = 'shielded') AS shielded,
                   (array_agg(id ORDER BY number))[1] AS cover,
                   min(time) AS first_time, max(time) AS last_time
            FROM members GROUP BY kind, slug
            HAVING kind = 'zrc721' OR count(*) >= {MIN_DROP}),
        sales AS (
            SELECT m.kind, m.slug, count(*) AS sales, sum(e.price)::bigint AS volume,
                   (array_agg(e.price ORDER BY e.height DESC))[1] AS last_price
            FROM members m JOIN inscription_events e ON e.inscription_id = m.id
            WHERE e.price IS NOT NULL GROUP BY m.kind, m.slug)
        SELECT g.kind, g.slug, c.supply, g.items, g.unique_items, g.holders, g.shielded,
               c.name IS NOT NULL AS deployed, c.deploy_inscription,
               CASE WHEN g.kind = 'drop' THEN g.slug ELSE c.deployer END AS deployer,
               g.cover, g.first_time, g.last_time,
               coalesce(s.sales, 0) AS sales, coalesce(s.volume, 0) AS volume, s.last_price
        FROM groups g
        LEFT JOIN collections c ON g.kind = 'zrc721' AND c.name = g.slug
        LEFT JOIN sales s ON s.kind = g.kind AND s.slug = g.slug
        {filter}"
    )
}

/// Listing order: newest launches first (ISO dates sort as text), undated last.
fn rank(r: &CollectionRow) -> std::cmp::Reverse<String> {
    std::cmp::Reverse(r.launched.clone().unwrap_or_default())
}

/// Zcash NFT collections, newest first: every project in the registry, with on-chain figures
/// where ZScan indexes its items, plus ZRC-721 collections no project claims yet. (Launchpad
/// drops no project claims stay reachable by address but aren't listed.)
#[utoipa::path(get, path = "/v1/collections",
    responses((status = 200, body = [CollectionRow])), tag = "inscriptions")]
pub async fn collections(State(s): State<AppState>) -> ApiResult<Vec<CollectionRow>> {
    let groups: Vec<Group> = sqlx::query_as(&group_sql("LIMIT 1000"))
        .fetch_all(&s.db.pool)
        .await?;
    let mut rows = Vec::new();
    for p in nft_projects::projects() {
        let g = groups
            .iter()
            .find(|g| project_of(g).is_some_and(|q| q.slug == p.slug));
        rows.push(row(Some(p), g).await);
    }
    for g in groups.iter().filter(|g| {
        project_of(g).is_none() && g.kind == "zrc721" && g.items >= MIN_UNCLAIMED_ZRC721
    }) {
        rows.push(row(None, Some(g)).await);
    }
    rows.sort_by_key(rank);
    Ok(Json(rows))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct CollectionQuery {
    /// On-chain items after this inscription number (the previous page's `next`).
    pub cursor: Option<i64>,
    /// Off-chain items: zero-based page of 48.
    pub page: Option<usize>,
    /// Off-chain items: `id` (default), `rank` (rarest first) or `price` (cheapest listed first).
    pub sort: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct Holder {
    pub address: String,
    pub count: i64,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct Sale {
    pub id: String,
    pub number: i64,
    pub media: String,
    pub collection: Option<String>,
    pub item: Option<String>,
    /// Paid to the seller, zatoshis.
    pub price: i64,
    pub seller: Option<String>,
    pub buyer: Option<String>,
    pub txid: String,
    pub height: i32,
    pub time: DateTime<Utc>,
    /// Name of the launchpad drop it was minted in, when a project claims it.
    #[sqlx(skip)]
    pub drop_name: Option<String>,
    #[serde(skip)]
    drop_key: Option<String>,
}

const SALE_COLUMNS: &str = "i.id, i.number, i.media, i.collection, i.item, e.price,
    e.from_addr AS seller, e.to_addr AS buyer, e.txid, e.height, e.time, i.drop_key";

fn named_sales(rows: Vec<Sale>) -> Vec<Sale> {
    rows.into_iter()
        .map(|mut s| {
            s.drop_name = s
                .drop_key
                .as_deref()
                .and_then(nft_projects::for_drop)
                .map(|p| p.name.clone());
            s
        })
        .collect()
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CollectionDetail {
    #[serde(flatten)]
    pub collection: CollectionRow,
    /// IPFS or HTTPS folder of ZRC-721 item metadata.
    pub meta: Option<String>,
    /// On-chain items in inscription order, 48 per page.
    pub items_page: Vec<InscriptionRow>,
    pub next: Option<i64>,
    /// Top 25 on-chain holders.
    pub holders_list: Vec<Holder>,
    /// Latest 25 on-chain sales.
    pub recent_sales: Vec<Sale>,
    /// Items kept by the marketplace (off-chain projects), 48 per page.
    pub venue_items: Vec<VenueItem>,
    pub venue_total: Option<usize>,
    /// Latest sales reported by the marketplace (off-chain projects).
    pub venue_sales: Vec<VenueSale>,
}

/// A project or on-chain collection by slug (project slug, ZRC-721 name or drop address).
async fn resolve(
    s: &AppState,
    slug: &str,
) -> Result<(Option<&'static Project>, Option<Group>), ApiError> {
    let project = nft_projects::by_slug(slug);
    let (kind, key) = match project {
        Some(p) => match (&p.onchain.zrc721, &p.onchain.drop) {
            (Some(n), _) => ("zrc721", n.clone()),
            (_, Some(d)) => ("drop", d.clone()),
            _ => return Ok((Some(p), None)),
        },
        None if slug.starts_with("t1") => ("drop", slug.to_string()),
        None => ("zrc721", slug.to_uppercase()),
    };
    let group: Option<Group> = sqlx::query_as(&group_sql("WHERE g.kind = $1 AND g.slug = $2"))
        .bind(kind)
        .bind(&key)
        .fetch_optional(&s.db.pool)
        .await?;
    // An on-chain collection a project claims is shown under the project.
    let project = project.or_else(|| group.as_ref().and_then(project_of));
    if project.is_none() && group.is_none() {
        return Err(ApiError::NotFound(format!("Collection {slug} not found")));
    }
    Ok((project, group))
}

/// One collection: registry facts, marketplace figures, items, holders and sales.
#[utoipa::path(get, path = "/v1/collections/{slug}", params(("slug" = String, Path), CollectionQuery),
    responses((status = 200, body = CollectionDetail), (status = 404)), tag = "inscriptions")]
pub async fn collection(
    State(s): State<AppState>,
    Path(slug): Path<String>,
    Query(q): Query<CollectionQuery>,
) -> ApiResult<CollectionDetail> {
    let (project, group) = resolve(&s, slug.trim()).await?;
    let db = &s.db.pool;
    let collection = row(project, group.as_ref()).await;

    let (mut items, mut holders_list, mut recent_sales, mut meta) = (vec![], vec![], vec![], None);
    let mut next = None;
    if let Some(g) = &group {
        let member = if g.kind == "drop" {
            "i.drop_key = $1"
        } else {
            "i.media = 'collection' AND i.collection = $1"
        };
        meta = sqlx::query_scalar("SELECT meta FROM collections WHERE name = $1")
            .bind(&g.slug)
            .fetch_optional(db)
            .await?
            .flatten();
        let mut page: Vec<InscriptionRow> = sqlx::query_as(&format!(
            "SELECT {ROW_COLUMNS} FROM inscriptions i
             WHERE {member} AND ($2::bigint IS NULL OR number > $2)
             ORDER BY number LIMIT 49"
        ))
        .bind(&g.slug)
        .bind(q.cursor)
        .fetch_all(db)
        .await?;
        if page.len() > 48 {
            page.truncate(48);
            next = page.last().map(|r| r.number);
        }
        items = page.into_iter().map(moderate).collect();
        holders_list = sqlx::query_as(&format!(
            "SELECT owner AS address, count(*) AS count FROM inscriptions i
             WHERE {member} AND owner IS NOT NULL
             GROUP BY owner ORDER BY 2 DESC, 1 LIMIT 25"
        ))
        .bind(&g.slug)
        .fetch_all(db)
        .await?;
        recent_sales = named_sales(
            sqlx::query_as(&format!(
                "SELECT {SALE_COLUMNS} FROM inscription_events e
             JOIN inscriptions i ON i.id = e.inscription_id
             WHERE e.price IS NOT NULL AND {member} ORDER BY e.height DESC LIMIT 25"
            ))
            .bind(&g.slug)
            .fetch_all(db)
            .await?,
        );
    }

    let (mut venue_items, mut venue_total, mut venue_sales) = (vec![], None, vec![]);
    if let Some(p) = project {
        let page = q.page.unwrap_or(0).min(10_000);
        let sort = q.sort.as_deref().unwrap_or("id");
        if let Some((list, total)) = nft_markets::items(p, sort, page * 48, 48).await {
            venue_items = list;
            venue_total = Some(total);
        }
        venue_sales = nft_markets::sales(p).await;
    }

    Ok(Json(CollectionDetail {
        collection,
        meta,
        items_page: items,
        next,
        holders_list,
        recent_sales,
        venue_items,
        venue_total,
        venue_sales,
    }))
}

/// A project's logo, relayed so visitors never contact its host.
#[utoipa::path(get, path = "/v1/collections/{slug}/logo", params(("slug" = String, Path)),
    responses((status = 200, description = "Image"), (status = 404), (status = 502)), tag = "inscriptions")]
pub async fn logo(
    State(_s): State<AppState>,
    Path(slug): Path<String>,
) -> Result<Response, ApiError> {
    let src = nft_projects::by_slug(&slug)
        .and_then(|p| p.logo.clone())
        .ok_or_else(|| ApiError::NotFound(format!("No logo for {slug}")))?;
    relay(&src).await
}

async fn relay(src: &str) -> Result<Response, ApiError> {
    use axum::response::IntoResponse;
    match ipfs::image(src, None).await {
        Ok((ctype, body)) => Ok(super::inscriptions::sandboxed_image(&ctype, body)),
        Err(e) => {
            tracing::warn!("image {src}: {e:#}");
            Ok((axum::http::StatusCode::BAD_GATEWAY, "Image unavailable").into_response())
        }
    }
}

/// An item kept by a marketplace (off-chain projects such as zkSNARKs): traits, listing and
/// venue history.
#[utoipa::path(get, path = "/v1/collections/{slug}/items/{id}", params(("slug" = String, Path), ("id" = String, Path)),
    responses((status = 200, body = VenueItemDetail), (status = 404)), tag = "inscriptions")]
pub async fn venue_item(
    State(_s): State<AppState>,
    Path((slug, id)): Path<(String, String)>,
) -> ApiResult<VenueItemDetail> {
    let p = nft_projects::by_slug(&slug)
        .ok_or_else(|| ApiError::NotFound(format!("Collection {slug} not found")))?;
    nft_markets::item(p, &id)
        .await
        .map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("{} #{id} not found", p.name)))
}

/// The picture of a marketplace-kept item, relayed.
#[utoipa::path(get, path = "/v1/collections/{slug}/items/{id}/image", params(("slug" = String, Path), ("id" = String, Path)),
    responses((status = 200, description = "Image"), (status = 404), (status = 502)), tag = "inscriptions")]
pub async fn venue_item_image(
    State(_s): State<AppState>,
    Path((slug, id)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let p = nft_projects::by_slug(&slug)
        .ok_or_else(|| ApiError::NotFound(format!("Collection {slug} not found")))?;
    let src =
        nft_markets::item_image_source(p, &id).map_err(|e| ApiError::NotFound(e.to_string()))?;
    relay(&src).await
}

/// Latest NFT sales detected on chain, across marketplaces.
#[utoipa::path(get, path = "/v1/nft-sales",
    responses((status = 200, body = [Sale])), tag = "inscriptions")]
pub async fn sales(State(s): State<AppState>) -> ApiResult<Vec<Sale>> {
    let rows = sqlx::query_as(&format!(
        "SELECT {SALE_COLUMNS} FROM inscription_events e JOIN inscriptions i ON i.id = e.inscription_id
         WHERE e.price IS NOT NULL ORDER BY e.height DESC LIMIT 50"
    ))
    .fetch_all(&s.db.pool)
    .await?;
    Ok(Json(named_sales(rows)))
}
