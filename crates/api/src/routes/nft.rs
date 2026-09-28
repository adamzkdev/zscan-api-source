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
use crate::nft_markets::{
    self, Book, ItemFilter, TraitValue, VenueItem, VenueItemDetail, VenueSale, VenueStats,
};
use crate::{ipfs, AppState};

/// Fewest items for a launchpad drop to be listed as a collection.
pub const MIN_DROP: i64 = 20;

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
    /// Recent trading, from on-chain sales or ZScan's archive of the venue's sales.
    pub activity: Option<Activity>,
}

/// Recent trading of a collection. Amounts in zatoshis.
#[derive(Debug, Clone, Default, Serialize, ToSchema)]
pub struct Activity {
    /// `chain` (sales detected on chain) or `archive` (the venue's sales, archived by ZScan).
    pub source: String,
    pub sales_24h: i64,
    pub volume_24h: i64,
    pub sales_7d: i64,
    pub volume_7d: i64,
    pub sales_30d: i64,
    pub volume_30d: i64,
    /// Floor change over 24 hours, percent (from ZScan's order-book snapshots).
    pub floor_change_24h: Option<f64>,
    /// Volume per UTC day, oldest first, for the last 14 days (today included).
    pub daily_volume: Vec<i64>,
}

impl Activity {
    fn from_sales(
        source: &str,
        sales: &[(DateTime<Utc>, i64)],
        floor: Option<i64>,
        floor_24h: Option<i64>,
    ) -> Self {
        let now = Utc::now();
        let today = now.date_naive();
        let mut a = Activity {
            source: source.into(),
            daily_volume: vec![0; 14],
            floor_change_24h: match (floor, floor_24h) {
                (Some(f), Some(p)) if p > 0 => Some((f as f64 / p as f64 - 1.0) * 100.0),
                _ => None,
            },
            ..Default::default()
        };
        for (t, price) in sales {
            let age = now - *t;
            if age <= chrono::Duration::hours(24) {
                a.sales_24h += 1;
                a.volume_24h += price;
            }
            if age <= chrono::Duration::days(7) {
                a.sales_7d += 1;
                a.volume_7d += price;
            }
            if age <= chrono::Duration::days(30) {
                a.sales_30d += 1;
                a.volume_30d += price;
            }
            let days_ago = (today - t.date_naive()).num_days();
            if (0..14).contains(&days_ago) {
                a.daily_volume[13 - days_ago as usize] += price;
            }
        }
        a
    }
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
            activity: None,
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
                activity: None,
            }
        }
    }
}

/// Inscriptions of on-chain collections, keyed by (kind, slug).
const MEMBERS: &str = "members AS (
    SELECT 'zrc721' AS kind, collection AS slug, i.* FROM inscriptions i
    WHERE media = 'collection'
    UNION ALL
    SELECT 'drop', drop_key, i.* FROM inscriptions i
    WHERE drop_key IS NOT NULL AND drop_key <> '')";

/// On-chain collections with item, holder and sale aggregates; `$1` filters when given.
fn group_sql(filter: &str) -> String {
    format!(
        "WITH {MEMBERS},
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

/// Zcash NFT collections listed on ZScan (the public registry), newest first, with on-chain
/// figures where ZScan indexes their items and marketplace figures where a venue reports them.
/// Collections no project claims stay reachable by name or address but aren't listed.
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
    add_activity(&s, &mut rows).await?;
    rows.sort_by_key(rank);
    Ok(Json(rows))
}

/// Where a collection's sales are read from.
enum SaleSource<'a> {
    /// ZScan's archive of the venue's sales (items kept off chain).
    Archive(&'a str),
    /// Sales detected on chain, for a (kind, key) collection.
    Chain(&'a str, &'a str),
}

fn sale_source<'a>(p: Option<&'a Project>, o: Option<&'a Onchain>) -> Option<SaleSource<'a>> {
    if let Some(p) = p.filter(|p| p.market.as_ref().is_some_and(|m| m.kind == "zilkroad")) {
        return Some(SaleSource::Archive(&p.slug));
    }
    o.map(|o| SaleSource::Chain(&o.kind, &o.key))
}

/// Latest floor per project, and the one 24 hours ago, from order-book snapshots.
async fn floors_24h_ago(s: &AppState) -> Result<std::collections::HashMap<String, i64>, ApiError> {
    let rows: Vec<(String, Option<i64>)> = sqlx::query_as(
        "SELECT DISTINCT ON (project) project, floor FROM nft_market_snapshots
         WHERE time <= now() - interval '24 hours' AND time > now() - interval '48 hours'
         ORDER BY project, time DESC",
    )
    .fetch_all(&s.db.pool)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(p, f)| Some((p, f?)))
        .collect())
}

/// Fills each row's recent trading.
async fn add_activity(s: &AppState, rows: &mut [CollectionRow]) -> Result<(), ApiError> {
    let db = &s.db.pool;
    let chain: Vec<(String, String, DateTime<Utc>, i64)> = sqlx::query_as(&format!(
        "WITH {MEMBERS} SELECT m.kind, m.slug, e.time, e.price
         FROM members m JOIN inscription_events e ON e.inscription_id = m.id
         WHERE e.price IS NOT NULL AND e.time > now() - interval '30 days'"
    ))
    .fetch_all(db)
    .await?;
    let archive: Vec<(String, DateTime<Utc>, i64)> = sqlx::query_as(
        "SELECT project, time, price FROM nft_market_sales WHERE time > now() - interval '30 days'",
    )
    .fetch_all(db)
    .await?;
    let before = floors_24h_ago(s).await?;
    for r in rows.iter_mut() {
        let p = r.project.as_ref().and(nft_projects::by_slug(&r.slug));
        let sales: Vec<(DateTime<Utc>, i64)> = match sale_source(p, r.onchain.as_ref()) {
            Some(SaleSource::Archive(slug)) => archive
                .iter()
                .filter(|(p, ..)| p == slug)
                .map(|(_, t, v)| (*t, *v))
                .collect(),
            Some(SaleSource::Chain(kind, key)) => chain
                .iter()
                .filter(|(k, g, ..)| k == kind && g == key)
                .map(|(_, _, t, v)| (*t, *v))
                .collect(),
            None => continue,
        };
        let source = match sale_source(p, r.onchain.as_ref()) {
            Some(SaleSource::Archive(_)) => "archive",
            _ => "chain",
        };
        r.activity = Some(Activity::from_sales(
            source,
            &sales,
            r.market.as_ref().and_then(|m| m.floor),
            before.get(&r.slug).copied(),
        ));
    }
    Ok(())
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct CollectionQuery {
    /// On-chain items after this inscription number (the previous page's `next`).
    pub cursor: Option<i64>,
    /// Off-chain items: zero-based page of 48.
    pub page: Option<usize>,
    /// Off-chain items: `id` (default), `rank` (rarest first), `price` (cheapest listed first),
    /// `price_desc`, `last` (highest last sale first) or `bid` (highest bid first).
    pub sort: Option<String>,
    /// Off-chain items: comma-separated `Type:Value` traits. Values of one type are
    /// alternatives; different types must all match.
    pub traits: Option<String>,
    /// Off-chain items: `listed`, `bids` or `sold`.
    pub status: Option<String>,
    /// Off-chain items: an item number.
    pub q: Option<String>,
    /// `art`: every artwork of a ZRC-721 collection by id, minted or not (48 per `page`); the
    /// default when most artworks aren't minted yet. `minted`: minted items only.
    pub view: Option<String>,
}

/// An artwork of a ZRC-721 collection, whether or not it has been minted yet.
#[derive(Debug, Serialize, ToSchema)]
pub struct Artwork {
    pub id: String,
    pub name: String,
    /// API path of its picture (from the collection's metadata).
    pub image: String,
    /// The inscription that minted it, if any (the first one when minted twice).
    pub inscription: Option<String>,
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
    /// Items kept by the marketplace (off-chain projects) matching the filters, 48 per page.
    pub venue_items: Vec<VenueItem>,
    /// How many items match the filters.
    pub venue_total: Option<usize>,
    /// Latest sales reported by the marketplace (off-chain projects).
    pub venue_sales: Vec<VenueSale>,
    /// With `view=art`: a page of every artwork of a ZRC-721 collection.
    pub artworks: Vec<Artwork>,
    /// How many artworks the collection declares.
    pub artworks_total: Option<i64>,
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
        meta =
            sqlx::query_scalar::<_, Option<String>>("SELECT meta FROM collections WHERE name = $1")
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
        let filter = ItemFilter {
            sort: q.sort.clone().unwrap_or_default(),
            traits: q
                .traits
                .as_deref()
                .unwrap_or("")
                .split(',')
                .filter_map(|t| t.split_once(':'))
                .map(|(t, v)| (t.trim().to_string(), v.trim().to_string()))
                .take(40)
                .collect(),
            status: q.status.clone(),
            q: q.q.clone().filter(|q| !q.trim().is_empty()),
        };
        if let Some((list, total)) = nft_markets::items(p, &filter, page * 48, 48).await {
            venue_items = list;
            venue_total = Some(total);
        }
        venue_sales = nft_markets::sales(p).await;
    }

    let (mut artworks, mut artworks_total) = (vec![], None);
    if let (Some(g), Some(supply), Some(meta)) = (&group, collection.supply, &meta) {
        // Only offer the artworks when their metadata can actually be read (some collections'
        // folders aren't served by any public gateway).
        let readable = g.kind == "zrc721" && supply > 0 && metadata_readable(meta);
        if readable {
            artworks_total = Some(supply);
            // Mostly unminted collections open on every artwork, minted or not.
            let default_art = q.view.is_none() && q.cursor.is_none() && g.unique_items < supply;
            if q.view.as_deref() == Some("art") || default_art {
                let page = q.page.unwrap_or(0).min(100_000) as i64;
                let ids: Vec<String> = (page * 48..(page * 48 + 48).min(supply))
                    .map(|i| i.to_string())
                    .collect();
                let minted: Vec<(String, String)> = sqlx::query_as(
                    "SELECT DISTINCT ON (item) item, id FROM inscriptions
                     WHERE media = 'collection' AND collection = $1 AND item = ANY($2)
                     ORDER BY item, number",
                )
                .bind(&g.slug)
                .bind(&ids)
                .fetch_all(db)
                .await?;
                artworks = ids
                    .into_iter()
                    .map(|id| Artwork {
                        name: format!("{} #{id}", collection.name),
                        image: format!("/v1/collections/{}/art/{id}/image", collection.slug),
                        inscription: minted
                            .iter()
                            .find(|(i, _)| *i == id)
                            .map(|(_, x)| x.clone()),
                        id,
                    })
                    .collect();
            }
        }
    }

    Ok(Json(CollectionDetail {
        artworks,
        artworks_total,
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

/// Starts the metadata checks of listed ZRC-721 collections at startup, so their first page view
/// already knows whether to offer every artwork.
pub async fn warm_metadata(db: sqlx::PgPool) {
    let names: Vec<String> = nft_projects::projects()
        .iter()
        .filter_map(|p| p.onchain.zrc721.clone())
        .collect();
    let metas: Vec<Option<String>> =
        sqlx::query_scalar("SELECT meta FROM collections WHERE name = ANY($1)")
            .bind(&names)
            .fetch_all(&db)
            .await
            .unwrap_or_default();
    for m in metas.into_iter().flatten() {
        metadata_readable(&m);
    }
}

/// Whether a collection's metadata folder can be read, checked in the background so a page
/// never waits on IPFS gateways (an unreachable folder can take a minute to fail). Unknown
/// folders count as unreadable until the first check finishes.
fn metadata_readable(meta: &str) -> bool {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    use std::time::{Duration, Instant};
    /// `None` while a check is running.
    type Known = HashMap<String, Option<(Instant, bool)>>;
    static KNOWN: OnceLock<Mutex<Known>> = OnceLock::new();
    let known = KNOWN.get_or_init(Default::default);
    let mut map = known.lock().unwrap();
    let last = match map.get(meta) {
        Some(None) => return false,
        Some(Some((at, ok))) => {
            let ttl = Duration::from_secs(if *ok { 6 * 3600 } else { 1800 });
            if at.elapsed() < ttl {
                return *ok;
            }
            *ok
        }
        None => false,
    };
    map.insert(meta.to_string(), None);
    let meta = meta.to_string();
    tokio::spawn(async move {
        let ok = ipfs::metadata(&meta, "0").await.is_some()
            || ipfs::metadata(&meta, "1").await.is_some();
        known
            .lock()
            .unwrap()
            .insert(meta, Some((Instant::now(), ok)));
    });
    last
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

/// The picture of a ZRC-721 artwork, from the collection's metadata (minted or not).
#[utoipa::path(get, path = "/v1/collections/{slug}/art/{id}/image", params(("slug" = String, Path), ("id" = String, Path)),
    responses((status = 200, description = "Image"), (status = 404), (status = 502)), tag = "inscriptions")]
pub async fn artwork_image(
    State(s): State<AppState>,
    Path((slug, id)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let (_, group) = resolve(&s, &slug).await?;
    let g = group
        .filter(|g| g.kind == "zrc721")
        .ok_or_else(|| ApiError::NotFound(format!("{slug} has no artworks")))?;
    let meta: String = sqlx::query_scalar("SELECT meta FROM collections WHERE name = $1")
        .bind(&g.slug)
        .fetch_optional(&s.db.pool)
        .await?
        .flatten()
        .ok_or_else(|| ApiError::NotFound(format!("{slug} has no metadata")))?;
    let src = ipfs::metadata(&meta, &id)
        .await
        .as_ref()
        .and_then(ipfs::image_of)
        .ok_or_else(|| ApiError::NotFound(format!("No picture for {slug} #{id}")))?;
    relay(&src).await
}

/// An item kept by a marketplace (off-chain projects such as zkSNARKs): traits, listing and
/// venue history.
#[utoipa::path(get, path = "/v1/collections/{slug}/items/{id}", params(("slug" = String, Path), ("id" = String, Path)),
    responses((status = 200, body = VenueItemDetail), (status = 404)), tag = "inscriptions")]
pub async fn venue_item(
    State(s): State<AppState>,
    Path((slug, id)): Path<(String, String)>,
) -> ApiResult<VenueItemDetail> {
    let p = nft_projects::by_slug(&slug)
        .ok_or_else(|| ApiError::NotFound(format!("Collection {slug} not found")))?;
    let mut item = nft_markets::item(p, &id)
        .await
        .ok_or_else(|| ApiError::NotFound(format!("{} #{id} not found", p.name)))?;
    let recent = recent_archived(&s, &p.slug, 7).await?;
    (item.trait_stats, item.tier) = nft_markets::item_context(p, &id, &recent).await;
    item.sales = sqlx::query_as(
        "SELECT time, price, via FROM nft_market_sales WHERE project = $1 AND item = $2
         ORDER BY time",
    )
    .bind(&p.slug)
    .bind(&id)
    .fetch_all(&s.db.pool)
    .await?;
    Ok(Json(item))
}

/// Archived sales (`item`, `price`) of a project in the last `days` days.
async fn recent_archived(
    s: &AppState,
    project: &str,
    days: i32,
) -> Result<Vec<(String, i64)>, ApiError> {
    Ok(sqlx::query_as(
        "SELECT item, price FROM nft_market_sales
         WHERE project = $1 AND time > now() - make_interval(days => $2)",
    )
    .bind(project)
    .bind(days)
    .fetch_all(&s.db.pool)
    .await?)
}

/// The picture of a marketplace-kept item, relayed.
#[utoipa::path(get, path = "/v1/collections/{slug}/items/{id}/image", params(("slug" = String, Path), ("id" = String, Path)),
    responses((status = 200, description = "Image"), (status = 404), (status = 502)), tag = "inscriptions")]
pub async fn venue_item_image(
    State(s): State<AppState>,
    Path((slug, id)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    use axum::response::IntoResponse;
    let p = nft_projects::by_slug(&slug)
        .ok_or_else(|| ApiError::NotFound(format!("Collection {slug} not found")))?;
    let src =
        nft_markets::item_image_source(p, &id).map_err(|e| ApiError::NotFound(e.to_string()))?;
    if let Some((ctype, body)) = nft_markets::stored_image(&s.db.pool, &p.slug, &id).await {
        return Ok(super::inscriptions::sandboxed_image(&ctype, body));
    }
    match ipfs::image(&src, None).await {
        Ok((ctype, body)) => {
            nft_markets::store_image(&s.db.pool, &p.slug, &id, &ctype, &body).await;
            Ok(super::inscriptions::sandboxed_image(&ctype, body))
        }
        Err(e) => {
            tracing::warn!("image {src}: {e:#}");
            Ok((axum::http::StatusCode::BAD_GATEWAY, "Image unavailable").into_response())
        }
    }
}

/// One UTC day of trading. Amounts in zatoshis.
#[derive(Debug, Serialize, ToSchema)]
pub struct MarketDay {
    /// YYYY-MM-DD.
    pub day: String,
    pub sales: i64,
    pub volume: i64,
    /// Median sale price.
    pub median: Option<i64>,
    /// Floor at the day's last order-book snapshot.
    pub floor: Option<i64>,
    pub listings: Option<i32>,
}

/// An order-book snapshot (one per hour).
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct FloorPoint {
    pub time: DateTime<Utc>,
    pub floor: Option<i64>,
    pub listings: Option<i32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TopSale {
    /// Item id on the venue, or the inscription id for on-chain items.
    pub item: String,
    pub name: String,
    pub price: i64,
    pub time: DateTime<Utc>,
    /// API path of the item's picture.
    pub image: String,
    /// ZScan page path of the item.
    pub href: String,
}

/// Market analytics of a collection, from on-chain sales or ZScan's archive of its venue.
#[derive(Debug, Serialize, ToSchema)]
pub struct CollectionStats {
    pub activity: Option<Activity>,
    /// Trading per UTC day since the first recorded sale.
    pub history: Vec<MarketDay>,
    /// Hourly order-book snapshots of the last 14 days.
    pub floor_series: Vec<FloorPoint>,
    /// Sales on record, and the first one's time.
    pub recorded_sales: i64,
    pub recorded_volume: i64,
    pub first_sale: Option<DateTime<Utc>>,
    /// Distinct items sold at least once.
    pub traded_items: i64,
    /// Listed items divided by average daily sales over 7 days: how long the current listings
    /// would take to sell at the recent pace.
    pub days_of_inventory: Option<f64>,
    /// Order-book analysis (off-chain collections): rarity bands, deals, depth.
    pub book: Book,
    /// Every trait value with its count, floor and recent sales (off-chain collections).
    pub traits: Vec<TraitValue>,
    pub top_sales: Vec<TopSale>,
    /// Whether the archive's import of the venue's past sales is still running.
    pub importing: bool,
}

/// Market analytics: daily volume and floor, rarity bands, trait floors, deals, top sales.
#[utoipa::path(get, path = "/v1/collections/{slug}/stats", params(("slug" = String, Path)),
    responses((status = 200, body = CollectionStats), (status = 404)), tag = "inscriptions")]
pub async fn collection_stats(
    State(s): State<AppState>,
    Path(slug): Path<String>,
) -> ApiResult<CollectionStats> {
    let (project, group) = resolve(&s, slug.trim()).await?;
    let db = &s.db.pool;
    let onchain = group.as_ref().map(onchain_of);
    let market = match project {
        Some(p) => nft_markets::stats(p).await,
        None => None,
    };
    let snap_key = project.map(|p| p.slug.clone()).unwrap_or_default();

    // (day, sales, volume, median), every sale (time, price) of the last 30 days, and
    // (sales, volume, first sale, distinct items) on record.
    type Day = (String, i64, i64, Option<i64>);
    type Totals = (i64, i64, Option<DateTime<Utc>>, i64);
    type Recorded = (
        Vec<Day>,
        Vec<(DateTime<Utc>, i64)>,
        Vec<TopSale>,
        Totals,
        bool,
    );
    let (days, recent, top, totals, importing): Recorded =
        match sale_source(project, onchain.as_ref()) {
            Some(SaleSource::Archive(key)) => {
                let days = sqlx::query_as(
                    "SELECT to_char(time AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS day, count(*),
                            sum(price)::bigint,
                            (percentile_cont(0.5) WITHIN GROUP (ORDER BY price))::bigint
                     FROM nft_market_sales WHERE project = $1 GROUP BY 1 ORDER BY 1",
                )
                .bind(key)
                .fetch_all(db)
                .await?;
                let recent = sqlx::query_as(
                    "SELECT time, price FROM nft_market_sales
                     WHERE project = $1 AND time > now() - interval '30 days'",
                )
                .bind(key)
                .fetch_all(db)
                .await?;
                let top: Vec<(String, i64, DateTime<Utc>)> = sqlx::query_as(
                    "SELECT item, price, time FROM nft_market_sales WHERE project = $1
                     ORDER BY price DESC, time LIMIT 10",
                )
                .bind(key)
                .fetch_all(db)
                .await?;
                let name = project.map(|p| p.name.as_str()).unwrap_or(key);
                let top = top
                    .into_iter()
                    .map(|(item, price, time)| TopSale {
                        name: format!("{name} #{item}"),
                        image: format!("/v1/collections/{key}/items/{item}/image"),
                        href: format!("/collection/{key}/item/{item}"),
                        item,
                        price,
                        time,
                    })
                    .collect();
                let totals = sqlx::query_as(
                    "SELECT count(*), coalesce(sum(price), 0)::bigint, min(time),
                            count(DISTINCT item)
                     FROM nft_market_sales WHERE project = $1",
                )
                .bind(key)
                .fetch_one(db)
                .await?;
                let done: Option<bool> =
                    sqlx::query_scalar("SELECT done FROM nft_market_backfill WHERE project = $1")
                        .bind(key)
                        .fetch_optional(db)
                        .await?;
                (days, recent, top, totals, done != Some(true))
            }
            Some(SaleSource::Chain(kind, key)) => {
                let member = if kind == "drop" {
                    "i.drop_key = $1"
                } else {
                    "i.media = 'collection' AND i.collection = $1"
                };
                let from = format!(
                    "FROM inscription_events e JOIN inscriptions i ON i.id = e.inscription_id
                     WHERE e.price IS NOT NULL AND {member}"
                );
                let days = sqlx::query_as(&format!(
                    "SELECT to_char(e.time AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS day, count(*),
                            sum(e.price)::bigint,
                            (percentile_cont(0.5) WITHIN GROUP (ORDER BY e.price))::bigint
                     {from} GROUP BY 1 ORDER BY 1"
                ))
                .bind(key)
                .fetch_all(db)
                .await?;
                let recent = sqlx::query_as(&format!(
                    "SELECT e.time, e.price {from} AND e.time > now() - interval '30 days'"
                ))
                .bind(key)
                .fetch_all(db)
                .await?;
                type Row = (String, i64, i64, DateTime<Utc>, Option<String>);
                let top: Vec<Row> = sqlx::query_as(&format!(
                    "SELECT i.id, i.number, e.price, e.time, i.item {from}
                         ORDER BY e.price DESC, e.height LIMIT 10"
                ))
                .bind(key)
                .fetch_all(db)
                .await?;
                let cname = project
                    .map(|p| p.name.clone())
                    .unwrap_or_else(|| key.to_string());
                let top = top
                    .into_iter()
                    .map(|(id, number, price, time, item)| TopSale {
                        name: match item {
                            Some(i) => format!("{cname} #{i}"),
                            None => format!("Inscription #{number}"),
                        },
                        image: format!("/v1/inscriptions/{id}/image"),
                        href: format!("/inscription/{id}"),
                        item: id,
                        price,
                        time,
                    })
                    .collect();
                let totals = sqlx::query_as(&format!(
                    "SELECT count(*), coalesce(sum(e.price), 0)::bigint, min(e.time),
                            count(DISTINCT i.id) {from}"
                ))
                .bind(key)
                .fetch_one(db)
                .await?;
                (days, recent, top, totals, false)
            }
            None => (vec![], vec![], vec![], (0, 0, None, 0), false),
        };

    let day_floors: Vec<(String, Option<i64>, Option<i32>)> = sqlx::query_as(
        "SELECT DISTINCT ON (1) to_char(time AT TIME ZONE 'UTC', 'YYYY-MM-DD'), floor, listings
         FROM nft_market_snapshots WHERE project = $1 ORDER BY 1, time DESC",
    )
    .bind(&snap_key)
    .fetch_all(db)
    .await?;
    let mut history: std::collections::BTreeMap<String, MarketDay> = days
        .into_iter()
        .map(|(day, sales, volume, median)| {
            (
                day.clone(),
                MarketDay {
                    day,
                    sales,
                    volume,
                    median,
                    floor: None,
                    listings: None,
                },
            )
        })
        .collect();
    for (day, floor, listings) in day_floors {
        let d = history.entry(day.clone()).or_insert(MarketDay {
            day,
            sales: 0,
            volume: 0,
            median: None,
            floor: None,
            listings: None,
        });
        d.floor = floor;
        d.listings = listings;
    }
    // Days without sales between the first and last are shown as zero.
    let mut filled = Vec::new();
    if let (Some(first), Some(last)) = (
        history.keys().next().cloned(),
        history.keys().last().cloned(),
    ) {
        let parse = |d: &str| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok();
        if let (Some(mut d), Some(end)) = (parse(&first), parse(&last)) {
            while d <= end {
                let key = d.format("%Y-%m-%d").to_string();
                filled.push(history.remove(&key).unwrap_or(MarketDay {
                    day: key,
                    sales: 0,
                    volume: 0,
                    median: None,
                    floor: None,
                    listings: None,
                }));
                d = d.succ_opt().unwrap_or(end + chrono::Duration::days(1));
            }
        }
    }

    let floor_series = sqlx::query_as(
        "SELECT DISTINCT ON (date_trunc('hour', time)) time, floor, listings
         FROM nft_market_snapshots
         WHERE project = $1 AND time > now() - interval '14 days'
         ORDER BY date_trunc('hour', time), time DESC",
    )
    .bind(&snap_key)
    .fetch_all(db)
    .await?;

    let floor_before = floors_24h_ago(&s).await?.get(&snap_key).copied();
    let activity = (sale_source(project, onchain.as_ref()).is_some()).then(|| {
        Activity::from_sales(
            if matches!(
                sale_source(project, onchain.as_ref()),
                Some(SaleSource::Archive(_))
            ) {
                "archive"
            } else {
                "chain"
            },
            &recent,
            market.as_ref().and_then(|m| m.floor),
            floor_before,
        )
    });
    let days_of_inventory = match (&activity, market.as_ref().and_then(|m| m.listings)) {
        (Some(a), Some(listed)) if a.sales_7d > 0 => {
            Some(listed as f64 / (a.sales_7d as f64 / 7.0))
        }
        _ => None,
    };

    let (book, traits) = match project {
        Some(p) => {
            let items = recent_archived(&s, &p.slug, 7).await?;
            (
                nft_markets::book(p, &items).await,
                nft_markets::traits(p, &items).await,
            )
        }
        None => (Book::default(), vec![]),
    };

    Ok(Json(CollectionStats {
        activity,
        history: filled,
        floor_series,
        recorded_sales: totals.0,
        recorded_volume: totals.1,
        first_sale: totals.2,
        traded_items: totals.3,
        days_of_inventory,
        book,
        traits,
        top_sales: top,
        importing,
    }))
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
