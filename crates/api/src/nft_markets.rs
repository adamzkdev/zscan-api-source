//! Public marketplace APIs for NFT projects in the registry (`labels/nft_projects.json`):
//! order-book figures, and items and sales for projects whose records live off chain. Every
//! figure here is as the venue reports it; responses are cached a few minutes.
//!
//! Venues only publish their current order book and latest sales, so [`archive`] keeps the
//! history (order-book snapshots, every sale, item pictures) in Postgres.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sqlx::PgPool;
use tokio::sync::Mutex;
use utoipa::ToSchema;
use zscan_chain::nft_projects::{self, Project};

const TTL: Duration = Duration::from_secs(300);
const ZILKROAD: &str = "https://zilkroad.com";
/// How often the order book is sampled and the latest sales are archived.
const SNAPSHOT_EVERY: Duration = Duration::from_secs(300);
/// Pause between item reads of the one-time history import and the picture warm-up.
const BACKFILL_PAUSE: Duration = Duration::from_millis(400);

fn client() -> &'static reqwest::Client {
    static C: OnceLock<reqwest::Client> = OnceLock::new();
    C.get_or_init(|| {
        zscan_chain::http_client_builder()
            .timeout(Duration::from_secs(30))
            .user_agent("zscan/0.1 (+https://zscan.cash)")
            .build()
            .expect("http client")
    })
}

/// GET a JSON document, uncached.
async fn fetch_json(url: &str) -> Option<Value> {
    match client()
        .get(url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
    {
        Ok(r) => r.json::<Value>().await.ok(),
        Err(e) => {
            tracing::warn!("nft market {url}: {e:#}");
            None
        }
    }
}

/// Client for reads made while a visitor waits: fails fast.
fn quick_client() -> &'static reqwest::Client {
    static C: OnceLock<reqwest::Client> = OnceLock::new();
    C.get_or_init(|| {
        zscan_chain::http_client_builder()
            .timeout(Duration::from_secs(10))
            .user_agent("zscan/0.1 (+https://zscan.cash)")
            .build()
            .expect("http client")
    })
}

type Entry = (Instant, Option<Value>);

fn cache() -> &'static Mutex<HashMap<String, Entry>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// Fetches `url` and records it. A failed read keeps the last good value (only its time
/// moves, so a venue that's down isn't asked again on every request).
async fn refresh(url: String) -> Option<Value> {
    let fresh = match quick_client()
        .get(&url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
    {
        Ok(r) => r.json::<Value>().await.ok(),
        Err(e) => {
            tracing::warn!("nft market {url}: {e:#}");
            None
        }
    };
    let mut c = cache().lock().await;
    // Item pages add one entry each; drop old ones so the map stays small.
    if c.len() > 2_000 {
        c.retain(|_, (t, _)| t.elapsed() < TTL * 4);
    }
    let kept = match (&fresh, c.get(&url)) {
        (None, Some((_, old))) => old.clone(),
        _ => fresh,
    };
    c.insert(url, (Instant::now(), kept.clone()));
    kept
}

/// GET a JSON document with stale-while-revalidate caching: a cached value is returned at
/// once, and refreshed in the background once older than [`TTL`]. Only the very first read of
/// a URL waits for the venue, so a slow or down marketplace never holds a page.
async fn get_json(url: &str) -> Option<Value> {
    static REFRESHING: OnceLock<Mutex<std::collections::HashSet<String>>> = OnceLock::new();
    let cached = cache().lock().await.get(url).cloned();
    match cached {
        Some((t, v)) => {
            if t.elapsed() >= TTL {
                let busy = REFRESHING.get_or_init(Default::default);
                if busy.lock().await.insert(url.to_string()) {
                    let url = url.to_string();
                    tokio::spawn(async move {
                        refresh(url.clone()).await;
                        busy.lock().await.remove(&url);
                    });
                }
            }
            v
        }
        None => refresh(url.to_string()).await,
    }
}

/// A decimal ZEC string (`"0.69"`) in zatoshis.
fn zec_str(s: &str) -> Option<i64> {
    let (whole, frac) = s.trim().split_once('.').unwrap_or((s.trim(), ""));
    if frac.len() > 8
        || !whole
            .bytes()
            .chain(frac.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let whole: i64 = if whole.is_empty() {
        0
    } else {
        whole.parse().ok()?
    };
    let frac: i64 = format!("{frac:0<8}").parse().ok()?;
    whole.checked_mul(100_000_000)?.checked_add(frac)
}

fn int(v: &Value, k: &str) -> Option<i64> {
    match v.get(k)? {
        Value::String(s) => s.parse().ok(),
        n => n.as_i64(),
    }
}

fn zec(v: &Value, k: &str) -> Option<i64> {
    v.get(k)?.as_str().and_then(zec_str)
}

/// Order-book and sales figures reported by a project's marketplace. Amounts in zatoshis.
#[derive(Debug, Clone, Default, Serialize, ToSchema)]
pub struct VenueStats {
    pub venue: String,
    pub url: String,
    /// Lowest ask.
    pub floor: Option<i64>,
    pub listings: Option<i64>,
    /// Items with an open bid.
    pub bids: Option<i64>,
    pub volume: Option<i64>,
    pub sales: Option<i64>,
    pub volume_24h: Option<i64>,
    pub sales_24h: Option<i64>,
    pub owners: Option<i64>,
    /// Items minted so far, for launches in progress.
    pub minted: Option<i64>,
}

pub async fn stats(p: &Project) -> Option<VenueStats> {
    let m = p.market.as_ref()?;
    let base = VenueStats {
        venue: reqwest::Url::parse(&m.url)
            .ok()?
            .host_str()?
            .trim_start_matches("www.")
            .to_string(),
        url: m.url.clone(),
        ..Default::default()
    };
    match m.kind.as_str() {
        "zemon" => {
            let v = get_json("https://zemon.cash/api/stats").await?;
            Some(VenueStats {
                floor: int(&v, "floorZat"),
                listings: int(&v, "listings"),
                volume: int(&v, "volumeZat"),
                sales: int(&v, "sales"),
                owners: int(&v, "owners"),
                ..base
            })
        }
        "zilkroad" => {
            let v = get_json(&format!("{ZILKROAD}/api/market/stats")).await?;
            let items = zilkroad_items().await;
            let count = |f: fn(&VenueItem) -> bool| {
                items
                    .as_ref()
                    .map(|all| all.iter().filter(|i| f(i)).count() as i64)
            };
            Some(VenueStats {
                floor: zec(&v, "floorZec"),
                listings: count(|i| i.ask.is_some()),
                bids: count(|i| i.bid.is_some()),
                volume: zec(&v, "volumeTotalZec"),
                sales: int(&v, "salesTotal"),
                volume_24h: zec(&v, "volume24hZec"),
                sales_24h: int(&v, "sales24h"),
                ..base
            })
        }
        "zeccat" => {
            let v = get_json("https://zeccat.com/api/marketplace").await?;
            Some(VenueStats {
                floor: int(&v, "floorZatoshis"),
                listings: int(&v, "listed"),
                volume: int(&v, "volumeZatoshis"),
                sales: int(&v, "sales"),
                ..base
            })
        }
        "zecpad" => {
            let id = m.id.as_deref()?;
            let v = get_json(&format!("https://zecpad.net/api/collection?slug={id}")).await?;
            Some(VenueStats {
                minted: int(&v, "minted"),
                ..base
            })
        }
        _ => None,
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Trait {
    pub trait_type: String,
    pub value: String,
}

/// An item whose records live on a marketplace, not on chain.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct VenueItem {
    pub id: String,
    pub name: String,
    pub traits: Vec<Trait>,
    /// Rarity rank, 1 = rarest.
    pub rank: Option<i64>,
    /// Current ask, zatoshis.
    pub ask: Option<i64>,
    /// Best open bid, zatoshis.
    pub bid: Option<i64>,
    /// Last sale, zatoshis.
    pub last: Option<i64>,
    /// The item on its marketplace.
    pub url: String,
}

impl VenueItem {
    fn trait_value(&self, trait_type: &str) -> Option<&str> {
        self.traits
            .iter()
            .find(|t| t.trait_type == trait_type)
            .map(|t| t.value.as_str())
    }
}

fn is_zilkroad(p: &Project) -> bool {
    p.market.as_ref().is_some_and(|m| m.kind == "zilkroad")
}

/// All items of an off-chain project (Zilkroad publishes the full set in one document), parsed
/// once per [`TTL`].
async fn zilkroad_items() -> Option<Arc<Vec<VenueItem>>> {
    type Entry = Option<(Instant, Arc<Vec<VenueItem>>)>;
    static PARSED: OnceLock<Mutex<Entry>> = OnceLock::new();
    let parsed = PARSED.get_or_init(Default::default);
    if let Some((t, v)) = parsed.lock().await.as_ref() {
        if t.elapsed() < TTL {
            return Some(v.clone());
        }
    }
    let v = get_json(&format!("{ZILKROAD}/api/market/tokens")).await?;
    let tokens = v.get("tokens")?.as_array()?;
    let list: Vec<VenueItem> = tokens
        .iter()
        .filter_map(|t| {
            let id = t.get("id")?.as_i64()?;
            let traits = t
                .get("traits")
                .and_then(|t| t.as_object())
                .map(|o| {
                    o.iter()
                        .filter_map(|(k, v)| {
                            Some(Trait {
                                trait_type: k.clone(),
                                value: v.as_str()?.to_string(),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            let amount = |k: &str| t.get(k).and_then(|a| a.as_str()).and_then(zec_str);
            Some(VenueItem {
                id: id.to_string(),
                name: format!("zkSNARKs #{id}"),
                traits,
                rank: t.get("rank").and_then(|r| r.as_i64()),
                ask: amount("ask"),
                bid: amount("bid"),
                last: amount("last"),
                url: format!("{ZILKROAD}/#/{id}"),
            })
        })
        .collect();
    let list = Arc::new(list);
    *parsed.lock().await = Some((Instant::now(), list.clone()));
    Some(list)
}

/// Which off-chain items to list.
#[derive(Debug, Default)]
pub struct ItemFilter {
    /// `id` (default), `rank` (rarest first), `price` (cheapest listed first), `price_desc`,
    /// `last` (highest last sale first) or `bid` (highest bid first).
    pub sort: String,
    /// `(trait_type, value)` pairs: an item must match one value of every trait type named.
    pub traits: Vec<(String, String)>,
    /// `listed`, `bids` or `sold` (has sold at least once).
    pub status: Option<String>,
    /// An item number.
    pub q: Option<String>,
}

impl ItemFilter {
    fn keeps(&self, i: &VenueItem) -> bool {
        let status = match self.status.as_deref() {
            Some("listed") => i.ask.is_some(),
            Some("bids") => i.bid.is_some(),
            Some("sold") => i.last.is_some(),
            _ => true,
        };
        let q = self
            .q
            .as_deref()
            .is_none_or(|q| i.id == q.trim_start_matches('#'));
        let mut types: Vec<&str> = self.traits.iter().map(|(t, _)| t.as_str()).collect();
        types.sort_unstable();
        types.dedup();
        let traits = types.iter().all(|ty| {
            let have = i.trait_value(ty);
            self.traits
                .iter()
                .any(|(t, v)| t == ty && have == Some(v.as_str()))
        });
        status && q && traits
    }
}

/// A page of items matching `f`, with how many match.
pub async fn items(
    p: &Project,
    f: &ItemFilter,
    offset: usize,
    limit: usize,
) -> Option<(Vec<VenueItem>, usize)> {
    if !is_zilkroad(p) {
        return None;
    }
    let all = zilkroad_items().await?;
    let mut list: Vec<&VenueItem> = all.iter().filter(|i| f.keeps(i)).collect();
    let desc = |v: Option<i64>| std::cmp::Reverse(v.unwrap_or(i64::MIN));
    match f.sort.as_str() {
        "rank" => list.sort_by_key(|i| i.rank.unwrap_or(i64::MAX)),
        "price" => list.sort_by_key(|i| i.ask.unwrap_or(i64::MAX)),
        "price_desc" => list.sort_by_key(|i| desc(i.ask)),
        "last" => list.sort_by_key(|i| desc(i.last)),
        "bid" => list.sort_by_key(|i| desc(i.bid)),
        _ => {}
    }
    let total = list.len();
    Some((
        list.into_iter().skip(offset).take(limit).cloned().collect(),
        total,
    ))
}

/// One value of a trait across an off-chain collection.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TraitValue {
    pub trait_type: String,
    pub value: String,
    /// Items with this value.
    pub count: i64,
    /// Of those, listed for sale now.
    pub listed: i64,
    /// Lowest ask among them, zatoshis.
    pub floor: Option<i64>,
    /// Sales of items with this value in the last 7 days (ZScan's archive).
    pub sales_7d: i64,
    /// Median of those sales, zatoshis.
    pub median_7d: Option<i64>,
}

/// Every trait value of an off-chain collection with its count and floor, by trait type then
/// rarest first. `recent` are archived sales (`item`, `price`) used for the 7-day figures.
pub async fn traits(p: &Project, recent: &[(String, i64)]) -> Vec<TraitValue> {
    if !is_zilkroad(p) {
        return vec![];
    }
    let Some(all) = zilkroad_items().await else {
        return vec![];
    };
    let by_id: HashMap<&str, &VenueItem> = all.iter().map(|i| (i.id.as_str(), i)).collect();
    let mut sold: HashMap<(&str, &str), Vec<i64>> = HashMap::new();
    for (item, price) in recent {
        if let Some(i) = by_id.get(item.as_str()) {
            for t in &i.traits {
                sold.entry((&t.trait_type, &t.value))
                    .or_default()
                    .push(*price);
            }
        }
    }
    let mut map: BTreeMap<(&str, &str), TraitValue> = BTreeMap::new();
    for i in all.iter() {
        for t in &i.traits {
            let e = map
                .entry((&t.trait_type, &t.value))
                .or_insert_with(|| TraitValue {
                    trait_type: t.trait_type.clone(),
                    value: t.value.clone(),
                    count: 0,
                    listed: 0,
                    floor: None,
                    sales_7d: 0,
                    median_7d: None,
                });
            e.count += 1;
            if let Some(a) = i.ask {
                e.listed += 1;
                e.floor = Some(e.floor.map_or(a, |f| f.min(a)));
            }
        }
    }
    let mut out: Vec<TraitValue> = map
        .into_iter()
        .map(|(k, mut v)| {
            if let Some(prices) = sold.get_mut(&k) {
                v.sales_7d = prices.len() as i64;
                v.median_7d = median(prices);
            }
            v
        })
        .collect();
    out.sort_by(|a, b| a.trait_type.cmp(&b.trait_type).then(a.count.cmp(&b.count)));
    out
}

pub fn median(v: &mut [i64]) -> Option<i64> {
    if v.is_empty() {
        return None;
    }
    v.sort_unstable();
    let n = v.len();
    Some(if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2
    })
}

/// Rarity bands, by rank (1 = rarest), for a collection of `supply` items.
fn tier_bounds(supply: i64) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    let mut lo = 1;
    for hi in [10, 100, 500, 1_000, 2_500, 5_000, 10_000, 100_000] {
        if lo > supply {
            break;
        }
        out.push((lo, hi.min(supply)));
        lo = hi + 1;
    }
    out
}

/// Market figures for one rarity band.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Tier {
    /// Ranks covered, inclusive.
    pub from_rank: i64,
    pub to_rank: i64,
    pub items: i64,
    pub listed: i64,
    /// Lowest ask in the band, zatoshis.
    pub floor: Option<i64>,
    /// Sales in the last 7 days.
    pub sales_7d: i64,
    /// Median sale in the last 7 days, zatoshis.
    pub median_7d: Option<i64>,
}

/// A listing priced under what comparable items (same rarity band) sold for lately.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Deal {
    pub item: VenueItem,
    /// Median of the band's sales in the last 7 days, zatoshis.
    pub reference: i64,
    /// How far the ask is below it, in percent.
    pub discount_pct: f64,
}

/// Listings bucketed by price, as multiples of the floor.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DepthBucket {
    /// Upper bound, zatoshis (`None` for the last, open-ended bucket).
    pub up_to: Option<i64>,
    pub listings: i64,
}

/// Order-book analysis of an off-chain collection.
#[derive(Debug, Clone, Default, Serialize, ToSchema)]
pub struct Book {
    pub tiers: Vec<Tier>,
    pub deals: Vec<Deal>,
    pub depth: Vec<DepthBucket>,
    /// Best open bid across the collection, zatoshis.
    pub top_bid: Option<i64>,
}

/// Fewest recent sales in a band for its median to be used as a reference price.
const MIN_COMPARABLES: usize = 5;
/// Smallest discount to that median for a listing to count as a deal.
const MIN_DISCOUNT: f64 = 0.10;

pub async fn book(p: &Project, recent: &[(String, i64)]) -> Book {
    if !is_zilkroad(p) {
        return Book::default();
    }
    let Some(all) = zilkroad_items().await else {
        return Book::default();
    };
    let supply = all.len() as i64;
    let rank_of: HashMap<&str, i64> = all
        .iter()
        .filter_map(|i| Some((i.id.as_str(), i.rank?)))
        .collect();
    let bounds = tier_bounds(supply);
    let band = |rank: i64| {
        bounds
            .iter()
            .position(|(lo, hi)| (*lo..=*hi).contains(&rank))
    };
    let mut sold: Vec<Vec<i64>> = vec![vec![]; bounds.len()];
    for (item, price) in recent {
        if let Some(b) = rank_of.get(item.as_str()).and_then(|r| band(*r)) {
            sold[b].push(*price);
        }
    }
    let mut tiers: Vec<Tier> = bounds
        .iter()
        .map(|(lo, hi)| Tier {
            from_rank: *lo,
            to_rank: *hi,
            items: 0,
            listed: 0,
            floor: None,
            sales_7d: 0,
            median_7d: None,
        })
        .collect();
    for i in all.iter() {
        let Some(b) = i.rank.and_then(band) else {
            continue;
        };
        let t = &mut tiers[b];
        t.items += 1;
        if let Some(a) = i.ask {
            t.listed += 1;
            t.floor = Some(t.floor.map_or(a, |f| f.min(a)));
        }
    }
    let mut reference = vec![None; bounds.len()];
    for (b, prices) in sold.iter_mut().enumerate() {
        tiers[b].sales_7d = prices.len() as i64;
        tiers[b].median_7d = median(prices);
        if prices.len() >= MIN_COMPARABLES {
            reference[b] = tiers[b].median_7d;
        }
    }

    let mut deals: Vec<Deal> = all
        .iter()
        .filter_map(|i| {
            let ask = i.ask?;
            let r = reference[i.rank.and_then(band)?]?;
            // Within a few percent of the median is ordinary pricing, not a deal.
            (ask as f64 <= r as f64 * (1.0 - MIN_DISCOUNT)).then(|| Deal {
                item: i.clone(),
                reference: r,
                discount_pct: (1.0 - ask as f64 / r as f64) * 100.0,
            })
        })
        .collect();
    deals.sort_by(|a, b| b.discount_pct.total_cmp(&a.discount_pct));
    deals.truncate(12);

    let asks: Vec<i64> = all.iter().filter_map(|i| i.ask).collect();
    let depth = match asks.iter().min() {
        Some(&floor) => {
            let edges: Vec<Option<i64>> = [1.1, 1.25, 1.5, 2.0, 3.0, 5.0, 10.0]
                .iter()
                .map(|m| Some((floor as f64 * m) as i64))
                .chain([None])
                .collect();
            let mut lo = 0;
            edges
                .into_iter()
                .map(|up| {
                    let n = asks
                        .iter()
                        .filter(|&&a| a >= lo && up.is_none_or(|u| a < u))
                        .count() as i64;
                    lo = up.unwrap_or(i64::MAX);
                    DepthBucket {
                        up_to: up,
                        listings: n,
                    }
                })
                .collect()
        }
        None => vec![],
    };

    Book {
        tiers,
        deals,
        depth,
        top_bid: all.iter().filter_map(|i| i.bid).max(),
    }
}

/// Market facts about one off-chain item, for its page.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ItemTrait {
    pub trait_type: String,
    pub value: String,
    /// Share of the collection with this value, percent.
    pub pct: f64,
    /// Lowest ask among items with this value, zatoshis.
    pub floor: Option<i64>,
}

/// The item's traits with their rarity and floors, and its rarity band.
pub async fn item_context(
    p: &Project,
    id: &str,
    recent: &[(String, i64)],
) -> (Vec<ItemTrait>, Option<Tier>) {
    let (Some(all), true) = (zilkroad_items().await, is_zilkroad(p)) else {
        return (vec![], None);
    };
    let Some(item) = all.iter().find(|i| i.id == id) else {
        return (vec![], None);
    };
    let supply = all.len() as f64;
    let traits = item
        .traits
        .iter()
        .map(|t| {
            let same = all
                .iter()
                .filter(|i| i.trait_value(&t.trait_type) == Some(t.value.as_str()));
            let (mut n, mut floor) = (0, None::<i64>);
            for i in same {
                n += 1;
                if let Some(a) = i.ask {
                    floor = Some(floor.map_or(a, |f| f.min(a)));
                }
            }
            ItemTrait {
                trait_type: t.trait_type.clone(),
                value: t.value.clone(),
                pct: n as f64 / supply * 100.0,
                floor,
            }
        })
        .collect();
    let tier = match item.rank {
        Some(r) => book(p, recent)
            .await
            .tiers
            .into_iter()
            .find(|t| (t.from_rank..=t.to_rank).contains(&r)),
        None => None,
    };
    (traits, tier)
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct VenueSale {
    pub item_id: String,
    pub name: String,
    pub price: i64,
    pub time: String,
}

/// A sale as the venue reports it, ready to archive.
struct RawSale {
    item: String,
    time: DateTime<Utc>,
    price: i64,
    via: Option<String>,
}

async fn zilkroad_sales(limit: usize) -> Vec<RawSale> {
    let Some(v) = get_json(&format!("{ZILKROAD}/api/market/sales?limit={limit}")).await else {
        return Vec::new();
    };
    v.get("sales")
        .and_then(|s| s.as_array())
        .into_iter()
        .flatten()
        .filter_map(|s| {
            Some(RawSale {
                item: s.get("id")?.as_i64()?.to_string(),
                price: zec_str(s.get("price")?.as_str()?)?,
                time: s.get("time")?.as_str()?.parse().ok()?,
                via: s.get("via").and_then(|v| v.as_str()).map(String::from),
            })
        })
        .collect()
}

/// Latest sales reported by the venue (for off-chain projects; on-chain sales come from the
/// chain).
pub async fn sales(p: &Project) -> Vec<VenueSale> {
    if !is_zilkroad(p) {
        return Vec::new();
    }
    zilkroad_sales(50)
        .await
        .into_iter()
        .map(|s| VenueSale {
            name: format!("zkSNARKs #{}", s.item),
            item_id: s.item,
            price: s.price,
            time: s.time.to_rfc3339(),
        })
        .collect()
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct VenueEvent {
    /// As the venue names it, e.g. snark_minted, sale, listing.
    pub kind: String,
    pub price: Option<i64>,
    pub time: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct VenueItemDetail {
    #[serde(flatten)]
    pub item: VenueItem,
    pub description: Option<String>,
    pub minted_at: Option<String>,
    pub history: Vec<VenueEvent>,
    /// Traits with their share of the collection and floor.
    pub trait_stats: Vec<ItemTrait>,
    /// The item's rarity band, with what comparable items trade at.
    pub tier: Option<Tier>,
    /// Every sale of the item ZScan has archived, oldest first.
    pub sales: Vec<ItemSale>,
}

#[derive(Debug, Clone, Serialize, ToSchema, sqlx::FromRow)]
pub struct ItemSale {
    pub time: DateTime<Utc>,
    pub price: i64,
    pub via: Option<String>,
}

/// Event types that are sales.
fn is_sale(kind: &str) -> bool {
    matches!(kind, "order_paid" | "offer_accepted")
}

/// One off-chain item with its venue history.
pub async fn item(p: &Project, id: &str) -> Option<VenueItemDetail> {
    if !is_zilkroad(p) || !id.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let listed = zilkroad_items()
        .await
        .and_then(|all| all.iter().find(|i| i.id == id).cloned());
    let v = get_json(&format!("{ZILKROAD}/api/snarks/{id}")).await?;
    let snark = v.get("snark")?;
    let md = snark.get("metadata");
    let traits = md
        .and_then(|m| m.get("attributes"))
        .and_then(|a| a.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|t| {
                    Some(Trait {
                        trait_type: t.get("trait_type")?.as_str()?.to_string(),
                        value: t.get("value")?.as_str()?.to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let history = v
        .get("history")
        .and_then(|h| h.as_array())
        .into_iter()
        .flatten()
        .take(50)
        .map(|e| VenueEvent {
            kind: e
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("event")
                .to_string(),
            price: e
                .get("amountZec")
                .and_then(|a| a.as_str())
                .and_then(zec_str),
            time: e
                .get("createdAt")
                .or_else(|| e.get("at"))
                .and_then(|t| t.as_str())
                .map(String::from),
        })
        .collect();
    Some(VenueItemDetail {
        item: VenueItem {
            id: id.to_string(),
            name: snark
                .get("name")
                .and_then(|n| n.as_str())
                .map(String::from)
                .unwrap_or_else(|| format!("zkSNARKs #{id}")),
            traits,
            rank: listed.as_ref().and_then(|i| i.rank),
            ask: v
                .get("listing")
                .and_then(|l| l.get("priceZec"))
                .and_then(|p| p.as_str())
                .and_then(zec_str),
            bid: listed.as_ref().and_then(|i| i.bid),
            last: listed.as_ref().and_then(|i| i.last),
            url: format!("{ZILKROAD}/#/{id}"),
        },
        description: md
            .and_then(|m| m.get("description"))
            .and_then(|d| d.as_str())
            .map(String::from),
        minted_at: snark
            .get("mintedAt")
            .and_then(|t| t.as_str())
            .map(String::from),
        history,
        trait_stats: vec![],
        tier: None,
        sales: vec![],
    })
}

/// Where an off-chain item's picture is, for the image relay.
pub fn item_image_source(p: &Project, id: &str) -> Result<String> {
    let m = p.market.as_ref().context("no marketplace")?;
    anyhow::ensure!(
        m.kind == "zilkroad"
            && !id.is_empty()
            && id.len() <= 6
            && id.bytes().all(|b| b.is_ascii_digit()),
        "no item images for {}",
        p.slug
    );
    Ok(format!("{ZILKROAD}/api/art/{id}"))
}

async fn store_sales(db: &PgPool, project: &str, sales: &[RawSale]) -> Result<u64> {
    let mut n = 0;
    for s in sales {
        n += sqlx::query(
            "INSERT INTO nft_market_sales (project, item, time, price, via)
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
        )
        .bind(project)
        .bind(&s.item)
        .bind(s.time)
        .bind(s.price)
        .bind(&s.via)
        .execute(db)
        .await?
        .rows_affected();
    }
    Ok(n)
}

/// Samples every project's order book and archives the venues' latest sales, forever.
pub async fn archive(db: PgPool) {
    tokio::spawn(backfill(db.clone()));
    tokio::spawn(warm_images(db.clone()));
    loop {
        if let Err(e) = snapshot(&db).await {
            tracing::warn!("nft market snapshot: {e:#}");
        }
        tokio::time::sleep(SNAPSHOT_EVERY).await;
    }
}

async fn snapshot(db: &PgPool) -> Result<()> {
    let now = Utc::now();
    for p in nft_projects::projects()
        .iter()
        .filter(|p| p.market.is_some())
    {
        if let Some(s) = stats(p).await {
            sqlx::query(
                "INSERT INTO nft_market_snapshots
                    (project, time, floor, listings, bids, volume, sales, owners)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING",
            )
            .bind(&p.slug)
            .bind(now)
            .bind(s.floor)
            .bind(s.listings.map(|n| n as i32))
            .bind(s.bids.map(|n| n as i32))
            .bind(s.volume)
            .bind(s.sales)
            .bind(s.owners.map(|n| n as i32))
            .execute(db)
            .await?;
        }
        if is_zilkroad(p) {
            let n = store_sales(db, &p.slug, &zilkroad_sales(500).await).await?;
            if n > 0 {
                tracing::debug!(project = %p.slug, new = n, "nft sales archived");
            }
        }
    }
    Ok(())
}

/// One-time import of every past sale: the venue's sales feed only reaches back a few days,
/// but each item's page carries its whole history. Resumes where it stopped.
async fn backfill(db: PgPool) {
    loop {
        for p in nft_projects::projects().iter().filter(|p| is_zilkroad(p)) {
            if let Err(e) = backfill_project(&db, p).await {
                tracing::warn!("nft sales backfill {}: {e:#}", p.slug);
            }
        }
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

async fn backfill_project(db: &PgPool, p: &Project) -> Result<()> {
    let row: Option<(i64, bool)> =
        sqlx::query_as("SELECT next_item, done FROM nft_market_backfill WHERE project = $1")
            .bind(&p.slug)
            .fetch_optional(db)
            .await?;
    let (mut next, done) = row.unwrap_or((1, false));
    if done {
        return Ok(());
    }
    let last = p.supply.unwrap_or(10_000);
    tracing::info!(project = %p.slug, from = next, "nft sales backfill");
    while next <= last {
        let Some(v) = fetch_json(&format!("{ZILKROAD}/api/snarks/{next}")).await else {
            // Venue unreachable or throttling: try again on the next round.
            return Ok(());
        };
        let sales: Vec<RawSale> = v
            .get("history")
            .and_then(|h| h.as_array())
            .into_iter()
            .flatten()
            .filter(|e| e.get("type").and_then(|t| t.as_str()).is_some_and(is_sale))
            .filter_map(|e| {
                Some(RawSale {
                    item: next.to_string(),
                    price: zec_str(e.get("amountZec")?.as_str()?)?,
                    time: e.get("createdAt")?.as_str()?.parse().ok()?,
                    via: e.get("type").and_then(|t| t.as_str()).map(|t| {
                        if t == "offer_accepted" {
                            "offer"
                        } else {
                            "ask"
                        }
                        .to_string()
                    }),
                })
            })
            .collect();
        store_sales(db, &p.slug, &sales).await?;
        next += 1;
        sqlx::query(
            "INSERT INTO nft_market_backfill (project, next_item, done) VALUES ($1, $2, $3)
             ON CONFLICT (project) DO UPDATE SET next_item = $2, done = $3",
        )
        .bind(&p.slug)
        .bind(next)
        .bind(next > last)
        .execute(db)
        .await?;
        tokio::time::sleep(BACKFILL_PAUSE).await;
    }
    tracing::info!(project = %p.slug, "nft sales backfill done");
    Ok(())
}

/// Saves a relayed item picture so later views don't need the venue.
pub async fn store_image(db: &PgPool, project: &str, item: &str, ctype: &str, body: &[u8]) {
    let r = sqlx::query(
        "INSERT INTO nft_item_images (project, item, content_type, body) VALUES ($1, $2, $3, $4)
         ON CONFLICT DO NOTHING",
    )
    .bind(project)
    .bind(item)
    .bind(ctype)
    .bind(body)
    .execute(db)
    .await;
    if let Err(e) = r {
        tracing::warn!("store image {project}/{item}: {e:#}");
    }
}

pub async fn stored_image(db: &PgPool, project: &str, item: &str) -> Option<(String, Vec<u8>)> {
    sqlx::query_as(
        "SELECT content_type, body FROM nft_item_images WHERE project = $1 AND item = $2",
    )
    .bind(project)
    .bind(item)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
}

/// Fetches, slowly, the pictures of off-chain items not stored yet.
async fn warm_images(db: PgPool) {
    loop {
        for p in nft_projects::projects().iter().filter(|p| is_zilkroad(p)) {
            let have: Vec<String> =
                sqlx::query_scalar("SELECT item FROM nft_item_images WHERE project = $1")
                    .bind(&p.slug)
                    .fetch_all(&db)
                    .await
                    .unwrap_or_default();
            let have: std::collections::HashSet<String> = have.into_iter().collect();
            for id in 1..=p.supply.unwrap_or(0) {
                let id = id.to_string();
                if have.contains(&id) {
                    continue;
                }
                let Ok(src) = item_image_source(p, &id) else {
                    break;
                };
                match crate::ipfs::image(&src, None).await {
                    Ok((ctype, body)) => store_image(&db, &p.slug, &id, &ctype, &body).await,
                    Err(e) => tracing::debug!("warm image {src}: {e:#}"),
                }
                tokio::time::sleep(BACKFILL_PAUSE).await;
            }
        }
        tokio::time::sleep(Duration::from_secs(6 * 3600)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zec_strings() {
        assert_eq!(zec_str("0.69"), Some(69_000_000));
        assert_eq!(zec_str("33.33"), Some(3_333_000_000));
        assert_eq!(zec_str("5680.92592299"), Some(568_092_592_299));
        assert_eq!(zec_str("1"), Some(100_000_000));
        assert_eq!(zec_str("0.123456789"), None);
        assert_eq!(zec_str("-1"), None);
    }

    #[test]
    fn tiers_cover_the_supply() {
        let t = tier_bounds(10_000);
        assert_eq!(t.first(), Some(&(1, 10)));
        assert_eq!(t.last(), Some(&(5_001, 10_000)));
        assert_eq!(tier_bounds(999).last(), Some(&(501, 999)));
    }

    #[test]
    fn medians() {
        assert_eq!(median(&mut []), None);
        assert_eq!(median(&mut [3, 1, 2]), Some(2));
        assert_eq!(median(&mut [4, 1, 2, 3]), Some(2));
    }

    fn item(id: &str, traits: &[(&str, &str)], ask: Option<i64>) -> VenueItem {
        VenueItem {
            id: id.into(),
            name: id.into(),
            traits: traits
                .iter()
                .map(|(t, v)| Trait {
                    trait_type: (*t).into(),
                    value: (*v).into(),
                })
                .collect(),
            rank: None,
            ask,
            bid: None,
            last: None,
            url: String::new(),
        }
    }

    #[test]
    fn filters_and_or_traits() {
        let a = item("1", &[("Eyes", "Volt"), ("Head", "Nyx")], Some(1));
        let b = item("2", &[("Eyes", "Ruby"), ("Head", "Nyx")], None);
        let f = ItemFilter {
            traits: vec![
                ("Eyes".into(), "Volt".into()),
                ("Eyes".into(), "Ruby".into()),
                ("Head".into(), "Nyx".into()),
            ],
            ..Default::default()
        };
        assert!(f.keeps(&a) && f.keeps(&b));
        let listed = ItemFilter {
            status: Some("listed".into()),
            ..f
        };
        assert!(listed.keeps(&a) && !listed.keeps(&b));
        let q = ItemFilter {
            q: Some("#2".into()),
            ..Default::default()
        };
        assert!(!q.keeps(&a) && q.keeps(&b));
    }
}
