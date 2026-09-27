//! Public marketplace APIs for NFT projects in the registry (`labels/nft_projects.json`):
//! order-book figures, and items and sales for projects whose records live off chain. Every
//! figure here is as the venue reports it; responses are cached a few minutes.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex;
use utoipa::ToSchema;
use zscan_chain::nft_projects::Project;

const TTL: Duration = Duration::from_secs(300);
const ZILKROAD: &str = "https://zilkroad.com";

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

/// GET a JSON document, cached for [`TTL`]. Failures are cached too (briefly), so a venue
/// that's down doesn't slow every page.
async fn get_json(url: &str) -> Option<Value> {
    type Entry = (Instant, Option<Value>);
    static CACHE: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some((t, v)) = cache.lock().await.get(url) {
        if t.elapsed() < TTL {
            return v.clone();
        }
    }
    let v = match client()
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
    };
    cache
        .lock()
        .await
        .insert(url.to_string(), (Instant::now(), v.clone()));
    v
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
            Some(VenueStats {
                floor: zec(&v, "floorZec"),
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
    /// Last sale, zatoshis.
    pub last: Option<i64>,
    /// The item on its marketplace.
    pub url: String,
}

/// All items of an off-chain project (Zilkroad publishes the full set in one document).
async fn zilkroad_items() -> Option<Vec<VenueItem>> {
    let v = get_json(&format!("{ZILKROAD}/api/market/tokens")).await?;
    let tokens = v.get("tokens")?.as_array()?;
    Some(
        tokens
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
                Some(VenueItem {
                    id: id.to_string(),
                    name: format!("zkSNARKs #{id}"),
                    traits,
                    rank: t.get("rank").and_then(|r| r.as_i64()),
                    ask: t.get("ask").and_then(|a| a.as_str()).and_then(zec_str),
                    last: t.get("last").and_then(|a| a.as_str()).and_then(zec_str),
                    url: format!("{ZILKROAD}/#/{id}"),
                })
            })
            .collect(),
    )
}

/// A page of items, sorted by `id`, `rank` or `price` (listed first, cheapest first).
pub async fn items(
    p: &Project,
    sort: &str,
    offset: usize,
    limit: usize,
) -> Option<(Vec<VenueItem>, usize)> {
    if p.market.as_ref()?.kind != "zilkroad" {
        return None;
    }
    let mut all = zilkroad_items().await?;
    match sort {
        "rank" => all.sort_by_key(|i| i.rank.unwrap_or(i64::MAX)),
        "price" => all.sort_by_key(|i| i.ask.unwrap_or(i64::MAX)),
        _ => {}
    }
    let total = all.len();
    Some((all.into_iter().skip(offset).take(limit).collect(), total))
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct VenueSale {
    pub item_id: String,
    pub name: String,
    pub price: i64,
    pub time: String,
}

/// Latest sales reported by the venue (for off-chain projects; on-chain sales come from the
/// chain).
pub async fn sales(p: &Project) -> Vec<VenueSale> {
    if p.market.as_ref().map(|m| m.kind.as_str()) != Some("zilkroad") {
        return Vec::new();
    }
    let Some(v) = get_json(&format!("{ZILKROAD}/api/market/sales?limit=50")).await else {
        return Vec::new();
    };
    v.get("sales")
        .and_then(|s| s.as_array())
        .into_iter()
        .flatten()
        .filter_map(|s| {
            let id = s.get("id")?.as_i64()?;
            Some(VenueSale {
                item_id: id.to_string(),
                name: format!("zkSNARKs #{id}"),
                price: zec_str(s.get("price")?.as_str()?)?,
                time: s.get("time")?.as_str()?.to_string(),
            })
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
}

/// One off-chain item with its venue history.
pub async fn item(p: &Project, id: &str) -> Option<VenueItemDetail> {
    if p.market.as_ref()?.kind != "zilkroad" || !id.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let listed = zilkroad_items()
        .await
        .and_then(|all| all.into_iter().find(|i| i.id == id));
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
    })
}

/// Where an off-chain item's picture is, for the image relay.
pub fn item_image_source(p: &Project, id: &str) -> Result<String> {
    let m = p.market.as_ref().context("no marketplace")?;
    anyhow::ensure!(
        m.kind == "zilkroad" && !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()),
        "no item images for {}",
        p.slug
    );
    Ok(format!("{ZILKROAD}/api/art/{id}"))
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
}
