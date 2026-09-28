//! ZEC markets (spot and perpetual venues) and ZEC on other chains, from the tables the
//! background collector keeps (see `crate::market`).

use std::collections::BTreeMap;

use axum::extract::{Query, State};
use axum::Json;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::error::ApiResult;
use crate::market;
use crate::AppState;

/// Figures older than this are flagged `stale` (the venue failed several rounds in a row).
const STALE_MINUTES: i64 = 30;

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct VenueRow {
    pub venue: String,
    #[sqlx(skip)]
    pub name: String,
    /// spot or perp.
    pub market: String,
    pub pair: String,
    /// cex or dex.
    pub kind: String,
    pub price: Option<f64>,
    pub volume_24h_usd: Option<f64>,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    /// Bid/ask spread in basis points.
    #[sqlx(skip)]
    pub spread_bps: Option<f64>,
    pub oi_usd: Option<f64>,
    pub oi_zec: Option<f64>,
    /// Funding rate over 8 hours (0.0001 = 0.01%).
    pub funding_8h: Option<f64>,
    /// Annualized funding (3 payments a day).
    #[sqlx(skip)]
    pub funding_apr: Option<f64>,
    /// Share of the 24h volume of its market type.
    #[sqlx(skip)]
    pub volume_share: Option<f64>,
    pub updated_at: DateTime<Utc>,
    #[sqlx(skip)]
    pub stale: bool,
}

#[derive(Debug, Default, Serialize, ToSchema)]
pub struct MarketTotals {
    pub spot_volume_24h_usd: f64,
    pub perp_volume_24h_usd: f64,
    /// On-chain venues: Hyperliquid spot plus DEX pools of ZEC on other chains.
    pub dex_volume_24h_usd: f64,
    pub oi_usd: f64,
    pub oi_zec: f64,
    /// Open-interest-weighted 8-hour funding rate.
    pub funding_8h: Option<f64>,
    /// Perp volume over spot volume.
    pub perp_spot_ratio: Option<f64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MarketOverview {
    pub price_usd: Option<f64>,
    pub change_24h: Option<f64>,
    pub market_cap_usd: Option<f64>,
    /// Price × 21M ZEC.
    pub fdv_usd: Option<f64>,
    pub totals: MarketTotals,
    pub venues: Vec<VenueRow>,
}

/// ZEC markets now: every tracked spot and perpetual venue (price, 24h volume, spread, open
/// interest, funding) and the totals. Refreshed every 5 minutes.
#[utoipa::path(get, path = "/v1/market", responses((status = 200, body = MarketOverview)), tag = "market")]
pub async fn overview(State(s): State<AppState>) -> ApiResult<MarketOverview> {
    let mut venues = sqlx::query_as::<_, VenueRow>(
        "SELECT venue, market, pair, kind, price, volume_24h_usd, bid, ask, oi_usd, oi_zec,
                funding_8h, updated_at
         FROM market_venues ORDER BY market DESC, volume_24h_usd DESC NULLS LAST",
    )
    .fetch_all(&s.db.pool)
    .await?;
    let dex_assets: Option<f64> =
        sqlx::query_scalar("SELECT sum(volume_24h_usd) FROM zec_assets_latest")
            .fetch_one(&s.db.pool)
            .await?;

    let now = Utc::now();
    let mut t = MarketTotals::default();
    let (mut fw, mut fwsum) = (0.0, 0.0);
    for v in venues.iter_mut() {
        v.name = market::venue_name(&v.venue).to_string();
        v.stale = (now - v.updated_at).num_minutes() > STALE_MINUTES;
        v.spread_bps = match (v.bid, v.ask) {
            (Some(b), Some(a)) if b > 0.0 && a >= b => Some((a - b) / ((a + b) / 2.0) * 1e4),
            _ => None,
        };
        v.funding_apr = v.funding_8h.map(|r| r * 3.0 * 365.0);
        if v.stale {
            continue;
        }
        let vol = v.volume_24h_usd.unwrap_or(0.0);
        match v.market.as_str() {
            "spot" => t.spot_volume_24h_usd += vol,
            _ => t.perp_volume_24h_usd += vol,
        }
        if v.kind == "dex" && v.market == "spot" {
            t.dex_volume_24h_usd += vol;
        }
        if let Some(oi) = v.oi_usd {
            t.oi_usd += oi;
            t.oi_zec += v.oi_zec.unwrap_or(0.0);
            if let Some(r) = v.funding_8h {
                fw += r * oi;
                fwsum += oi;
            }
        }
    }
    t.dex_volume_24h_usd += dex_assets.unwrap_or(0.0);
    t.funding_8h = (fwsum > 0.0).then(|| fw / fwsum);
    t.perp_spot_ratio =
        (t.spot_volume_24h_usd > 0.0).then(|| t.perp_volume_24h_usd / t.spot_volume_24h_usd);
    for v in venues.iter_mut().filter(|v| !v.stale) {
        let total = if v.market == "spot" {
            t.spot_volume_24h_usd
        } else {
            t.perp_volume_24h_usd
        };
        v.volume_share = v.volume_24h_usd.filter(|_| total > 0.0).map(|x| x / total);
    }

    let price = s.price.lock().await.latest();
    Ok(Json(MarketOverview {
        price_usd: price.as_ref().map(|p| p.usd),
        change_24h: price.as_ref().map(|p| p.usd_24h_change),
        market_cap_usd: price.as_ref().map(|p| p.market_cap_usd),
        fdv_usd: price.as_ref().map(|p| p.usd * 21_000_000.0),
        totals: t,
        venues,
    }))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct DaysQuery {
    /// Number of days (default 365, max 3650).
    pub days: Option<i32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MarketDay {
    pub day: NaiveDate,
    /// Close of the most liquid spot venue with a candle that day.
    pub close: Option<f64>,
    pub spot_volume_usd: f64,
    pub perp_volume_usd: f64,
    /// Sum over the venues with a figure that day (coverage grows as venues' history starts).
    pub oi_usd: Option<f64>,
    /// Open-interest-weighted mean 8-hour funding.
    pub funding_8h: Option<f64>,
    /// Per venue: spot volume, perp volume, open interest (USD).
    pub spot: BTreeMap<String, f64>,
    pub perp: BTreeMap<String, f64>,
    pub oi: BTreeMap<String, f64>,
    /// Per venue: mean 8-hour funding.
    pub funding: BTreeMap<String, f64>,
}

type DailyRow = (
    String,
    String,
    NaiveDate,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
);

/// Daily ZEC market history: close, spot and perp volume per venue, open interest per venue and
/// funding, from each venue's candles and history endpoints.
#[utoipa::path(get, path = "/v1/market/daily", params(DaysQuery),
    responses((status = 200, body = [MarketDay])), tag = "market")]
pub async fn daily(
    State(s): State<AppState>,
    Query(q): Query<DaysQuery>,
) -> ApiResult<Vec<MarketDay>> {
    let rows: Vec<DailyRow> = sqlx::query_as(
        "SELECT venue, market, day, close, volume_usd, oi_usd, funding_8h FROM market_daily
         WHERE day > (now() AT TIME ZONE 'UTC')::date - $1 ORDER BY day",
    )
    .bind(q.days.unwrap_or(365).clamp(1, 3650))
    .fetch_all(&s.db.pool)
    .await?;
    // Close preference: the deepest spot books first.
    const CLOSE_ORDER: [&str; 4] = ["binance", "coinbase", "okx", "kraken"];
    let mut days: BTreeMap<NaiveDate, (MarketDay, Option<usize>, f64, f64)> = BTreeMap::new();
    for (venue, market, day, close, vol, oi, funding) in rows {
        let (d, close_rank, fw, fwsum) = days.entry(day).or_insert_with(|| {
            (
                MarketDay {
                    day,
                    close: None,
                    spot_volume_usd: 0.0,
                    perp_volume_usd: 0.0,
                    oi_usd: None,
                    funding_8h: None,
                    spot: BTreeMap::new(),
                    perp: BTreeMap::new(),
                    oi: BTreeMap::new(),
                    funding: BTreeMap::new(),
                },
                None,
                0.0,
                0.0,
            )
        });
        if market == "spot" {
            if let Some(c) = close {
                let rank = CLOSE_ORDER
                    .iter()
                    .position(|v| *v == venue)
                    .unwrap_or(CLOSE_ORDER.len());
                if close_rank.is_none_or(|r| rank < r) {
                    d.close = Some(c);
                    *close_rank = Some(rank);
                }
            }
            if let Some(v) = vol {
                d.spot_volume_usd += v;
                d.spot.insert(venue.clone(), v);
            }
        } else {
            if let Some(v) = vol {
                d.perp_volume_usd += v;
                d.perp.insert(venue.clone(), v);
            }
            if let Some(f) = funding {
                d.funding.insert(venue.clone(), f);
            }
            if let Some(o) = oi {
                *d.oi_usd.get_or_insert(0.0) += o;
                d.oi.insert(venue.clone(), o);
                if let Some(f) = funding {
                    *fw += f * o;
                    *fwsum += o;
                }
            }
        }
    }
    Ok(Json(
        days.into_values()
            .map(|(mut d, _, fw, fwsum)| {
                d.funding_8h = (fwsum > 0.0).then(|| fw / fwsum);
                d
            })
            .collect(),
    ))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ZecAsset {
    pub id: String,
    pub name: String,
    pub symbol: String,
    pub chain: String,
    pub chain_name: String,
    pub address: String,
    pub issuer: String,
    /// custodial, bridge or lock-and-mint.
    pub model: String,
    /// `zcash`, or the id of the asset locked to mint this one.
    pub backed_by: String,
    pub website: String,
    /// The token's holder list on its chain's explorer.
    pub explorer_url: String,
    /// Holders on its chain and the share held by the 10 largest (0–1), where indexed.
    pub holders: Option<i64>,
    pub holders_top10: Option<f64>,
    /// Total supply on its chain, ZEC.
    pub supply: Option<f64>,
    /// Part of the supply locked to back other listed assets.
    pub locked: Option<f64>,
    /// Supply circulating on this chain (supply − locked).
    pub circulating: Option<f64>,
    pub price_usd: Option<f64>,
    pub liquidity_usd: Option<f64>,
    pub volume_24h_usd: Option<f64>,
    #[schema(value_type = Object)]
    pub pools: serde_json::Value,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ChainTotal {
    pub chain: String,
    pub chain_name: String,
    /// ZEC circulating on the chain across its assets.
    pub zec: f64,
    pub liquidity_usd: f64,
    pub volume_24h_usd: f64,
    pub assets: usize,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Ecosystem {
    /// ZEC held for other chains (assets backed directly by ZEC; nothing counted twice).
    pub zec_outside: f64,
    /// Of the ZEC supply on Zcash.
    pub share_of_supply: Option<f64>,
    pub liquidity_usd: f64,
    pub volume_24h_usd: f64,
    pub chains: Vec<ChainTotal>,
    pub assets: Vec<ZecAsset>,
}

fn explorer_url(chain: &str, address: &str) -> String {
    match chain {
        "bsc" => format!("https://bscscan.com/token/{address}#balances"),
        "base" => format!("https://basescan.org/token/{address}#balances"),
        "eth" => format!("https://etherscan.io/token/{address}#balances"),
        "solana" => format!("https://solscan.io/token/{address}#holders"),
        "near" => format!("https://nearblocks.io/token/{address}?tab=holders"),
        "hyperliquid" => format!("https://app.hyperliquid.xyz/explorer/token/{address}"),
        _ => String::new(),
    }
}

type LatestRow = (
    String,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<serde_json::Value>,
    DateTime<Utc>,
    Option<i64>,
    Option<f64>,
);

/// ZEC on other chains: every ZEC-backed asset (Binance-Peg, NEAR Intents, Omni Bridge, Unit,
/// cbZEC, zenZEC…) with its supply read from its own chain, DEX liquidity and volume, and
/// totals per chain.
#[utoipa::path(get, path = "/v1/ecosystem/assets", responses((status = 200, body = Ecosystem)), tag = "market")]
pub async fn assets(State(s): State<AppState>) -> ApiResult<Ecosystem> {
    let rows: Vec<LatestRow> = sqlx::query_as(
        "SELECT asset, supply, locked, price_usd, liquidity_usd, volume_24h_usd, pools, updated_at,
                holders, holders_top10
         FROM zec_assets_latest",
    )
    .fetch_all(&s.db.pool)
    .await?;
    let zec_supply: Option<i64> =
        sqlx::query_scalar("SELECT supply FROM pool_snapshots ORDER BY height DESC LIMIT 1")
            .fetch_optional(&s.db.pool)
            .await?;

    let mut assets: Vec<ZecAsset> = market::assets()
        .iter()
        .map(|a| {
            let r = rows.iter().find(|r| r.0 == a.id);
            let supply = r.and_then(|r| r.1);
            let locked = r.and_then(|r| r.2);
            ZecAsset {
                id: a.id.clone(),
                name: a.name.clone(),
                symbol: a.symbol.clone(),
                chain: a.chain.clone(),
                chain_name: a.chain_name.clone(),
                address: a.address.clone(),
                issuer: a.issuer.clone(),
                model: a.model.clone(),
                backed_by: a.backed_by.clone(),
                website: a.website.clone(),
                explorer_url: explorer_url(&a.chain, &a.address),
                holders: r.and_then(|r| r.8),
                holders_top10: r.and_then(|r| r.9),
                supply,
                locked,
                circulating: supply.map(|x| (x - locked.unwrap_or(0.0)).max(0.0)),
                price_usd: r.and_then(|r| r.3),
                liquidity_usd: r.and_then(|r| r.4),
                volume_24h_usd: r.and_then(|r| r.5),
                pools: r
                    .and_then(|r| r.6.clone())
                    .unwrap_or(serde_json::Value::Array(vec![])),
                updated_at: r.map(|r| r.7),
            }
        })
        .collect();
    assets.sort_by(|a, b| b.supply.unwrap_or(0.0).total_cmp(&a.supply.unwrap_or(0.0)));

    let zec_outside: f64 = assets
        .iter()
        .filter(|a| a.backed_by == "zcash")
        .filter_map(|a| a.supply)
        .sum();
    let mut chains: Vec<ChainTotal> = Vec::new();
    for a in &assets {
        let c = match chains.iter_mut().find(|c| c.chain == a.chain) {
            Some(c) => c,
            None => {
                chains.push(ChainTotal {
                    chain: a.chain.clone(),
                    chain_name: a.chain_name.clone(),
                    zec: 0.0,
                    liquidity_usd: 0.0,
                    volume_24h_usd: 0.0,
                    assets: 0,
                });
                chains.last_mut().unwrap()
            }
        };
        c.zec += a.circulating.unwrap_or(0.0);
        c.liquidity_usd += a.liquidity_usd.unwrap_or(0.0);
        c.volume_24h_usd += a.volume_24h_usd.unwrap_or(0.0);
        c.assets += 1;
    }
    chains.sort_by(|a, b| b.zec.total_cmp(&a.zec));
    Ok(Json(Ecosystem {
        zec_outside,
        share_of_supply: zec_supply
            .filter(|s| *s > 0)
            .map(|s| zec_outside / (s as f64 / 1e8)),
        liquidity_usd: assets.iter().filter_map(|a| a.liquidity_usd).sum(),
        volume_24h_usd: assets.iter().filter_map(|a| a.volume_24h_usd).sum(),
        chains,
        assets,
    }))
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct AssetDay {
    pub asset: String,
    pub day: NaiveDate,
    pub supply: Option<f64>,
    pub liquidity_usd: Option<f64>,
    pub volume_usd: Option<f64>,
}

/// Daily supply, liquidity and volume of each ZEC asset on other chains (collected since
/// ZScan started tracking it).
#[utoipa::path(get, path = "/v1/ecosystem/assets/daily", params(DaysQuery),
    responses((status = 200, body = [AssetDay])), tag = "market")]
pub async fn assets_daily(
    State(s): State<AppState>,
    Query(q): Query<DaysQuery>,
) -> ApiResult<Vec<AssetDay>> {
    Ok(Json(
        sqlx::query_as::<_, AssetDay>(
            "SELECT asset, day, supply, liquidity_usd, volume_usd FROM zec_assets_daily
             WHERE day > (now() AT TIME ZONE 'UTC')::date - $1 ORDER BY day, asset",
        )
        .bind(q.days.unwrap_or(365).clamp(1, 3650))
        .fetch_all(&s.db.pool)
        .await?,
    ))
}
