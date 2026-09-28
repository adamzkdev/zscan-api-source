//! ZEC market price, cached so the public API never fans out upstream.
//!
//! CoinGecko has everything in one call but throttles cloud addresses, so each figure has
//! fallbacks: the spot price and 24h change come from CoinPaprika, Kraken, Coinbase, OKX or
//! Binance; other currencies are derived from ECB rates (Frankfurter) and Kraken's BTC/ETH
//! prices; the market cap is the price times the supply ZScan computes itself. A background
//! task refreshes the cache every minute, and the last good value is served if every source
//! fails.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;
use utoipa::ToSchema;

use crate::error::{ApiError, ApiResult};
use crate::AppState;

const TTL: Duration = Duration::from_secs(60);
/// Currencies amounts can be shown in, besides ZEC.
pub const CURRENCIES: &str = "usd,eur,gbp,chf,jpy,cad,aud,btc,eth";
const SIMPLE: &str = "https://api.coingecko.com/api/v3/simple/price?ids=zcash&vs_currencies=usd,eur,gbp,chf,jpy,cad,aud,btc,eth&include_24hr_change=true&include_market_cap=true&include_24hr_vol=true";
const YEAR: &str = "https://api.coingecko.com/api/v3/coins/zcash/market_chart?vs_currency=usd&days=365&interval=daily";
const CHART: &str = "https://api.coingecko.com/api/v3/coins/zcash/market_chart?vs_currency=usd&days=14&interval=daily";
const KRAKEN_DAILY: &str = "https://api.kraken.com/0/public/OHLC?pair=ZECUSD&interval=1440";

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Price {
    pub usd: f64,
    pub btc: f64,
    pub usd_24h_change: f64,
    pub market_cap_usd: f64,
    pub volume_24h_usd: f64,
    /// Daily USD closes for the last 14 days: `[unix_ms, price]`.
    pub history: Vec<(i64, f64)>,
    /// Price of 1 ZEC in each supported currency (usd, eur, gbp, chf, jpy, cad, aud, btc, eth).
    pub rates: BTreeMap<String, f64>,
    /// Where the spot price came from (coingecko, coinpaprika, kraken, coinbase, okx, binance).
    pub source: String,
}

#[derive(Default)]
pub struct PriceCache {
    value: Option<(Instant, Price)>,
}

pub type SharedPriceCache = Arc<Mutex<PriceCache>>;

#[derive(Deserialize)]
struct Simple {
    zcash: SimpleZec,
}

#[derive(Deserialize)]
struct SimpleZec {
    usd: f64,
    btc: f64,
    usd_24h_change: f64,
    usd_market_cap: f64,
    usd_24h_vol: f64,
    #[serde(flatten)]
    other: BTreeMap<String, serde_json::Value>,
}

#[derive(Deserialize)]
struct Chart {
    prices: Vec<(f64, f64)>,
}

async fn get_json(http: &reqwest::Client, url: &str) -> anyhow::Result<Value> {
    Ok(http
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

/// A JSON number, or a number written as a string (as exchanges do).
fn f(v: &Value) -> Option<f64> {
    match v {
        Value::String(s) => s.parse().ok(),
        n => n.as_f64(),
    }
    .filter(|x: &f64| x.is_finite())
}

async fn coingecko(http: &reqwest::Client) -> anyhow::Result<Price> {
    let s: Simple = http
        .get(SIMPLE)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    // The chart is decorative; a failure there shouldn't hide the price.
    let history = match http.get(CHART).send().await {
        Ok(r) => r
            .json::<Chart>()
            .await
            .map(|c| c.prices.into_iter().map(|(t, p)| (t as i64, p)).collect())
            .unwrap_or_default(),
        Err(_) => vec![],
    };
    let mut rates: BTreeMap<String, f64> = CURRENCIES
        .split(',')
        .filter_map(|c| Some((c.to_string(), s.zcash.other.get(c)?.as_f64()?)))
        .collect();
    rates.insert("usd".into(), s.zcash.usd);
    rates.insert("btc".into(), s.zcash.btc);
    Ok(Price {
        rates,
        usd: s.zcash.usd,
        btc: s.zcash.btc,
        usd_24h_change: s.zcash.usd_24h_change,
        market_cap_usd: s.zcash.usd_market_cap,
        volume_24h_usd: s.zcash.usd_24h_vol,
        history,
        source: "coingecko".into(),
    })
}

/// Spot figures from one exchange or aggregator.
struct Spot {
    source: &'static str,
    usd: f64,
    /// Percent.
    change_24h: Option<f64>,
    volume_24h_usd: Option<f64>,
    btc: Option<f64>,
    eth: Option<f64>,
}

async fn coinpaprika(http: &reqwest::Client) -> anyhow::Result<Spot> {
    let v = get_json(
        http,
        "https://api.coinpaprika.com/v1/tickers/zec-zcash?quotes=USD,BTC,ETH",
    )
    .await?;
    let q = &v["quotes"];
    Ok(Spot {
        source: "coinpaprika",
        usd: f(&q["USD"]["price"]).context("no USD price")?,
        change_24h: f(&q["USD"]["percent_change_24h"]),
        volume_24h_usd: f(&q["USD"]["volume_24h"]),
        btc: f(&q["BTC"]["price"]),
        eth: f(&q["ETH"]["price"]),
    })
}

/// Kraken ticker `pair` (e.g. ZECUSD): last price, 24h change from today's open, 24h volume in USD.
async fn kraken_ticker(http: &reqwest::Client, pair: &str) -> anyhow::Result<(f64, f64, f64)> {
    let v = get_json(
        http,
        &format!("https://api.kraken.com/0/public/Ticker?pair={pair}"),
    )
    .await?;
    let t = v["result"]
        .as_object()
        .and_then(|o| o.values().next())
        .context("no ticker")?;
    let last = f(&t["c"][0]).context("no last price")?;
    let open = f(&t["o"]).unwrap_or(last);
    let vwap = f(&t["p"][1]).unwrap_or(last);
    let volume = f(&t["v"][1]).unwrap_or(0.0) * vwap;
    Ok((last, (last / open - 1.0) * 100.0, volume))
}

async fn kraken(http: &reqwest::Client) -> anyhow::Result<Spot> {
    let (usd, change, volume) = kraken_ticker(http, "ZECUSD").await?;
    Ok(Spot {
        source: "kraken",
        usd,
        change_24h: Some(change),
        volume_24h_usd: Some(volume),
        btc: None,
        eth: None,
    })
}

async fn coinbase(http: &reqwest::Client) -> anyhow::Result<Spot> {
    let v = get_json(
        http,
        "https://api.exchange.coinbase.com/products/ZEC-USD/stats",
    )
    .await?;
    let last = f(&v["last"]).context("no last price")?;
    let open = f(&v["open"]).unwrap_or(last);
    Ok(Spot {
        source: "coinbase",
        usd: last,
        change_24h: Some((last / open - 1.0) * 100.0),
        volume_24h_usd: f(&v["volume"]).map(|z| z * last),
        btc: None,
        eth: None,
    })
}

async fn okx(http: &reqwest::Client) -> anyhow::Result<Spot> {
    let v = get_json(
        http,
        "https://www.okx.com/api/v5/market/ticker?instId=ZEC-USDT",
    )
    .await?;
    let t = &v["data"][0];
    let last = f(&t["last"]).context("no last price")?;
    let open = f(&t["open24h"]).unwrap_or(last);
    Ok(Spot {
        source: "okx",
        usd: last,
        change_24h: Some((last / open - 1.0) * 100.0),
        volume_24h_usd: f(&t["volCcy24h"]),
        btc: None,
        eth: None,
    })
}

async fn binance(http: &reqwest::Client) -> anyhow::Result<Spot> {
    let v = get_json(
        http,
        "https://api.binance.com/api/v3/ticker/24hr?symbol=ZECUSDT",
    )
    .await?;
    Ok(Spot {
        source: "binance",
        usd: f(&v["lastPrice"]).context("no last price")?,
        change_24h: f(&v["priceChangePercent"]),
        volume_24h_usd: f(&v["quoteVolume"]),
        btc: None,
        eth: None,
    })
}

/// The first source that answers, in order of how much it tells.
async fn spot(http: &reqwest::Client) -> anyhow::Result<Spot> {
    let mut errors = Vec::new();
    for (name, r) in [
        ("coinpaprika", coinpaprika(http).await),
        ("kraken", kraken(http).await),
        ("coinbase", coinbase(http).await),
        ("okx", okx(http).await),
        ("binance", binance(http).await),
    ] {
        match r {
            Ok(s) if s.usd > 0.0 => return Ok(s),
            Ok(_) => errors.push(format!("{name}: zero price")),
            Err(e) => errors.push(format!("{name}: {e:#}")),
        }
    }
    bail!("no price source answered ({})", errors.join("; "))
}

/// Units of each fiat currency per US dollar (ECB reference rates).
async fn fx(http: &reqwest::Client) -> anyhow::Result<BTreeMap<String, f64>> {
    let v = get_json(
        http,
        "https://api.frankfurter.dev/v1/latest?base=USD&symbols=EUR,GBP,CHF,JPY,CAD,AUD",
    )
    .await?;
    Ok(v["rates"]
        .as_object()
        .context("no rates")?
        .iter()
        .filter_map(|(k, r)| Some((k.to_lowercase(), f(r)?)))
        .collect())
}

/// Daily USD closes from Kraken, newest last: `[unix_ms, close]`.
async fn kraken_daily(http: &reqwest::Client, days: usize) -> anyhow::Result<Vec<(i64, f64)>> {
    let v = get_json(http, KRAKEN_DAILY).await?;
    let rows = v["result"]
        .as_object()
        .and_then(|o| o.iter().find(|(k, _)| *k != "last"))
        .and_then(|(_, r)| r.as_array())
        .context("no candles")?;
    let out: Vec<(i64, f64)> = rows
        .iter()
        .filter_map(|c| Some((c[0].as_i64()? * 1000, f(&c[4])?)))
        .collect();
    Ok(out[out.len().saturating_sub(days)..].to_vec())
}

/// ZEC in circulation, from ZScan's own supply computation.
async fn supply_zec(s: &AppState) -> Option<f64> {
    let zat: Option<i64> =
        sqlx::query_scalar("SELECT supply FROM pool_snapshots ORDER BY height DESC LIMIT 1")
            .fetch_optional(&s.db.pool)
            .await
            .ok()
            .flatten();
    zat.map(|z| z as f64 / 1e8)
}

/// Builds the price from the fallback sources when CoinGecko can't be reached.
async fn assembled(s: &AppState) -> anyhow::Result<Price> {
    let http = &s.http;
    let spot = spot(http).await?;
    let usd = spot.usd;
    let mut rates = BTreeMap::from([("usd".to_string(), usd)]);
    match fx(http).await {
        Ok(per_usd) => rates.extend(per_usd.into_iter().map(|(c, r)| (c, usd * r))),
        Err(e) => tracing::warn!("price: fx rates unavailable: {e:#}"),
    }
    let btc = match spot.btc {
        Some(b) => Some(b),
        None => kraken_ticker(http, "XBTUSD")
            .await
            .ok()
            .map(|(b, ..)| usd / b),
    };
    let eth = match spot.eth {
        Some(e) => Some(e),
        None => kraken_ticker(http, "ETHUSD")
            .await
            .ok()
            .map(|(e, ..)| usd / e),
    };
    if let Some(b) = btc {
        rates.insert("btc".into(), b);
    }
    if let Some(e) = eth {
        rates.insert("eth".into(), e);
    }
    Ok(Price {
        usd,
        btc: btc.unwrap_or(0.0),
        usd_24h_change: spot.change_24h.unwrap_or(0.0),
        market_cap_usd: supply_zec(s).await.map(|z| z * usd).unwrap_or(0.0),
        volume_24h_usd: spot.volume_24h_usd.unwrap_or(0.0),
        history: kraken_daily(http, 14).await.unwrap_or_default(),
        rates,
        source: spot.source.into(),
    })
}

async fn fetch(s: &AppState) -> anyhow::Result<Price> {
    // PRICE_SKIP_COINGECKO=1 goes straight to the fallbacks (tests, or CoinGecko misbehaving).
    if std::env::var("PRICE_SKIP_COINGECKO").is_ok_and(|v| v == "1") {
        return assembled(s).await;
    }
    match coingecko(&s.http).await {
        Ok(p) => Ok(p),
        Err(e) => {
            tracing::debug!("price: coingecko unavailable ({e:#}), using fallbacks");
            assembled(s).await
        }
    }
}

/// Keeps the price cache warm (and filled right after a restart).
pub async fn refresh(s: AppState) {
    loop {
        match fetch(&s).await {
            Ok(p) => s.price.lock().await.value = Some((Instant::now(), p)),
            Err(e) => tracing::warn!("price refresh: {e:#}"),
        }
        tokio::time::sleep(TTL).await;
    }
}

/// ZEC market price (cached 60 s; CoinGecko with exchange fallbacks).
#[utoipa::path(get, path = "/v1/price", responses((status = 200, body = Price)), tag = "network")]
pub async fn price(State(s): State<AppState>) -> ApiResult<Price> {
    if let Some((_, p)) = &s.price.lock().await.value {
        return Ok(Json(p.clone()));
    }
    // Only before the first refresh has finished.
    let p = fetch(&s).await.map_err(ApiError::Internal)?;
    s.price.lock().await.value = Some((Instant::now(), p.clone()));
    Ok(Json(p))
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct PriceHistory {
    /// Daily USD closes for the last year: `[unix_ms, price]`.
    pub usd: Vec<(i64, f64)>,
}

/// Daily ZEC/USD closes for the last 365 days (CoinGecko, else Kraken; cached 1h). Used to
/// value transactions at the time they happened.
#[utoipa::path(get, path = "/v1/price/history", responses((status = 200, body = PriceHistory)), tag = "network")]
pub async fn history(State(s): State<AppState>) -> ApiResult<PriceHistory> {
    type Cached = Option<(Instant, PriceHistory)>;
    static CACHE: OnceLock<Mutex<Cached>> = OnceLock::new();
    let mut cache = CACHE.get_or_init(|| Mutex::new(None)).lock().await;
    if let Some((at, h)) = cache.as_ref() {
        if at.elapsed() < Duration::from_secs(3600) {
            return Ok(Json(h.clone()));
        }
    }
    let fetched: anyhow::Result<Vec<(i64, f64)>> = async {
        match s
            .http
            .get(YEAR)
            .send()
            .await
            .and_then(|r| r.error_for_status())
        {
            Ok(r) => Ok(r
                .json::<Chart>()
                .await?
                .prices
                .into_iter()
                .map(|(t, p)| (t as i64, p))
                .collect()),
            Err(e) => {
                tracing::debug!("price history: coingecko unavailable ({e:#}), using kraken");
                kraken_daily(&s.http, 365).await
            }
        }
    }
    .await;
    match fetched {
        Ok(usd) => {
            let h = PriceHistory { usd };
            *cache = Some((Instant::now(), h.clone()));
            Ok(Json(h))
        }
        Err(e) => match cache.as_ref() {
            Some((_, h)) => Ok(Json(h.clone())),
            None => Err(ApiError::Internal(e)),
        },
    }
}
