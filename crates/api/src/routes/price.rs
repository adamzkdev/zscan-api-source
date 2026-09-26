//! ZEC market price from CoinGecko, cached so the public API never fans out upstream.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
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

async fn fetch(http: &reqwest::Client) -> anyhow::Result<Price> {
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
            .map(|c| c.prices)
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
        history: history.into_iter().map(|(t, p)| (t as i64, p)).collect(),
    })
}

/// ZEC market price (CoinGecko, cached 60s).
#[utoipa::path(get, path = "/v1/price", responses((status = 200, body = Price)), tag = "network")]
pub async fn price(State(s): State<AppState>) -> ApiResult<Price> {
    let mut cache = s.price.lock().await;
    if let Some((at, p)) = &cache.value {
        if at.elapsed() < TTL {
            return Ok(Json(p.clone()));
        }
    }
    match fetch(&s.http).await {
        Ok(p) => {
            cache.value = Some((Instant::now(), p.clone()));
            Ok(Json(p))
        }
        // Serve stale data rather than nothing if CoinGecko is down.
        Err(e) => match &cache.value {
            Some((_, p)) => Ok(Json(p.clone())),
            None => Err(ApiError::Internal(e)),
        },
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct PriceHistory {
    /// Daily USD closes for the last year: `[unix_ms, price]`.
    pub usd: Vec<(i64, f64)>,
}

/// Daily ZEC/USD closes for the last 365 days (CoinGecko, cached 1h). Used to value
/// transactions at the time they happened.
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
    let fetched: anyhow::Result<Chart> = async {
        Ok(s.http
            .get(YEAR)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
    .await;
    match fetched {
        Ok(c) => {
            let h = PriceHistory {
                usd: c.prices.into_iter().map(|(t, p)| (t as i64, p)).collect(),
            };
            *cache = Some((Instant::now(), h.clone()));
            Ok(Json(h))
        }
        Err(e) => match cache.as_ref() {
            Some((_, h)) => Ok(Json(h.clone())),
            None => Err(ApiError::Internal(e)),
        },
    }
}
