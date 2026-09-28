//! ZEC markets and ZEC on other chains, collected in the background from public APIs.
//!
//! - Venues: spot order books on the main exchanges and perpetual futures (open interest,
//!   funding), every [`SNAPSHOT_EVERY`] into `market_venues`, with their daily candles, open
//!   interest and funding history in `market_daily` (a year of backfill at start).
//! - Assets: every ZEC representation listed in `labels/zec_assets.json`, with its supply read from
//!   its own chain and its DEX liquidity and volume from GeckoTerminal.
//!
//! A venue that fails (rate limit, region block) is skipped until the next round; its last
//! figures stay in place with their `updated_at`.

use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::PgPool;

const SNAPSHOT_EVERY: Duration = Duration::from_secs(300);
/// Candles and history are re-read for the last few days every round, fully once at start.
const RECENT_DAYS: i64 = 3;
const BACKFILL_DAYS: i64 = 365;

/// A JSON number, or a number written as a string (as exchanges do).
fn f(v: &Value) -> Option<f64> {
    match v {
        Value::String(s) => s.parse().ok(),
        n => n.as_f64(),
    }
    .filter(|x: &f64| x.is_finite())
}

fn day_ms(ms: f64) -> NaiveDate {
    DateTime::<Utc>::from_timestamp_millis(ms as i64)
        .unwrap_or_default()
        .date_naive()
}

fn day_s(s: f64) -> NaiveDate {
    day_ms(s * 1000.0)
}

fn http() -> &'static reqwest::Client {
    static C: OnceLock<reqwest::Client> = OnceLock::new();
    C.get_or_init(|| {
        zscan_chain::http_client_builder()
            .timeout(Duration::from_secs(15))
            .user_agent("Mozilla/5.0 (compatible; ZScan/1.0; +https://zscan.cash)")
            .build()
            .expect("http client")
    })
}

async fn get(url: &str) -> Result<Value> {
    let r = http().get(url).send().await?;
    let status = r.status();
    if !status.is_success() {
        bail!("{url}: HTTP {status}");
    }
    Ok(r.json().await?)
}

async fn post(url: &str, body: Value) -> Result<Value> {
    let r = http().post(url).json(&body).send().await?;
    let status = r.status();
    if !status.is_success() {
        bail!("{url}: HTTP {status}");
    }
    Ok(r.json().await?)
}

const HL: &str = "https://api.hyperliquid.xyz/info";

// ---------------------------------------------------------------------------------------------
// Venues
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Venue {
    venue: &'static str,
    market: &'static str,
    pair: String,
    kind: &'static str,
    price: Option<f64>,
    volume_24h_usd: Option<f64>,
    bid: Option<f64>,
    ask: Option<f64>,
    oi_usd: Option<f64>,
    oi_zec: Option<f64>,
    funding_8h: Option<f64>,
}

fn spot(venue: &'static str, pair: &str) -> Venue {
    Venue {
        venue,
        market: "spot",
        pair: pair.into(),
        kind: "cex",
        ..Default::default()
    }
}

fn perp(venue: &'static str, pair: &str) -> Venue {
    Venue {
        venue,
        market: "perp",
        pair: pair.into(),
        kind: "cex",
        ..Default::default()
    }
}

/// One day of a venue's history. Missing fields are left as they are in the table.
#[derive(Debug, Default, Clone)]
struct Day {
    day: NaiveDate,
    close: Option<f64>,
    volume_usd: Option<f64>,
    oi_usd: Option<f64>,
    funding_8h: Option<f64>,
}

fn candle(day: NaiveDate, close: Option<f64>, volume_usd: Option<f64>) -> Day {
    Day {
        day,
        close,
        volume_usd,
        ..Default::default()
    }
}

/// Mean funding per day from `(time_ms, rate)` samples, scaled to an 8-hour rate.
fn daily_funding(samples: &[(f64, f64)], to_8h: f64) -> Vec<Day> {
    let mut by_day: std::collections::BTreeMap<NaiveDate, (f64, u32)> = Default::default();
    for (t, r) in samples {
        let e = by_day.entry(day_ms(*t)).or_default();
        e.0 += r;
        e.1 += 1;
    }
    by_day
        .into_iter()
        .map(|(day, (sum, n))| Day {
            day,
            funding_8h: Some(sum / n as f64 * to_8h),
            ..Default::default()
        })
        .collect()
}

/// Every venue ZScan tracks: `(venue, market)` → snapshot and history readers.
const VENUES: &[(&str, &str)] = &[
    ("binance", "spot"),
    ("coinbase", "spot"),
    ("okx", "spot"),
    ("kraken", "spot"),
    ("kucoin", "spot"),
    ("gate", "spot"),
    ("bitget", "spot"),
    ("mexc", "spot"),
    ("htx", "spot"),
    ("hyperliquid", "spot"),
    ("binance", "perp"),
    ("okx", "perp"),
    ("bybit", "perp"),
    ("hyperliquid", "perp"),
];

/// Display names for the API.
pub fn venue_name(v: &str) -> &str {
    match v {
        "binance" => "Binance",
        "coinbase" => "Coinbase",
        "okx" => "OKX",
        "kraken" => "Kraken",
        "kucoin" => "KuCoin",
        "gate" => "Gate",
        "bitget" => "Bitget",
        "mexc" => "MEXC",
        "htx" => "HTX",
        "hyperliquid" => "Hyperliquid",
        "bybit" => "Bybit",
        other => other,
    }
}

/// Hyperliquid's spot pair name for UZEC/USDC (an index like "@272"), looked up once.
async fn hl_spot_pair() -> Result<String> {
    static PAIR: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
    PAIR.get_or_try_init(|| async {
        let m = post(HL, json!({ "type": "spotMeta" })).await?;
        let tokens = m["tokens"].as_array().context("tokens")?;
        let uzec = tokens
            .iter()
            .find(|t| t["name"] == "UZEC")
            .and_then(|t| t["index"].as_u64())
            .context("UZEC token")?;
        m["universe"]
            .as_array()
            .context("universe")?
            .iter()
            .find(|u| {
                u["tokens"]
                    .as_array()
                    .is_some_and(|t| t[0].as_u64() == Some(uzec))
            })
            .and_then(|u| u["name"].as_str().map(str::to_string))
            .context("UZEC pair")
    })
    .await
    .cloned()
}

async fn snapshot(venue: &str, market: &str) -> Result<Venue> {
    Ok(match (venue, market) {
        ("binance", "spot") => {
            let t =
                get("https://data-api.binance.vision/api/v3/ticker/24hr?symbol=ZECUSDT").await?;
            Venue {
                price: f(&t["lastPrice"]),
                volume_24h_usd: f(&t["quoteVolume"]),
                bid: f(&t["bidPrice"]),
                ask: f(&t["askPrice"]),
                ..spot("binance", "ZEC/USDT")
            }
        }
        ("coinbase", "spot") => {
            let (s, t) = tokio::try_join!(
                get("https://api.exchange.coinbase.com/products/ZEC-USD/stats"),
                get("https://api.exchange.coinbase.com/products/ZEC-USD/ticker"),
            )?;
            let price = f(&s["last"]);
            Venue {
                price,
                volume_24h_usd: f(&s["volume"]).zip(price).map(|(v, p)| v * p),
                bid: f(&t["bid"]),
                ask: f(&t["ask"]),
                ..spot("coinbase", "ZEC/USD")
            }
        }
        ("okx", "spot") => {
            let t = get("https://www.okx.com/api/v5/market/ticker?instId=ZEC-USDT").await?;
            let t = &t["data"][0];
            Venue {
                price: f(&t["last"]),
                volume_24h_usd: f(&t["volCcy24h"]),
                bid: f(&t["bidPx"]),
                ask: f(&t["askPx"]),
                ..spot("okx", "ZEC/USDT")
            }
        }
        ("kraken", "spot") => {
            let t = get("https://api.kraken.com/0/public/Ticker?pair=ZECUSD").await?;
            let t = t["result"]
                .as_object()
                .and_then(|o| o.values().next())
                .context("kraken ticker")?;
            let price = f(&t["c"][0]);
            Venue {
                price,
                // 24h volume in ZEC times the 24h VWAP.
                volume_24h_usd: f(&t["v"][1]).zip(f(&t["p"][1])).map(|(v, p)| v * p),
                bid: f(&t["b"][0]),
                ask: f(&t["a"][0]),
                ..spot("kraken", "ZEC/USD")
            }
        }
        ("kucoin", "spot") => {
            let (s, b) = tokio::try_join!(
                get("https://api.kucoin.com/api/v1/market/stats?symbol=ZEC-USDT"),
                get("https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=ZEC-USDT"),
            )?;
            Venue {
                price: f(&s["data"]["last"]),
                volume_24h_usd: f(&s["data"]["volValue"]),
                bid: f(&b["data"]["bestBid"]),
                ask: f(&b["data"]["bestAsk"]),
                ..spot("kucoin", "ZEC/USDT")
            }
        }
        ("gate", "spot") => {
            let t = get("https://api.gateio.ws/api/v4/spot/tickers?currency_pair=ZEC_USDT").await?;
            let t = &t[0];
            Venue {
                price: f(&t["last"]),
                volume_24h_usd: f(&t["quote_volume"]),
                bid: f(&t["highest_bid"]),
                ask: f(&t["lowest_ask"]),
                ..spot("gate", "ZEC/USDT")
            }
        }
        ("bitget", "spot") => {
            let t = get("https://api.bitget.com/api/v2/spot/market/tickers?symbol=ZECUSDT").await?;
            let t = &t["data"][0];
            Venue {
                price: f(&t["lastPr"]),
                volume_24h_usd: f(&t["usdtVolume"]),
                bid: f(&t["bidPr"]),
                ask: f(&t["askPr"]),
                ..spot("bitget", "ZEC/USDT")
            }
        }
        ("mexc", "spot") => {
            let (t, b) = tokio::try_join!(
                get("https://api.mexc.com/api/v3/ticker/24hr?symbol=ZECUSDT"),
                get("https://api.mexc.com/api/v3/ticker/bookTicker?symbol=ZECUSDT"),
            )?;
            Venue {
                price: f(&t["lastPrice"]),
                volume_24h_usd: f(&t["quoteVolume"]),
                bid: f(&b["bidPrice"]),
                ask: f(&b["askPrice"]),
                ..spot("mexc", "ZEC/USDT")
            }
        }
        ("htx", "spot") => {
            let t = get("https://api.huobi.pro/market/detail/merged?symbol=zecusdt").await?;
            let t = &t["tick"];
            Venue {
                price: f(&t["close"]),
                volume_24h_usd: f(&t["vol"]),
                bid: f(&t["bid"][0]),
                ask: f(&t["ask"][0]),
                ..spot("htx", "ZEC/USDT")
            }
        }
        ("hyperliquid", "spot") => {
            let pair = hl_spot_pair().await?;
            let m = post(HL, json!({ "type": "spotMetaAndAssetCtxs" })).await?;
            let idx = m[0]["universe"]
                .as_array()
                .and_then(|u| u.iter().find(|x| x["name"] == pair.as_str()))
                .and_then(|x| x["index"].as_u64())
                .context("hyperliquid spot pair")? as usize;
            let c = &m[1][idx];
            Venue {
                price: f(&c["markPx"]),
                volume_24h_usd: f(&c["dayNtlVlm"]),
                kind: "dex",
                ..spot("hyperliquid", "UZEC/USDC")
            }
        }
        ("binance", "perp") => {
            let (p, oi, t) = tokio::try_join!(
                get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ZECUSDT"),
                get("https://fapi.binance.com/fapi/v1/openInterest?symbol=ZECUSDT"),
                get("https://fapi.binance.com/fapi/v1/ticker/24hr?symbol=ZECUSDT"),
            )?;
            let price = f(&p["markPrice"]);
            let oi_zec = f(&oi["openInterest"]);
            Venue {
                price,
                volume_24h_usd: f(&t["quoteVolume"]),
                oi_zec,
                oi_usd: oi_zec.zip(price).map(|(a, b)| a * b),
                funding_8h: f(&p["lastFundingRate"]),
                ..perp("binance", "ZECUSDT")
            }
        }
        ("okx", "perp") => {
            let (oi, fr, t) = tokio::try_join!(
                get("https://www.okx.com/api/v5/public/open-interest?instId=ZEC-USDT-SWAP"),
                get("https://www.okx.com/api/v5/public/funding-rate?instId=ZEC-USDT-SWAP"),
                get("https://www.okx.com/api/v5/market/ticker?instId=ZEC-USDT-SWAP"),
            )?;
            let price = f(&t["data"][0]["last"]);
            Venue {
                price,
                // volCcy24h is in ZEC for swaps.
                volume_24h_usd: f(&t["data"][0]["volCcy24h"]).zip(price).map(|(v, p)| v * p),
                oi_zec: f(&oi["data"][0]["oiCcy"]),
                oi_usd: f(&oi["data"][0]["oiUsd"]),
                funding_8h: f(&fr["data"][0]["fundingRate"]),
                ..perp("okx", "ZEC-USDT-SWAP")
            }
        }
        ("bybit", "perp") => {
            let t = get("https://api.bybit.com/v5/market/tickers?category=linear&symbol=ZECUSDT")
                .await?;
            let t = &t["result"]["list"][0];
            Venue {
                price: f(&t["lastPrice"]),
                volume_24h_usd: f(&t["turnover24h"]),
                bid: f(&t["bid1Price"]),
                ask: f(&t["ask1Price"]),
                oi_zec: f(&t["openInterest"]),
                oi_usd: f(&t["openInterestValue"]),
                funding_8h: f(&t["fundingRate"]),
                ..perp("bybit", "ZECUSDT")
            }
        }
        ("hyperliquid", "perp") => {
            let m = post(HL, json!({ "type": "metaAndAssetCtxs" })).await?;
            let idx = m[0]["universe"]
                .as_array()
                .and_then(|u| u.iter().position(|x| x["name"] == "ZEC"))
                .context("hyperliquid ZEC perp")?;
            let c = &m[1][idx];
            let price = f(&c["markPx"]);
            let oi_zec = f(&c["openInterest"]);
            Venue {
                price,
                volume_24h_usd: f(&c["dayNtlVlm"]),
                oi_zec,
                oi_usd: oi_zec.zip(price).map(|(a, b)| a * b),
                // Hourly funding.
                funding_8h: f(&c["funding"]).map(|r| r * 8.0),
                kind: "dex",
                ..perp("hyperliquid", "ZEC-PERP")
            }
        }
        _ => bail!("unknown venue {venue} {market}"),
    })
}

/// Daily candles, open interest and funding for the last `days` days.
async fn history(venue: &str, market: &str, days: i64) -> Result<Vec<Day>> {
    let since_ms = (Utc::now() - chrono::Duration::days(days)).timestamp_millis();
    let n = days.clamp(2, 1000);
    let arr = |v: &Value| v.as_array().cloned().unwrap_or_default();
    let mut out = Vec::new();
    match (venue, market) {
        ("binance", "spot") | ("mexc", "spot") => {
            let base = if venue == "binance" {
                "https://data-api.binance.vision"
            } else {
                "https://api.mexc.com"
            };
            let k = get(&format!(
                "{base}/api/v3/klines?symbol=ZECUSDT&interval=1d&limit={n}"
            ))
            .await?;
            out = arr(&k)
                .iter()
                .map(|c| candle(day_ms(f(&c[0]).unwrap_or(0.0)), f(&c[4]), f(&c[7])))
                .collect();
        }
        ("coinbase", "spot") => {
            let k =
                get("https://api.exchange.coinbase.com/products/ZEC-USD/candles?granularity=86400")
                    .await?;
            out = arr(&k)
                .iter()
                .map(|c| {
                    let close = f(&c[4]);
                    candle(
                        day_s(f(&c[0]).unwrap_or(0.0)),
                        close,
                        f(&c[5]).zip(close).map(|(v, p)| v * p),
                    )
                })
                .collect();
        }
        ("okx", _) => {
            let inst = if market == "spot" {
                "ZEC-USDT"
            } else {
                "ZEC-USDT-SWAP"
            };
            // 100 candles per page, newest first; page back with `after`.
            let mut after = String::new();
            for _ in 0..(n / 100 + 1) {
                let k = get(&format!(
                    "https://www.okx.com/api/v5/market/history-candles?instId={inst}&bar=1Dutc&limit=100{after}"
                ))
                .await?;
                let page = arr(&k["data"]);
                let Some(last) = page.last() else { break };
                after = format!("&after={}", last[0].as_str().unwrap_or_default());
                out.extend(
                    page.iter()
                        .map(|c| candle(day_ms(f(&c[0]).unwrap_or(0.0)), f(&c[4]), f(&c[7]))),
                );
            }
            if market == "perp" {
                let oi = get("https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-history?instId=ZEC-USDT-SWAP&period=1D&limit=100").await?;
                out.extend(arr(&oi["data"]).iter().map(|r| Day {
                    day: day_ms(f(&r[0]).unwrap_or(0.0)),
                    oi_usd: f(&r[3]),
                    ..Default::default()
                }));
                let fr = get("https://www.okx.com/api/v5/public/funding-rate-history?instId=ZEC-USDT-SWAP&limit=100").await?;
                let s: Vec<(f64, f64)> = arr(&fr["data"])
                    .iter()
                    .filter_map(|r| {
                        Some((
                            f(&r["fundingTime"])?,
                            f(&r["realizedRate"]).or(f(&r["fundingRate"]))?,
                        ))
                    })
                    .collect();
                out.extend(daily_funding(&s, 1.0));
            }
        }
        ("kraken", "spot") => {
            let k = get("https://api.kraken.com/0/public/OHLC?pair=ZECUSD&interval=1440").await?;
            let rows = k["result"]
                .as_object()
                .and_then(|o| o.iter().find(|(k, _)| *k != "last").map(|(_, v)| arr(v)))
                .unwrap_or_default();
            out = rows
                .iter()
                .map(|c| {
                    candle(
                        day_s(f(&c[0]).unwrap_or(0.0)),
                        f(&c[4]),
                        f(&c[6]).zip(f(&c[5])).map(|(v, p)| v * p),
                    )
                })
                .collect();
        }
        ("kucoin", "spot") => {
            let start = since_ms / 1000;
            let k = get(&format!(
                "https://api.kucoin.com/api/v1/market/candles?type=1day&symbol=ZEC-USDT&startAt={start}"
            ))
            .await?;
            out = arr(&k["data"])
                .iter()
                .map(|c| candle(day_s(f(&c[0]).unwrap_or(0.0)), f(&c[2]), f(&c[6])))
                .collect();
        }
        ("gate", "spot") => {
            let k = get(&format!(
                "https://api.gateio.ws/api/v4/spot/candlesticks?currency_pair=ZEC_USDT&interval=1d&limit={n}"
            ))
            .await?;
            out = arr(&k)
                .iter()
                .map(|c| candle(day_s(f(&c[0]).unwrap_or(0.0)), f(&c[2]), f(&c[1])))
                .collect();
        }
        ("bitget", "spot") => {
            let k = get(&format!(
                "https://api.bitget.com/api/v2/spot/market/candles?symbol=ZECUSDT&granularity=1day&limit={}",
                n.min(365)
            ))
            .await?;
            out = arr(&k["data"])
                .iter()
                .map(|c| candle(day_ms(f(&c[0]).unwrap_or(0.0)), f(&c[4]), f(&c[6])))
                .collect();
        }
        ("htx", "spot") => {
            let k = get(&format!(
                "https://api.huobi.pro/market/history/kline?symbol=zecusdt&period=1day&size={}",
                n.min(2000)
            ))
            .await?;
            // HTX days run 16:00 → 16:00 UTC (midnight in UTC+8); filed under the UTC day
            // they start, so the newest candle never lands on a future date.
            out = arr(&k["data"])
                .iter()
                .map(|c| {
                    candle(
                        day_s(f(&c["id"]).unwrap_or(0.0)),
                        f(&c["close"]),
                        f(&c["vol"]),
                    )
                })
                .collect();
        }
        ("hyperliquid", _) => {
            let coin = if market == "spot" {
                hl_spot_pair().await?
            } else {
                "ZEC".into()
            };
            let k = post(
                HL,
                json!({ "type": "candleSnapshot", "req": { "coin": coin, "interval": "1d", "startTime": since_ms } }),
            )
            .await?;
            out = arr(&k)
                .iter()
                .map(|c| {
                    let close = f(&c["c"]);
                    candle(
                        day_ms(f(&c["t"]).unwrap_or(0.0)),
                        close,
                        f(&c["v"]).zip(close).map(|(v, p)| v * p),
                    )
                })
                .collect();
            if market == "perp" {
                // 500 hourly samples per call.
                let mut start = since_ms.max(Utc::now().timestamp_millis() - 60 * 86_400_000);
                let mut s = Vec::new();
                for _ in 0..8 {
                    let page = post(
                        HL,
                        json!({ "type": "fundingHistory", "coin": "ZEC", "startTime": start }),
                    )
                    .await?;
                    let page: Vec<(f64, f64)> = arr(&page)
                        .iter()
                        .filter_map(|r| Some((f(&r["time"])?, f(&r["fundingRate"])?)))
                        .collect();
                    let Some(last) = page.last() else { break };
                    start = last.0 as i64 + 1;
                    let full = page.len() >= 500;
                    s.extend(page);
                    if !full {
                        break;
                    }
                }
                out.extend(daily_funding(&s, 8.0));
            }
        }
        ("binance", "perp") => {
            let k = get(&format!(
                "https://fapi.binance.com/fapi/v1/klines?symbol=ZECUSDT&interval=1d&limit={n}"
            ))
            .await?;
            out = arr(&k)
                .iter()
                .map(|c| candle(day_ms(f(&c[0]).unwrap_or(0.0)), f(&c[4]), f(&c[7])))
                .collect();
            let oi = get("https://fapi.binance.com/futures/data/openInterestHist?symbol=ZECUSDT&period=1d&limit=30").await?;
            out.extend(arr(&oi).iter().map(|r| Day {
                // Snapshot at the start of the next day = end of this one.
                day: day_ms(f(&r["timestamp"]).unwrap_or(0.0) - 1.0),
                oi_usd: f(&r["sumOpenInterestValue"]),
                ..Default::default()
            }));
            let fr = get(&format!(
                "https://fapi.binance.com/fapi/v1/fundingRate?symbol=ZECUSDT&limit=1000&startTime={since_ms}"
            ))
            .await?;
            let s: Vec<(f64, f64)> = arr(&fr)
                .iter()
                .filter_map(|r| Some((f(&r["fundingTime"])?, f(&r["fundingRate"])?)))
                .collect();
            out.extend(daily_funding(&s, 1.0));
        }
        ("bybit", "perp") => {
            let k = get(&format!(
                "https://api.bybit.com/v5/market/kline?category=linear&symbol=ZECUSDT&interval=D&limit={}",
                n.min(1000)
            ))
            .await?;
            let closes: Vec<Day> = arr(&k["result"]["list"])
                .iter()
                .map(|c| candle(day_ms(f(&c[0]).unwrap_or(0.0)), f(&c[4]), f(&c[6])))
                .collect();
            let oi = get(&format!(
                "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=ZECUSDT&intervalTime=1d&limit={}",
                n.min(200)
            ))
            .await?;
            // Open interest is in ZEC: value it at that day's close.
            for r in arr(&oi["result"]["list"]) {
                let day = day_ms(f(&r["timestamp"]).unwrap_or(0.0));
                let close = closes.iter().find(|c| c.day == day).and_then(|c| c.close);
                out.push(Day {
                    day,
                    oi_usd: f(&r["openInterest"]).zip(close).map(|(a, b)| a * b),
                    ..Default::default()
                });
            }
            out.extend(closes);
            let fr = get(&format!(
                "https://api.bybit.com/v5/market/funding/history?category=linear&symbol=ZECUSDT&limit=200&startTime={since_ms}"
            ))
            .await?;
            let s: Vec<(f64, f64)> = arr(&fr["result"]["list"])
                .iter()
                .filter_map(|r| Some((f(&r["fundingRateTimestamp"])?, f(&r["fundingRate"])?)))
                .collect();
            out.extend(daily_funding(&s, 1.0));
        }
        _ => {}
    }
    let cutoff = (Utc::now() - chrono::Duration::days(days)).date_naive();
    out.retain(|d| d.day >= cutoff && d.day.year_ce().1 > 2000);
    Ok(out)
}

use chrono::Datelike;

async fn store_venue(db: &PgPool, v: &Venue) -> Result<()> {
    sqlx::query(
        "INSERT INTO market_venues (venue, market, pair, kind, price, volume_24h_usd, bid, ask,
                                    oi_usd, oi_zec, funding_8h, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
         ON CONFLICT (venue, market) DO UPDATE SET pair = $3, kind = $4, price = $5,
            volume_24h_usd = $6, bid = $7, ask = $8, oi_usd = $9, oi_zec = $10, funding_8h = $11,
            updated_at = now()",
    )
    .bind(v.venue)
    .bind(v.market)
    .bind(&v.pair)
    .bind(v.kind)
    .bind(v.price)
    .bind(v.volume_24h_usd)
    .bind(v.bid)
    .bind(v.ask)
    .bind(v.oi_usd)
    .bind(v.oi_zec)
    .bind(v.funding_8h)
    .execute(db)
    .await?;
    // Today's open interest and funding from the live figures, until history covers the day.
    if v.oi_usd.is_some() || v.funding_8h.is_some() {
        sqlx::query(
            "INSERT INTO market_daily (venue, market, day, oi_usd, funding_8h)
             VALUES ($1, $2, (now() AT TIME ZONE 'UTC')::date, $3, $4)
             ON CONFLICT (venue, market, day) DO UPDATE SET
                oi_usd = COALESCE(EXCLUDED.oi_usd, market_daily.oi_usd),
                funding_8h = COALESCE(market_daily.funding_8h, EXCLUDED.funding_8h)",
        )
        .bind(v.venue)
        .bind(v.market)
        .bind(v.oi_usd)
        .bind(v.funding_8h)
        .execute(db)
        .await?;
    }
    Ok(())
}

async fn store_days(db: &PgPool, venue: &str, market: &str, days: &[Day]) -> Result<()> {
    for d in days {
        sqlx::query(
            "INSERT INTO market_daily (venue, market, day, close, volume_usd, oi_usd, funding_8h)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (venue, market, day) DO UPDATE SET
                close = COALESCE(EXCLUDED.close, market_daily.close),
                volume_usd = COALESCE(EXCLUDED.volume_usd, market_daily.volume_usd),
                oi_usd = COALESCE(EXCLUDED.oi_usd, market_daily.oi_usd),
                funding_8h = COALESCE(EXCLUDED.funding_8h, market_daily.funding_8h)",
        )
        .bind(venue)
        .bind(market)
        .bind(d.day)
        .bind(d.close)
        .bind(d.volume_usd)
        .bind(d.oi_usd)
        .bind(d.funding_8h)
        .execute(db)
        .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// ZEC on other chains
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct Asset {
    pub id: String,
    pub name: String,
    pub symbol: String,
    pub chain: String,
    pub chain_name: String,
    pub address: String,
    pub decimals: u32,
    pub issuer: String,
    pub model: String,
    pub backed_by: String,
    #[serde(default)]
    pub locked_by: Option<String>,
    pub supply: SupplyReader,
    #[serde(default)]
    pub geckoterminal: Option<String>,
    #[serde(default)]
    pub coingecko: Option<String>,
    #[serde(default)]
    pub hyperliquid_spot: Option<String>,
    pub website: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SupplyReader {
    pub kind: String,
    #[serde(default)]
    pub rpc: Option<String>,
}

#[derive(Deserialize)]
struct AssetsFile {
    assets: Vec<Asset>,
}

pub fn assets() -> &'static [Asset] {
    static A: OnceLock<Vec<Asset>> = OnceLock::new();
    A.get_or_init(|| {
        serde_json::from_str::<AssetsFile>(include_str!("../../../labels/zec_assets.json"))
            .expect("labels/zec_assets.json is valid")
            .assets
    })
}

async fn rpc(url: &str, method: &str, params: Value) -> Result<Value> {
    let r = post(
        url,
        json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params }),
    )
    .await?;
    if let Some(e) = r.get("error") {
        bail!("{method}: {e}");
    }
    Ok(r["result"].clone())
}

/// Parses a big integer (hex from EVM, decimal elsewhere) into token units.
fn units(raw: &str, decimals: u32, hex: bool) -> Result<f64> {
    let v = if hex {
        u128::from_str_radix(raw.trim_start_matches("0x"), 16)?
    } else {
        raw.parse::<u128>()?
    };
    Ok(v as f64 / 10f64.powi(decimals as i32))
}

/// Total supply of the asset on its own chain, in ZEC.
async fn supply(a: &Asset) -> Result<f64> {
    let url = a.supply.rpc.as_deref().unwrap_or_default();
    match a.supply.kind.as_str() {
        "evm" => {
            let r = rpc(
                url,
                "eth_call",
                json!([{ "to": a.address, "data": "0x18160ddd" }, "latest"]),
            )
            .await?;
            units(r.as_str().context("totalSupply")?, a.decimals, true)
        }
        "solana" => {
            let r = rpc(url, "getTokenSupply", json!([a.address])).await?;
            units(
                r["value"]["amount"].as_str().context("amount")?,
                a.decimals,
                false,
            )
        }
        "near" => near_view(url, &a.address, "ft_total_supply", json!({}))
            .await
            .and_then(|s| units(&s, a.decimals, false)),
        "coingecko" => {
            // CoinGecko throttles cloud IPs: hourly at most, the last supply stays otherwise.
            if !due(&format!("supply:{}", a.id), 3600) {
                bail!("not due");
            }
            let id = a.coingecko.as_deref().context("coingecko id")?;
            let r = get(&format!(
                "https://api.coingecko.com/api/v3/coins/{id}?localization=false&tickers=false&community_data=false&developer_data=false"
            ))
            .await?;
            f(&r["market_data"]["total_supply"]).context("total_supply")
        }
        k => bail!("unknown supply reader {k}"),
    }
}

/// A NEAR view call returning a JSON string (token amounts).
async fn near_view(url: &str, account: &str, method: &str, args: Value) -> Result<String> {
    use base64::Engine;
    let r = rpc(
        url,
        "query",
        json!({ "request_type": "call_function", "finality": "final", "account_id": account,
                "method_name": method,
                "args_base64": base64::engine::general_purpose::STANDARD.encode(args.to_string()) }),
    )
    .await?;
    let bytes: Vec<u8> = serde_json::from_value(r["result"].clone())?;
    Ok(serde_json::from_slice::<String>(&bytes)?)
}

/// ZEC of `parent` locked by `holder` to back a child asset (e.g. NEAR ZEC held by the Omni
/// bridge for Solana).
pub async fn locked(parent: &Asset, holder: &str) -> Result<f64> {
    match parent.supply.kind.as_str() {
        "near" => {
            let s = near_view(
                parent.supply.rpc.as_deref().unwrap_or_default(),
                &parent.address,
                "ft_balance_of",
                json!({ "account_id": holder }),
            )
            .await?;
            units(&s, parent.decimals, false)
        }
        k => bail!("locked balance not supported for {k}"),
    }
}

struct Market {
    holders: Option<i64>,
    holders_top10: Option<f64>,
    price: Option<f64>,
    liquidity: Option<f64>,
    volume: Option<f64>,
    pools: Value,
}

/// Where to buy ZEC in a pool: the DEX's own swap page with ZEC as the output token, else the
/// pool page on GeckoTerminal.
fn trade_url(dex: &str, network: &str, token: &str, pool: &str) -> String {
    let evm_chain = |uniswap: bool| match network {
        "bsc" => {
            if uniswap {
                "bnb"
            } else {
                "bsc"
            }
        }
        "base" => "base",
        _ => {
            if uniswap {
                "mainnet"
            } else {
                "eth"
            }
        }
    };
    if dex.starts_with("uniswap") {
        format!(
            "https://app.uniswap.org/swap?chain={}&outputCurrency={token}",
            evm_chain(true)
        )
    } else if dex.starts_with("pancakeswap") {
        format!(
            "https://pancakeswap.finance/swap?chain={}&outputCurrency={token}",
            evm_chain(false)
        )
    } else if dex.starts_with("aerodrome") {
        format!("https://aerodrome.finance/swap?from=eth&to={token}")
    } else if dex.starts_with("orca") {
        format!("https://www.orca.so/?tokenIn=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v&tokenOut={token}")
    } else if dex.starts_with("raydium") {
        format!("https://raydium.io/swap/?inputMint=sol&outputMint={token}")
    } else if dex.starts_with("meteora") {
        format!("https://app.meteora.ag/dlmm/{pool}")
    } else if dex.starts_with("rhea") || dex.starts_with("ref-finance") {
        format!("https://app.rhea.finance/#usdt.tether-token.near|{token}")
    } else if dex == "near-intents" {
        "https://app.near-intents.org/".into()
    } else {
        format!("https://www.geckoterminal.com/{network}/pools/{pool}")
    }
}

/// True at most once per `secs` for a key (for sources that can't take every round).
fn due(key: &str, secs: u64) -> bool {
    static LAST: OnceLock<std::sync::Mutex<std::collections::HashMap<String, std::time::Instant>>> =
        OnceLock::new();
    let mut m = LAST.get_or_init(Default::default).lock().unwrap();
    match m.get(key) {
        Some(t) if t.elapsed() < Duration::from_secs(secs) => false,
        _ => {
            m.insert(key.to_string(), std::time::Instant::now());
            true
        }
    }
}

/// DexScreener's chain ids for our registry's network names.
fn dexscreener_chain(network: &str) -> &str {
    match network {
        "eth" => "ethereum",
        other => other,
    }
}

/// DEX figures from DexScreener (generous public API): every pair holding the token, its 24h
/// volume, liquidity and price from the deepest pair where it is the base token.
async fn dexscreener(network: &str, address: &str) -> Result<Market> {
    let pairs = get(&format!(
        "https://api.dexscreener.com/token-pairs/v1/{}/{address}",
        dexscreener_chain(network)
    ))
    .await?;
    let mut pairs = pairs.as_array().cloned().unwrap_or_default();
    let liq = |p: &Value| f(&p["liquidity"]["usd"]).unwrap_or(0.0);
    pairs.sort_by(|a, b| liq(b).total_cmp(&liq(a)));
    let is_base = |p: &Value| {
        p["baseToken"]["address"]
            .as_str()
            .is_some_and(|x| x.eq_ignore_ascii_case(address))
    };
    let pools: Vec<Value> = pairs
        .iter()
        .take(12)
        .map(|p| {
            let dex = p["dexId"].as_str().unwrap_or_default();
            let pool = p["pairAddress"].as_str().unwrap_or_default();
            json!({
                "name": format!("{} / {}", p["baseToken"]["symbol"].as_str().unwrap_or("?"), p["quoteToken"]["symbol"].as_str().unwrap_or("?")),
                "dex": dex,
                "address": pool,
                "liquidity_usd": f(&p["liquidity"]["usd"]),
                "volume_24h_usd": f(&p["volume"]["h24"]),
                "url": p["url"],
                "trade_url": trade_url(dex, network, address, pool),
            })
        })
        .collect();
    Ok(Market {
        holders: None,
        holders_top10: None,
        price: pairs
            .iter()
            .find(|p| is_base(p))
            .and_then(|p| f(&p["priceUsd"])),
        // No pair at all: unknown, not zero.
        liquidity: (!pairs.is_empty()).then(|| pairs.iter().map(liq).sum()),
        volume: (!pairs.is_empty())
            .then(|| pairs.iter().filter_map(|p| f(&p["volume"]["h24"])).sum()),
        pools: Value::Array(pools),
    })
}

/// Holder count and top-10 share from GeckoTerminal token info (one call, no retry).
async fn gt_holders(network: &str, address: &str) -> Result<(Option<i64>, Option<f64>)> {
    let info = get(&format!(
        "https://api.geckoterminal.com/api/v2/networks/{network}/tokens/{address}/info"
    ))
    .await?;
    let h = &info["data"]["attributes"]["holders"];
    Ok((
        h["count"].as_i64(),
        f(&h["distribution_percentage"]["top_10"]).map(|p| p / 100.0),
    ))
}

async fn refresh_asset(db: &PgPool, a: &Asset) -> Result<()> {
    let supply = supply(a)
        .await
        .map_err(|e| tracing::debug!(asset = %a.id, "supply: {e:#}"))
        .ok();
    let market = match (&a.geckoterminal, &a.hyperliquid_spot) {
        (Some(net), _) => {
            let mut m = dexscreener(net, &a.address)
                .await
                .map_err(|e| tracing::warn!(asset = %a.id, "dexscreener: {e:#}"))
                .ok();
            // Holders only come from GeckoTerminal, whose free tier cloud IPs exhaust: hourly,
            // best effort, the last figures stay otherwise.
            if due(&format!("holders:{}", a.id), 3600) {
                if let (Some(m), Ok((h, top))) = (m.as_mut(), gt_holders(net, &a.address).await) {
                    m.holders = h;
                    m.holders_top10 = top;
                }
            }
            m
        }
        (None, Some(_)) => {
            // An order book, not pools: the spot venue's own figures.
            let v = snapshot("hyperliquid", "spot").await.ok();
            v.map(|v| Market {
                holders: None,
                holders_top10: None,
                price: v.price,
                liquidity: None,
                volume: v.volume_24h_usd,
                pools: json!([{ "name": v.pair, "dex": "hyperliquid", "liquidity_usd": null,
                                "volume_24h_usd": v.volume_24h_usd,
                                "url": "https://app.hyperliquid.xyz/trade/UZEC/USDC",
                                "trade_url": "https://app.hyperliquid.xyz/trade/UZEC/USDC" }]),
            })
        }
        _ => None,
    };
    // ZEC of this asset locked to mint the listed assets it backs.
    let mut lock = None;
    for child in assets().iter().filter(|c| c.backed_by == a.id) {
        if let Some(holder) = &child.locked_by {
            match locked(a, holder).await {
                Ok(v) => *lock.get_or_insert(0.0) += v,
                Err(e) => tracing::debug!(asset = %a.id, "locked: {e:#}"),
            }
        }
    }
    let (holders, holders_top10) = market
        .as_ref()
        .map_or((None, None), |m| (m.holders, m.holders_top10));
    let (price, liquidity, volume, pools) = match market {
        Some(m) => (m.price, m.liquidity, m.volume, Some(m.pools)),
        None => (None, None, None, None),
    };
    sqlx::query(
        "INSERT INTO zec_assets_latest (asset, supply, price_usd, liquidity_usd, volume_24h_usd, pools, locked,
                                        holders, holders_top10, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
         ON CONFLICT (asset) DO UPDATE SET
            supply = COALESCE($2, zec_assets_latest.supply),
            locked = COALESCE($7, zec_assets_latest.locked),
            holders = COALESCE($8, zec_assets_latest.holders),
            holders_top10 = COALESCE($9, zec_assets_latest.holders_top10),
            price_usd = COALESCE($3, zec_assets_latest.price_usd),
            liquidity_usd = COALESCE($4, zec_assets_latest.liquidity_usd),
            volume_24h_usd = COALESCE($5, zec_assets_latest.volume_24h_usd),
            pools = COALESCE($6, zec_assets_latest.pools),
            updated_at = now()",
    )
    .bind(&a.id)
    .bind(supply)
    .bind(price)
    .bind(liquidity)
    .bind(volume)
    .bind(pools)
    .bind(lock)
    .bind(holders)
    .bind(holders_top10)
    .execute(db)
    .await?;
    sqlx::query(
        "INSERT INTO zec_assets_daily (asset, day, supply, liquidity_usd, volume_usd)
         SELECT asset, (now() AT TIME ZONE 'UTC')::date, supply, liquidity_usd, volume_24h_usd
         FROM zec_assets_latest WHERE asset = $1
         ON CONFLICT (asset, day) DO UPDATE SET supply = EXCLUDED.supply,
            liquidity_usd = EXCLUDED.liquidity_usd, volume_usd = EXCLUDED.volume_usd",
    )
    .bind(&a.id)
    .execute(db)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------

pub async fn run(db: PgPool) {
    // History first in the background: a year of candles takes a few dozen calls.
    tokio::spawn(backfill(db.clone(), BACKFILL_DAYS));
    let mut round = 0u32;
    loop {
        for (venue, market) in VENUES {
            match snapshot(venue, market).await {
                Ok(v) => {
                    if let Err(e) = store_venue(&db, &v).await {
                        tracing::warn!(venue, market, "market store: {e:#}");
                    }
                }
                Err(e) => tracing::debug!(venue, market, "market snapshot: {e:#}"),
            }
        }
        for a in assets() {
            if let Err(e) = refresh_asset(&db, a).await {
                tracing::warn!(asset = %a.id, "zec asset: {e:#}");
            }
        }
        // Recent candles every hour (days close at 00:00 UTC).
        if round > 0 && round.is_multiple_of(12) {
            tokio::spawn(backfill(db.clone(), RECENT_DAYS));
        }
        round = round.wrapping_add(1);
        tokio::time::sleep(SNAPSHOT_EVERY).await;
    }
}

async fn backfill(db: PgPool, days: i64) {
    for (venue, market) in VENUES {
        match history(venue, market, days).await {
            Ok(rows) => {
                if let Err(e) = store_days(&db, venue, market, &rows).await {
                    tracing::warn!(venue, market, "market history store: {e:#}");
                } else if days > RECENT_DAYS {
                    tracing::info!(venue, market, days = rows.len(), "market history");
                }
            }
            Err(e) => tracing::warn!(venue, market, "market history: {e:#}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_parses_and_chains_back_to_zcash() {
        let a = assets();
        assert!(a.len() >= 5);
        for x in a {
            assert!(
                x.backed_by == "zcash" || a.iter().any(|p| p.id == x.backed_by),
                "{} is backed by an unknown asset",
                x.id
            );
        }
    }

    #[test]
    fn parses_token_units() {
        assert_eq!(units("0x2e8b", 2, true).unwrap(), 119.15);
        assert_eq!(units("14406857702356", 8, false).unwrap(), 144068.57702356);
    }

    #[test]
    fn trade_links_open_the_dex_with_zec_as_output() {
        assert_eq!(
            trade_url("pancakeswap-v3-bsc", "bsc", "0xzec", "0xpool"),
            "https://pancakeswap.finance/swap?chain=bsc&outputCurrency=0xzec"
        );
        assert_eq!(
            trade_url("uniswap-bsc", "bsc", "0xzec", "0xpool"),
            "https://app.uniswap.org/swap?chain=bnb&outputCurrency=0xzec"
        );
        assert!(trade_url("unknown-dex", "solana", "mint", "pool").ends_with("/solana/pools/pool"));
    }

    #[test]
    fn funding_is_averaged_per_day() {
        let d = daily_funding(&[(0.0, 0.0001), (3_600_000.0, 0.0003)], 8.0);
        assert_eq!(d.len(), 1);
        assert!((d[0].funding_8h.unwrap() - 0.0016).abs() < 1e-12);
    }
}

// ---------------------------------------------------------------------------------------------
// Other coins (for comparisons and the calculator)
// ---------------------------------------------------------------------------------------------

/// Coins ZEC is compared with: (CoinPaprika id, symbol, name).
const COINS: &[(&str, &str, &str)] = &[
    ("btc-bitcoin", "BTC", "Bitcoin"),
    ("eth-ethereum", "ETH", "Ethereum"),
    ("usdt-tether", "USDT", "Tether"),
    ("bnb-binance-coin", "BNB", "BNB"),
    ("xrp-xrp", "XRP", "XRP"),
    ("usdc-usd-coin", "USDC", "USD Coin"),
    ("sol-solana", "SOL", "Solana"),
    ("trx-tron", "TRX", "TRON"),
    ("hype-hyperliquid", "HYPE", "Hyperliquid"),
    ("doge-dogecoin", "DOGE", "Dogecoin"),
    ("xmr-monero", "XMR", "Monero"),
    ("ada-cardano", "ADA", "Cardano"),
    ("bch-bitcoin-cash", "BCH", "Bitcoin Cash"),
    ("near-near-protocol", "NEAR", "NEAR"),
    ("ltc-litecoin", "LTC", "Litecoin"),
    ("sui-sui", "SUI", "Sui"),
    ("toncoin-the-open-network", "TON", "Toncoin"),
    ("avax-avalanche", "AVAX", "Avalanche"),
    ("dash-dash", "DASH", "Dash"),
    ("zec-zcash", "ZEC", "Zcash"),
];

#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct Coin {
    pub symbol: String,
    pub name: String,
    pub price_usd: f64,
    pub market_cap_usd: Option<f64>,
    pub change_24h: Option<f64>,
    pub rank: Option<i64>,
}

/// Prices and market caps of the coins above (CoinPaprika's bulk tickers, one call, cached 10
/// minutes; the last good list is kept if a refresh fails).
pub async fn coins() -> Result<Vec<Coin>> {
    type Cache = Option<(std::time::Instant, Vec<Coin>)>;
    static CACHE: OnceLock<tokio::sync::Mutex<Cache>> = OnceLock::new();
    let mut cache = CACHE
        .get_or_init(|| tokio::sync::Mutex::new(None))
        .lock()
        .await;
    if let Some((at, c)) = cache.as_ref() {
        if at.elapsed() < Duration::from_secs(600) {
            return Ok(c.clone());
        }
    }
    let fetched: Result<Vec<Coin>> = async {
        let all = get("https://api.coinpaprika.com/v1/tickers?quotes=USD").await?;
        let all = all.as_array().context("tickers")?;
        Ok(COINS
            .iter()
            .filter_map(|(id, symbol, name)| {
                let t = all.iter().find(|t| t["id"] == *id)?;
                let q = &t["quotes"]["USD"];
                Some(Coin {
                    symbol: (*symbol).into(),
                    name: (*name).into(),
                    price_usd: f(&q["price"])?,
                    market_cap_usd: f(&q["market_cap"]).filter(|m| *m > 0.0),
                    change_24h: f(&q["percent_change_24h"]).map(|p| p / 100.0),
                    rank: t["rank"].as_i64(),
                })
            })
            .collect())
    }
    .await;
    match fetched {
        Ok(c) if !c.is_empty() => {
            *cache = Some((std::time::Instant::now(), c.clone()));
            Ok(c)
        }
        Ok(_) => cache.as_ref().map(|(_, c)| c.clone()).context("no coins"),
        Err(e) => cache.as_ref().map(|(_, c)| c.clone()).ok_or(e),
    }
}
