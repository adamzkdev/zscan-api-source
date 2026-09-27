//! ZScan public REST API.

mod docs;
mod error;
mod models;
mod routes;
mod ws;

use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{
    extract::{ConnectInfo, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use tokio::sync::{broadcast, Semaphore};
use tower_http::{compression::CompressionLayer, cors::CorsLayer, trace::TraceLayer};
use tracing::info;
use zscan_chain::{Lwd, Resolver};
use zscan_db::Db;

#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub lwd: Lwd,
    pub resolver: Resolver,
    pub events: broadcast::Sender<String>,
    pub http: reqwest::Client,
    pub price: routes::price::SharedPriceCache,
    pub accounts: routes::rankings::AccountsCache,
    limiter: Arc<DefaultKeyedRateLimiter<IpAddr>>,
    /// Stricter per-IP limit for `POST /tx/broadcast`, which costs a node round-trip.
    broadcast_limiter: Arc<DefaultKeyedRateLimiter<IpAddr>>,
    /// Caps concurrent WebSocket clients.
    pub ws_slots: Arc<Semaphore>,
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    // `zscan-api healthcheck`: container healthcheck without curl (distroless image).
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        return healthcheck().await;
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=warn".into()),
        )
        .init();

    let db = Db::connect(
        &std::env::var("DATABASE_URL").context("DATABASE_URL is required")?,
        16,
    )
    .await?;
    db.migrate().await?;
    let lwd = Lwd::connect_with(
        &env_or(
            "LWD_URL",
            "https://zec.rocks:443,https://na.zec.rocks:443,https://eu.zec.rocks:443",
        ),
        env_or("LWD_RATE_PER_SEC", "20").parse()?,
    )?;
    let per_sec: u32 = env_or("API_RATE_PER_SEC", "10").parse()?;
    let limiter = Arc::new(RateLimiter::keyed(
        Quota::per_second(NonZeroU32::new(per_sec.max(1)).unwrap())
            .allow_burst(NonZeroU32::new(per_sec.max(1) * 3).unwrap()),
    ));
    let broadcast_limiter = Arc::new(RateLimiter::keyed(
        Quota::per_minute(NonZeroU32::new(6).unwrap()).allow_burst(NonZeroU32::new(3).unwrap()),
    ));
    let ws_slots = Arc::new(Semaphore::new(env_or("API_MAX_WS", "2000").parse()?));
    let (events, _) = broadcast::channel(1024);

    let state = AppState {
        resolver: Resolver::new(lwd.clone(), 50_000),
        db,
        lwd,
        events,
        http: zscan_chain::http_client_builder()
            .timeout(std::time::Duration::from_secs(10))
            .user_agent("zscan/0.1")
            .build()?,
        price: Default::default(),
        accounts: routes::rankings::empty_accounts(),
        limiter,
        broadcast_limiter,
        ws_slots,
    };
    // Forget idle clients so the per-IP tables don't grow without bound.
    {
        let (a, b) = (state.limiter.clone(), state.broadcast_limiter.clone());
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                for l in [&a, &b] {
                    l.retain_recent();
                    l.shrink_to_fit();
                }
            }
        });
    }
    tokio::spawn(ws::listen(state.clone()));
    tokio::spawn(routes::rankings::refresh_accounts(state.clone()));

    let v1 = Router::new()
        .route("/", get(docs::index))
        .route("/status", get(routes::network::status))
        .route("/blocks", get(routes::blocks::list))
        .route("/blocks/{id}", get(routes::blocks::detail))
        .route("/txs", get(routes::tx::list))
        .route("/tx/{txid}", get(routes::tx::detail))
        .route("/tx/broadcast", post(routes::tx::broadcast))
        .route("/tx/{txid}/shielded", get(routes::compact::tx_outputs))
        .route("/compact", get(routes::compact::range))
        .route("/address/{address}", get(routes::address::detail))
        .route("/search", get(routes::search::search))
        .route("/pools", get(routes::pools::current))
        .route("/pools/history", get(routes::pools::history))
        .route("/ironwood/migration", get(routes::pools::migration))
        .route("/mempool", get(routes::network::mempool))
        .route("/network", get(routes::network::network))
        .route("/stats/daily", get(routes::network::daily))
        .route("/price", get(routes::price::price))
        .route("/price/history", get(routes::price::history))
        .route("/miners", get(routes::rankings::miners))
        .route("/supply", get(routes::supply::supply))
        .route("/supply/audit", get(routes::supply::audit))
        .route("/funding", get(routes::supply::funding))
        .route("/privacy/anonymity-sets", get(routes::supply::anonymity))
        .route("/fees", get(routes::fees::fees))
        .route("/health", get(routes::status::health))
        .route("/unified/{address}", get(routes::address::unified))
        .route("/address/{address}/csv", get(routes::address::csv))
        .route("/accounts", get(routes::rankings::accounts))
        .route("/upgrades", get(routes::upgrades::list))
        .route("/labels", get(routes::labels::list))
        .route("/protocols", get(routes::protocols::list))
        .route("/protocols/{slug}", get(routes::protocols::detail))
        .route("/ws", get(ws::handler))
        .layer(middleware::from_fn(cache_control))
        .layer(middleware::from_fn_with_state(state.clone(), rate_limit));

    let app = Router::new()
        .nest("/v1", v1)
        .route("/openapi.json", get(|| async { Json(docs::openapi()) }))
        .route("/docs", get(docs::page))
        .route("/health", get(|| async { "ok" }))
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        // Traces carry method, path and status only: no client addresses or headers.
        .layer(
            TraceLayer::new_for_http().make_span_with(|req: &Request| {
                tracing::debug_span!("http", method = %req.method(), path = %req.uri().path())
            }),
        )
        .with_state(state);

    let addr: SocketAddr = env_or("API_ADDR", "0.0.0.0:8080").parse()?;
    info!(%addr, "zscan api listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

async fn healthcheck() -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let port = env_or("API_ADDR", "0.0.0.0:8080")
        .rsplit(':')
        .next()
        .unwrap_or("8080")
        .to_string();
    let mut s = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await?;
    s.write_all(b"GET /health HTTP/1.0\r\n\r\n").await?;
    let mut buf = [0u8; 12];
    s.read_exact(&mut buf).await?;
    anyhow::ensure!(buf.ends_with(b"200"), "unhealthy");
    Ok(())
}

fn is_internal(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback() || v4.is_private(),
        IpAddr::V6(v6) => v6.is_loopback() || (v6.segments()[0] & 0xfe00) == 0xfc00,
    }
}

/// Lets browsers and CDNs reuse successful GET responses briefly. Data changes at most once per
/// block, so a few seconds of staleness is invisible, and it absorbs bursts on hot endpoints.
async fn cache_control(req: Request, next: Next) -> Response {
    let get = req.method() == axum::http::Method::GET;
    let mut res = next.run(req).await;
    if get
        && res.status().is_success()
        && !res
            .headers()
            .contains_key(axum::http::header::CACHE_CONTROL)
    {
        res.headers_mut().insert(
            axum::http::header::CACHE_CONTROL,
            axum::http::HeaderValue::from_static("public, max-age=10"),
        );
    }
    res
}

/// The client address to rate-limit. `X-Forwarded-For` is only trusted when the request comes
/// from our own reverse proxy / web container (a private address); anyone else could forge it.
/// The last hop is the one appended by the proxy closest to us.
fn client_ip(peer: IpAddr, headers: &HeaderMap) -> Option<IpAddr> {
    if !is_internal(peer) {
        return Some(peer);
    }
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.rsplit(',').next())
        .and_then(|v| v.trim().parse().ok())
}

/// Per-IP rate limiting (see [`client_ip`]).
async fn rate_limit(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> Response {
    // Server-side rendering from our own web container arrives from a private address with no
    // forwarded client: it serves many visitors, so it isn't limited as one.
    let Some(ip) = client_ip(peer.ip(), &headers) else {
        return next.run(req).await;
    };
    let limited = state.limiter.check_key(&ip).is_err()
        || (req.uri().path().ends_with("/tx/broadcast")
            && state.broadcast_limiter.check_key(&ip).is_err());
    if limited {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({ "error": "rate_limited", "message": "Too many requests" })),
        )
            .into_response();
    }
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn xff(v: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", v.parse().unwrap());
        h
    }

    #[test]
    fn public_peers_cannot_spoof_forwarded_for() {
        let peer: IpAddr = "203.0.113.7".parse().unwrap();
        assert_eq!(client_ip(peer, &xff("1.2.3.4")), Some(peer));
    }

    #[test]
    fn proxied_requests_use_last_hop() {
        let proxy: IpAddr = "172.18.0.5".parse().unwrap();
        let got = client_ip(proxy, &xff("1.2.3.4, 198.51.100.9"));
        assert_eq!(got, Some("198.51.100.9".parse().unwrap()));
        assert_eq!(client_ip(proxy, &HeaderMap::new()), None);
    }
}
