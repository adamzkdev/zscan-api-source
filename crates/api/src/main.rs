//! ZScan public REST API.

mod docs;
mod error;
mod models;
mod routes;
mod ws;

use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;

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
use tokio::sync::broadcast;
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
    };
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
        .route("/address/{address}", get(routes::address::detail))
        .route("/search", get(routes::search::search))
        .route("/pools", get(routes::pools::current))
        .route("/pools/history", get(routes::pools::history))
        .route("/ironwood/migration", get(routes::pools::migration))
        .route("/mempool", get(routes::network::mempool))
        .route("/network", get(routes::network::network))
        .route("/stats/daily", get(routes::network::daily))
        .route("/price", get(routes::price::price))
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
        .route("/ws", get(ws::handler))
        .layer(middleware::from_fn_with_state(state.clone(), rate_limit));

    let app = Router::new()
        .nest("/v1", v1)
        .route("/openapi.json", get(|| async { Json(docs::openapi()) }))
        .route("/docs", get(docs::page))
        .route("/health", get(|| async { "ok" }))
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
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

/// Per-IP rate limiting. Behind a reverse proxy the first `X-Forwarded-For` hop is used.
async fn rate_limit(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> Response {
    let forwarded: Option<IpAddr> = headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .and_then(|v| v.trim().parse().ok());
    // Server-side rendering from our own web container arrives from a private address with no
    // forwarded client: it serves many visitors, so it isn't limited as one.
    if forwarded.is_none() && is_internal(peer.ip()) {
        return next.run(req).await;
    }
    let ip = forwarded.unwrap_or(peer.ip());
    if state.limiter.check_key(&ip).is_err() {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({ "error": "rate_limited", "message": "Too many requests" })),
        )
            .into_response();
    }
    next.run(req).await
}
