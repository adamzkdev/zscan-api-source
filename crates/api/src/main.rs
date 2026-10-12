//! ZScan public REST API.

mod accounts;
mod docs;
mod error;
mod hashsmash;
mod ipfs;
mod market;
mod models;
mod news;
mod nft_markets;
mod presence;
mod routes;
mod street;
mod street_agents;
mod street_chat;
mod street_llm;
mod street_pay;
mod street_plots;
mod street_pulse;
mod street_wallets;
mod street_works;
mod ws;
mod zips;

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
    routing::{delete, get, post},
    Json, Router,
};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use tokio::sync::{broadcast, Semaphore};
use tower_http::{
    catch_panic::CatchPanicLayer, compression::CompressionLayer, cors::CorsLayer, trace::TraceLayer,
};
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
    /// Separate, larger budget for pictures: a gallery page loads dozens at once.
    media_limiter: Arc<DefaultKeyedRateLimiter<IpAddr>>,
    /// Caps concurrent WebSocket clients.
    pub ws_slots: Arc<Semaphore>,
    /// API keys on public requests (own rate limit, usage counts), checked in Supabase.
    pub keys: accounts::Keys,
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

    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    // Migrations may run long (index builds), so they get their own connection without the
    // request pool's statement timeout.
    {
        let migrator = Db::connect(&database_url, 1).await?;
        migrator.migrate().await?;
        migrator.pool.close().await;
    }
    let db = Db::connect_bounded(
        &database_url,
        env_or("API_DB_CONNECTIONS", "16").parse()?,
        Duration::from_secs(env_or("API_STATEMENT_TIMEOUT_SECS", "15").parse()?),
    )
    .await?;
    // Short per-call timeout: a slow lightwalletd must not hold web requests for minutes.
    let lwd = Lwd::connect_with_timeout(
        &env_or(
            "LWD_URL",
            "https://zec.rocks:443,https://na.zec.rocks:443,https://eu.zec.rocks:443",
        ),
        env_or("LWD_RATE_PER_SEC", "20").parse()?,
        Duration::from_secs(env_or("API_LWD_TIMEOUT_SECS", "20").parse()?),
    )?;
    let per_sec: u32 = env_or("API_RATE_PER_SEC", "10").parse()?;
    let limiter = Arc::new(RateLimiter::keyed(
        Quota::per_second(NonZeroU32::new(per_sec.max(1)).unwrap())
            .allow_burst(NonZeroU32::new(per_sec.max(1) * 3).unwrap()),
    ));
    let broadcast_limiter = Arc::new(RateLimiter::keyed(
        Quota::per_minute(NonZeroU32::new(6).unwrap()).allow_burst(NonZeroU32::new(3).unwrap()),
    ));
    let media_per_sec: u32 = env_or("API_MEDIA_RATE_PER_SEC", "60").parse()?;
    let media_limiter = Arc::new(RateLimiter::keyed(
        Quota::per_second(NonZeroU32::new(media_per_sec.max(1)).unwrap())
            .allow_burst(NonZeroU32::new(media_per_sec.max(1) * 5).unwrap()),
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
        media_limiter,
        ws_slots,
        keys: accounts::Keys::new(
            zscan_chain::supabase::Supabase::from_env(zscan_chain::http_client_builder().build()?),
            env_or("API_KEY_RATE_PER_SEC", "50").parse()?,
        ),
    };
    // Forget idle clients so the per-IP tables don't grow without bound.
    {
        let (a, b, c) = (
            state.limiter.clone(),
            state.broadcast_limiter.clone(),
            state.media_limiter.clone(),
        );
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                for l in [&a, &b, &c] {
                    l.retain_recent();
                    l.shrink_to_fit();
                }
            }
        });
    }
    tokio::spawn(ws::listen(state.clone()));
    tokio::spawn(presence::tick());
    tokio::spawn(state.keys.clone().flush_loop());
    tokio::spawn(routes::rankings::refresh_accounts(state.clone()));
    tokio::spawn(refresh_nft_registry(state.http.clone()));
    if !zscan_chain::is_testnet() {
        tokio::spawn(routes::price::refresh(state.clone()));
    }
    tokio::spawn(routes::nft::warm_metadata(state.db.pool.clone()));
    // Markets and news don't depend on the network: a testnet instance leaves them to mainnet
    // (and doesn't double the calls against rate-limited sources like GeckoTerminal).
    let mainnet = std::env::var("ZSCAN_NETWORK").map_or(true, |n| n != "testnet");
    let default_on = if mainnet { "1" } else { "0" };
    if env_or("MARKET", default_on) != "0" {
        tokio::spawn(market::run(state.db.pool.clone()));
    }
    if env_or("NEWS", default_on) != "0" {
        tokio::spawn(news::run(state.db.pool.clone()));
    }
    if env_or("STREET_PAPER", default_on) != "0" {
        tokio::spawn(street::paper_loop(state.clone()));
    }
    if env_or("STREET_WORKS", default_on) != "0" {
        tokio::spawn(street_works::run(state.clone()));
    }
    // on when STREET_PAY_ADDRESS is set
    tokio::spawn(street_pay::run(state.clone()));
    if env_or("STREET_WALLETS", default_on) != "0" {
        tokio::spawn(street_wallets::run(state.clone()));
        tokio::spawn(street_pulse::run(state.clone()));
    }
    if env_or("HASHSMASH", default_on) != "0" {
        tokio::spawn(hashsmash::run(state.http.clone()));
    }
    if env_or("ZIPS", "1") != "0" {
        tokio::spawn(zips::run(state.http.clone()));
    }
    if env_or("NFT_ARCHIVE", "1") != "0" {
        tokio::spawn(nft_markets::archive(state.db.pool.clone()));
    }

    let v1 = Router::new()
        .route("/", get(docs::index))
        .route("/status", get(routes::network::status))
        .route("/blocks", get(routes::blocks::list))
        .route("/blocks/{id}", get(routes::blocks::detail))
        .route("/txs", get(routes::tx::list))
        .route("/tx/{txid}", get(routes::tx::detail))
        .route("/tx/broadcast", post(routes::tx::broadcast))
        .route("/decode/tx", post(routes::decode::tx))
        .route("/decode/address/{address}", get(routes::decode::address))
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
        .route("/market", get(routes::market::overview))
        .route("/market/daily", get(routes::market::daily))
        .route("/market/coins", get(routes::market::coins))
        .route("/ecosystem/assets", get(routes::market::assets))
        .route("/ecosystem/assets/daily", get(routes::market::assets_daily))
        .route("/news", get(routes::news::list))
        .route("/news/sources", get(routes::news::sources))
        .route("/zips", get(routes::zips::list))
        .route("/zips/{id}", get(routes::zips::detail))
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
        .route("/bridges", get(routes::bridges::overview))
        .route("/bridges/events", get(routes::bridges::events))
        .route("/bridges/flows", get(routes::bridges::flows))
        .route("/inscriptions", get(routes::inscriptions::list))
        .route("/inscriptions/stats", get(routes::inscriptions::stats))
        .route("/inscriptions/{id}", get(routes::inscriptions::detail))
        .route(
            "/inscriptions/{id}/content",
            get(routes::inscriptions::content),
        )
        .route(
            "/address/{address}/inscriptions",
            get(routes::inscriptions::by_address),
        )
        .route("/inscriptions/{id}/image", get(routes::inscriptions::image))
        .route("/collections", get(routes::nft::collections))
        .route("/collections/{slug}", get(routes::nft::collection))
        .route("/collections/{slug}/logo", get(routes::nft::logo))
        .route(
            "/collections/{slug}/stats",
            get(routes::nft::collection_stats),
        )
        .route("/collections/{slug}/map", get(routes::nft::collection_map))
        .route(
            "/collections/{slug}/map/image",
            get(routes::nft::collection_map_image),
        )
        .route(
            "/collections/{slug}/items/{id}",
            get(routes::nft::venue_item),
        )
        .route(
            "/collections/{slug}/items/{id}/image",
            get(routes::nft::venue_item_image),
        )
        .route(
            "/collections/{slug}/art/{id}/image",
            get(routes::nft::artwork_image),
        )
        .route("/nft-sales", get(routes::nft::sales))
        .route("/treasuries", get(routes::treasuries::list))
        .route("/treasuries/{slug}", get(routes::treasuries::detail))
        .route("/ws", get(ws::handler))
        .route("/street/presence", get(presence::handler))
        .route("/street/chat", post(street_chat::chat))
        .route("/street/gossip", get(street::gossip))
        .route("/street/agents", get(street_agents::agents))
        .route("/street/people", get(street_agents::people))
        .route("/street/paper", get(street::paper))
        .route("/street/mayor", get(street::mayor))
        .route("/street/events", get(street::events))
        .route("/street/works", get(street_works::works))
        .route("/street/works/vote", post(street_works::vote))
        .route("/street/memory", get(street::memory).post(street::remember))
        .route("/street/pay", get(street_pay::info))
        .route("/street/pay/intent", post(street_pay::intent))
        .route("/street/pay/{ref}", get(street_pay::status))
        .route("/street/marks", get(street_pay::marks))
        .route("/street/marks/{id}/hide", post(street_pay::hide))
        .route("/street/marks/{id}/{kind}", post(street_pay::rate))
        .route("/street/guestbook", get(street_pay::guestbook))
        .route("/street/tips", get(street_pay::tips))
        .route("/street/wallets", get(street_wallets::wallets))
        .route("/street/wallets/pulse", get(street_pulse::pulse))
        .route("/hashsmash", get(hashsmash::get))
        .route(
            "/street/plots",
            get(street_plots::list).post(street_plots::save),
        )
        .route("/street/plots/{plot}", delete(street_plots::free))
        .route("/street/plots/{plot}/report", post(street_plots::report))
        .route("/street/plots/{plot}/hide", post(street_plots::hide))
        .layer(middleware::from_fn(cache_control))
        .layer(middleware::from_fn_with_state(state.clone(), rate_limit));

    let app = Router::new()
        .nest("/v1", v1)
        .route("/openapi.json", get(|| async { Json(docs::openapi()) }))
        .route("/docs", get(docs::page))
        .route("/health", get(health));
    let app = outer_layers(app)
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
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    Ok(())
}

/// The layers every response goes through. Tower applies the last `.layer` outermost, so CORS
/// wraps everything else: rate-limit 429s (inner `rate_limit`), timeouts, panics turned into
/// 500s, unknown-route 404s and handler errors all carry `Access-Control-Allow-Origin`, and
/// browsers let the client read the status and `Retry-After` (exposed via `*`) to back off.
fn outer_layers<S: Clone + Send + Sync + 'static>(router: Router<S>) -> Router<S> {
    router
        .layer(middleware::from_fn(request_timeout))
        .layer(CompressionLayer::new())
        .layer(CatchPanicLayer::new())
        .layer(CorsLayer::permissive())
}

/// A 429 with `Retry-After` (whole seconds, at least 1), so clients know when to come back.
pub(crate) fn too_many_requests(wait: Duration, message: &str) -> Response {
    let secs = wait.as_secs() + u64::from(wait.subsec_nanos() > 0);
    (
        StatusCode::TOO_MANY_REQUESTS,
        [(axum::http::header::RETRY_AFTER, secs.max(1).to_string())],
        Json(serde_json::json!({ "error": "rate_limited", "message": message })),
    )
        .into_response()
}

/// How long until a keyed limiter would let `key` through again.
pub(crate) fn limited_for<K>(limiter: &DefaultKeyedRateLimiter<K>, key: &K) -> Option<Duration>
where
    K: std::hash::Hash + Eq + Clone,
{
    use governor::clock::Clock;
    limiter
        .check_key(key)
        .err()
        .map(|nu| nu.wait_time_from(limiter.clock().now()))
}

/// Resolves on SIGTERM (deploys, restarts) or Ctrl-C. In-flight requests then get a few seconds
/// to finish; long-lived connections (WebSockets) don't hold the shutdown past that.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = term => {},
    }
    let grace: u64 = env_or("API_SHUTDOWN_GRACE_SECS", "10")
        .parse()
        .unwrap_or(10);
    info!(grace, "shutting down: finishing in-flight requests");
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(grace)).await;
        info!("shutdown grace period over");
        std::process::exit(0);
    });
}

/// Readiness: the API answers and its database does. Platforms route traffic to a new
/// deployment only once this passes.
async fn health(State(s): State<AppState>) -> Response {
    let db = tokio::time::timeout(
        Duration::from_secs(3),
        sqlx::query("SELECT 1").execute(&s.db.pool),
    )
    .await;
    match db {
        Ok(Ok(_)) => "ok".into_response(),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "database unavailable").into_response(),
    }
}

/// Paths allowed to run long: the live WebSocket and compact-block downloads for the
/// in-browser viewing-key scanner.
fn long_running(path: &str) -> bool {
    path == "/v1/ws"
        || path == "/v1/street/presence"
        || path.starts_with("/v1/compact")
        || path.ends_with("/shielded")
}

/// Answers 503 instead of hanging when a request takes longer than `API_REQUEST_TIMEOUT_SECS`.
async fn request_timeout(req: Request, next: Next) -> Response {
    static LIMIT: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    let limit = *LIMIT.get_or_init(|| {
        Duration::from_secs(
            env_or("API_REQUEST_TIMEOUT_SECS", "30")
                .parse()
                .unwrap_or(30),
        )
    });
    if long_running(req.uri().path()) {
        return next.run(req).await;
    }
    let path = req.uri().path().to_string();
    match tokio::time::timeout(limit, next.run(req)).await {
        Ok(res) => res,
        Err(_) => {
            tracing::warn!(%path, "request timed out");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [(axum::http::header::RETRY_AFTER, "5")],
                Json(serde_json::json!({ "error": "timeout", "message": "The request took too long; try again" })),
            )
                .into_response()
        }
    }
}

/// Keeps the NFT collection registry current from its public repository, so merged changes
/// show up without a deploy. Versions that fail validation are ignored.
async fn refresh_nft_registry(http: reqwest::Client) {
    let url = env_or(
        "NFT_REGISTRY_URL",
        "https://raw.githubusercontent.com/adamzkdev/zscan-nft-registry/main/collections.json",
    );
    loop {
        let fetched = async {
            let r = http.get(&url).send().await?.error_for_status()?;
            r.text().await
        }
        .await;
        match fetched {
            Ok(body) => match zscan_chain::nft_projects::parse(&body) {
                Ok(list) => {
                    let n = list.len();
                    if zscan_chain::nft_projects::replace(&body, list) {
                        info!(collections = n, "NFT registry updated");
                    }
                }
                Err(e) => tracing::warn!("NFT registry rejected: {e}"),
            },
            Err(e) => tracing::warn!("NFT registry unreachable: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(600)).await;
    }
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

/// Addresses of our own network: loopback, RFC 1918, carrier-grade NAT (100.64.0.0/10, used by
/// some private networks), link-local, IPv6 unique-local and link-local. IPv4-mapped IPv6
/// addresses (`::ffff:a.b.c.d`, what a dual-stack listener reports) count as their IPv4.
fn is_internal(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(v4) => {
            let [a, b, ..] = v4.octets();
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || (a == 100 && (b & 0xc0) == 64)
        }
        IpAddr::V6(v6) => {
            let s0 = v6.segments()[0];
            v6.is_loopback() || (s0 & 0xfe00) == 0xfc00 || (s0 & 0xffc0) == 0xfe80
        }
    }
}

/// Whether a request carries our apps' shared token (`X-ZScan-Internal`, set from
/// `INTERNAL_API_TOKEN`): server-side rendering from the explorer, stats, console and AI. Their
/// requests are trusted like private-network ones, whatever address they arrive from.
fn has_internal_token(headers: &HeaderMap) -> bool {
    static TOKEN: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    let Some(expected) = TOKEN
        .get_or_init(|| {
            std::env::var("INTERNAL_API_TOKEN")
                .ok()
                .filter(|t| t.len() >= 16)
        })
        .as_deref()
    else {
        return false;
    };
    headers
        .get("x-zscan-internal")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|got| {
            got.len() == expected.len()
                && got
                    .bytes()
                    .zip(expected.bytes())
                    .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                    == 0
        })
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
pub(crate) fn client_ip(peer: IpAddr, headers: &HeaderMap) -> Option<IpAddr> {
    if !is_internal(peer) && !has_internal_token(headers) {
        return Some(peer.to_canonical());
    }
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.rsplit(',').next())
        .and_then(|v| v.trim().parse().ok())
}

/// Pictures and inscribed files: immutable, cached by browsers, and requested dozens at a time
/// by gallery pages.
fn is_media(path: &str) -> bool {
    path.ends_with("/image") || path.ends_with("/content") || path.ends_with("/logo")
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
    // A request with an API key is limited per key (a higher quota) instead of per IP.
    if let Some(key) = accounts::api_key(&headers).filter(|_| state.keys.enabled()) {
        let id = match state.keys.lookup(key).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({ "error": "invalid_api_key", "message": "Unknown or revoked API key" })),
                )
                    .into_response()
            }
            Err(e) => {
                tracing::error!("api key lookup: {e}");
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
        };
        if let Some(wait) = limited_for(&state.keys.limiter, &id) {
            return too_many_requests(wait, "Too many requests for this API key");
        }
        state.keys.record(id);
        return next.run(req).await;
    }
    let Some(ip) = client_ip(peer.ip(), &headers) else {
        return next.run(req).await;
    };
    let path = req.uri().path();
    let limiter = if is_media(path) {
        &state.media_limiter
    } else {
        &state.limiter
    };
    let limited = limited_for(limiter, &ip).or_else(|| {
        path.ends_with("/tx/broadcast")
            .then(|| limited_for(&state.broadcast_limiter, &ip))
            .flatten()
    });
    if let Some(wait) = limited {
        return too_many_requests(wait, "Too many requests");
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

    /// Every kind of response, including a 429 from an inner layer, a panic, an unknown route
    /// and a handler error, carries the CORS headers so browsers let the client read it.
    #[tokio::test]
    async fn every_response_carries_cors() {
        use axum::body::Body;
        use axum::http::header;
        use tower::ServiceExt;

        // Stands in for `rate_limit`: an inner middleware that turns the request away.
        let limited = get(|| async { "never reached" }).layer(middleware::from_fn(
            |_req: Request, _next: Next| async {
                too_many_requests(Duration::from_millis(1500), "Too many requests")
            },
        ));
        let app = outer_layers(
            Router::new()
                .route("/ok", get(|| async { "ok" }))
                .route(
                    "/boom",
                    get(|| async {
                        if true {
                            panic!("handler panic");
                        }
                        "unreachable"
                    }),
                )
                .route(
                    "/err",
                    get(|| async { (StatusCode::BAD_GATEWAY, "Image unavailable") }),
                )
                .route("/limited", limited),
        );

        for (path, status) in [
            ("/ok", StatusCode::OK),
            ("/limited", StatusCode::TOO_MANY_REQUESTS),
            ("/boom", StatusCode::INTERNAL_SERVER_ERROR),
            ("/err", StatusCode::BAD_GATEWAY),
            ("/missing", StatusCode::NOT_FOUND),
        ] {
            let req = axum::http::Request::get(path)
                .header(header::ORIGIN, "http://localhost:3007")
                .body(Body::empty())
                .unwrap();
            let res = app.clone().oneshot(req).await.unwrap();
            assert_eq!(res.status(), status, "{path}");
            let h = res.headers();
            assert_eq!(h[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*", "{path}");
            assert_eq!(h[header::ACCESS_CONTROL_EXPOSE_HEADERS], "*", "{path}");
            if status == StatusCode::TOO_MANY_REQUESTS {
                assert_eq!(h[header::RETRY_AFTER], "2");
            }
        }
    }

    #[test]
    fn retry_after_rounds_up() {
        let res = too_many_requests(Duration::from_millis(10), "x");
        assert_eq!(res.headers()[axum::http::header::RETRY_AFTER], "1");
        let res = too_many_requests(Duration::from_secs(3), "x");
        assert_eq!(res.headers()[axum::http::header::RETRY_AFTER], "3");
    }

    #[test]
    fn private_networks_are_internal() {
        for ip in [
            "10.1.2.3",
            "100.64.0.9",
            "100.127.255.1",
            "::ffff:10.0.0.4",
            "::ffff:100.100.1.1",
            "fd12:3456::1",
            "fe80::1",
            "127.0.0.1",
        ] {
            assert!(is_internal(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["100.128.0.1", "8.8.8.8", "::ffff:8.8.8.8", "2001:db8::1"] {
            assert!(!is_internal(ip.parse().unwrap()), "{ip}");
        }
    }
}
