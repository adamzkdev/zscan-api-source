//! Cross-chain flows: ZEC moving between Zcash and other chains through Maya Protocol swaps and
//! NEAR Intents, as reported by each protocol's public API (see the indexer's `bridges` task).

use axum::extract::{Query, State};
use axum::Json;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::error::ApiResult;
use crate::AppState;

/// Protocols tracked here, with display names.
const PROTOCOLS: [(&str, &str); 2] = [("near-intents", "NEAR Intents"), ("maya", "Maya Protocol")];

#[derive(Debug, Serialize, ToSchema)]
pub struct BridgeHolding {
    pub protocol: String,
    pub name: String,
    /// ZEC the bridge holds for other chains, zatoshis (NEAR: zec.omft.near supply; Maya: pool depth).
    pub amount: Option<i64>,
    pub change_1d: Option<i64>,
    pub change_7d: Option<i64>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct BridgeVolume {
    pub protocol: String,
    /// 24h | 7d | 30d | 90d
    pub window: String,
    /// ZEC that arrived on Zcash, zatoshis.
    pub zec_in: i64,
    /// ZEC that left Zcash, zatoshis.
    pub zec_out: i64,
    pub usd: Option<f64>,
    pub count: i64,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct ChainVolume {
    /// The other chain (ETH, BTC, NEAR, THOR…).
    pub chain: String,
    pub zec_in: i64,
    pub zec_out: i64,
    pub count: i64,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct BridgeDay {
    pub day: NaiveDate,
    pub protocol: String,
    pub zec_in: i64,
    pub zec_out: i64,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct BridgeHoldingDay {
    pub day: NaiveDate,
    pub protocol: String,
    /// End-of-day holdings, zatoshis.
    pub amount: i64,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct BridgeEventRow {
    pub protocol: String,
    /// Relative to Zcash: `in` = ZEC arrived on Zcash, `out` = ZEC left Zcash.
    pub direction: String,
    /// ZEC moved, zatoshis.
    pub amount: i64,
    pub counter_asset: String,
    pub counter_chain: String,
    pub usd: Option<f64>,
    pub zcash_txid: Option<String>,
    pub external_url: Option<String>,
    pub status: String,
    pub time: DateTime<Utc>,
    /// Amount of `counter_asset` on the other side, in its own units (NEAR Intents).
    pub counter_amount: Option<f64>,
    /// The Zcash address ZEC went to (in) or is refunded to (out), when reported.
    pub zcash_address: Option<String>,
    /// transparent | unified | sapling
    pub address_kind: Option<String>,
    /// The app or wallet that routed the swap (NEAR Intents referral).
    pub referral: Option<String>,
}

/// Columns of `BridgeEventRow`.
const EVENT_COLS: &str =
    "protocol, direction, amount, counter_asset, counter_chain, usd, zcash_txid,
    external_url, status, time, counter_amount, zcash_address, address_kind, referral";

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct ShieldedShare {
    /// ZEC delivered to Zcash through NEAR Intents over 30 days to a known address, zatoshis.
    pub zec_in: i64,
    /// The part of it sent to a shielded-capable (unified or Sapling) address.
    pub zec_in_shielded: i64,
    pub count: i64,
    pub count_shielded: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct BridgesOverview {
    pub holdings: Vec<BridgeHolding>,
    /// Event volumes per protocol over 24h, 7d, 30d and 90d.
    pub volumes: Vec<BridgeVolume>,
    /// 90-day volumes per counter chain, largest first.
    pub by_chain: Vec<ChainVolume>,
    /// Daily event volumes per protocol, last 90 days.
    pub daily: Vec<BridgeDay>,
    /// Daily holdings per protocol, last 90 days.
    pub holdings_daily: Vec<BridgeHoldingDay>,
    /// Latest 50 events.
    pub recent: Vec<BridgeEventRow>,
    /// Oldest stored event: volumes cover events from here on.
    pub since: Option<DateTime<Utc>>,
    /// How much ZEC swapped in through NEAR Intents lands on shielded-capable addresses (30 days).
    pub near_shielded: ShieldedShare,
}

/// ZEC crossing chains: bridge holdings, volumes by protocol and chain, daily series and the
/// latest swaps and bridge transfers.
#[utoipa::path(get, path = "/v1/bridges",
    responses((status = 200, body = BridgesOverview)), tag = "protocols")]
pub async fn overview(State(s): State<AppState>) -> ApiResult<BridgesOverview> {
    let db = &s.db.pool;
    let mut holdings = Vec::new();
    for (slug, name) in PROTOCOLS {
        let rows: Vec<(NaiveDate, i64, DateTime<Utc>)> = sqlx::query_as(
            "SELECT day, amount, updated_at FROM bridge_snapshots WHERE protocol = $1
             ORDER BY day DESC LIMIT 8",
        )
        .bind(slug)
        .fetch_all(db)
        .await?;
        let first = rows.first();
        let ago = |days: i64| {
            let (d0, a0, _) = first?;
            let (_, a, _) = rows
                .iter()
                .find(|(d, _, _)| (*d0 - *d).num_days() >= days)?;
            Some(a0 - a)
        };
        holdings.push(BridgeHolding {
            protocol: slug.into(),
            name: name.into(),
            amount: first.map(|r| r.1),
            change_1d: ago(1),
            change_7d: ago(7),
            updated_at: first.map(|r| r.2),
        });
    }

    let volumes = sqlx::query_as::<_, BridgeVolume>(
        "SELECT e.protocol, w.name AS window,
                COALESCE(sum(amount) FILTER (WHERE direction = 'in'), 0)::bigint AS zec_in,
                COALESCE(sum(amount) FILTER (WHERE direction = 'out'), 0)::bigint AS zec_out,
                sum(usd) AS usd, count(*) AS count
         FROM (VALUES ('24h', interval '1 day'), ('7d', interval '7 days'), ('30d', interval '30 days'), ('90d', interval '90 days'))
              AS w(name, span)
         JOIN bridge_events e ON e.time >= now() - w.span
         GROUP BY 1, 2 ORDER BY 1, min(w.span)",
    )
    .fetch_all(db)
    .await?;

    let by_chain = sqlx::query_as::<_, ChainVolume>(
        "SELECT counter_chain AS chain,
                COALESCE(sum(amount) FILTER (WHERE direction = 'in'), 0)::bigint AS zec_in,
                COALESCE(sum(amount) FILTER (WHERE direction = 'out'), 0)::bigint AS zec_out,
                count(*) AS count
         FROM bridge_events WHERE time >= now() - interval '90 days'
         GROUP BY 1 ORDER BY sum(amount) DESC",
    )
    .fetch_all(db)
    .await?;

    let daily = sqlx::query_as::<_, BridgeDay>(
        "SELECT (time AT TIME ZONE 'UTC')::date AS day, protocol,
                COALESCE(sum(amount) FILTER (WHERE direction = 'in'), 0)::bigint AS zec_in,
                COALESCE(sum(amount) FILTER (WHERE direction = 'out'), 0)::bigint AS zec_out
         FROM bridge_events WHERE time >= now() - interval '90 days'
         GROUP BY 1, 2 ORDER BY 1, 2",
    )
    .fetch_all(db)
    .await?;

    let holdings_daily = sqlx::query_as::<_, BridgeHoldingDay>(
        "SELECT day, protocol, amount FROM bridge_snapshots
         WHERE day >= (now() AT TIME ZONE 'UTC')::date - 90 ORDER BY day, protocol",
    )
    .fetch_all(db)
    .await?;

    let recent = sqlx::query_as::<_, BridgeEventRow>(&format!(
        "SELECT {EVENT_COLS} FROM bridge_events ORDER BY time DESC LIMIT 50"
    ))
    .fetch_all(db)
    .await?;

    let near_shielded = sqlx::query_as::<_, ShieldedShare>(
        "SELECT COALESCE(sum(amount), 0)::bigint AS zec_in,
                COALESCE(sum(amount) FILTER (WHERE address_kind <> 'transparent'), 0)::bigint AS zec_in_shielded,
                count(*) AS count,
                count(*) FILTER (WHERE address_kind <> 'transparent') AS count_shielded
         FROM bridge_events
         WHERE protocol = 'near-intents' AND direction = 'in' AND address_kind IS NOT NULL
           AND status = 'success' AND time >= now() - interval '30 days'",
    )
    .fetch_one(db)
    .await?;

    let since: Option<DateTime<Utc>> = sqlx::query_scalar("SELECT min(time) FROM bridge_events")
        .fetch_one(db)
        .await?;

    Ok(Json(BridgesOverview {
        holdings,
        volumes,
        by_chain,
        daily,
        holdings_daily,
        recent,
        since,
        near_shielded,
    }))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct EventsQuery {
    /// Only events moving at least this many ZEC (default 0).
    pub min_zec: Option<f64>,
    /// maya or near-intents.
    pub protocol: Option<String>,
    /// Only events after this time (for polling).
    pub since: Option<DateTime<Utc>>,
    /// in (ZEC arriving on Zcash) or out.
    pub direction: Option<String>,
    /// The other chain: ETH, BTC, SOL…
    pub chain: Option<String>,
    /// Default 50, max 500.
    pub limit: Option<i64>,
}

/// Cross-chain events, newest first: every Maya and NEAR Intents swap moving ZEC to or from Zcash.
/// With `min_zec` it is the live feed of large moves.
#[utoipa::path(get, path = "/v1/bridges/events", params(EventsQuery),
    responses((status = 200, body = [BridgeEventRow])), tag = "protocols")]
pub async fn events(
    State(s): State<AppState>,
    Query(q): Query<EventsQuery>,
) -> ApiResult<Vec<BridgeEventRow>> {
    Ok(Json(
        sqlx::query_as::<_, BridgeEventRow>(&format!(
            "SELECT {EVENT_COLS} FROM bridge_events
             WHERE amount >= $1 AND ($2::text IS NULL OR protocol = $2)
               AND ($3::timestamptz IS NULL OR time > $3)
               AND ($5::text IS NULL OR direction = $5)
               AND ($6::text IS NULL OR counter_chain = upper($6))
             ORDER BY time DESC LIMIT $4"
        ))
        .bind((q.min_zec.unwrap_or(0.0).max(0.0) * 1e8) as i64)
        .bind(q.protocol.as_deref())
        .bind(q.since)
        .bind(q.limit.unwrap_or(50).clamp(1, 500))
        .bind(q.direction.as_deref())
        .bind(q.chain.as_deref())
        .fetch_all(&s.db.pool)
        .await?,
    ))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct FlowsQuery {
    /// Window in hours (default 24, max 2160 = 90 days).
    pub hours: Option<i32>,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct Flow {
    pub protocol: String,
    /// The other side: ETH, BTC, NEAR…
    pub chain: String,
    /// ZEC arriving on Zcash from `chain`, and leaving Zcash for it (zatoshis).
    pub zec_in: i64,
    pub zec_out: i64,
    pub usd: Option<f64>,
    pub count: i64,
}

/// ZEC flows between Zcash and each counter chain over a window, per protocol.
#[utoipa::path(get, path = "/v1/bridges/flows", params(FlowsQuery),
    responses((status = 200, body = [Flow])), tag = "protocols")]
pub async fn flows(State(s): State<AppState>, Query(q): Query<FlowsQuery>) -> ApiResult<Vec<Flow>> {
    Ok(Json(
        sqlx::query_as::<_, Flow>(
            "SELECT protocol, counter_chain AS chain,
                    COALESCE(sum(amount) FILTER (WHERE direction = 'in'), 0)::bigint AS zec_in,
                    COALESCE(sum(amount) FILTER (WHERE direction = 'out'), 0)::bigint AS zec_out,
                    sum(usd) AS usd, count(*) AS count
             FROM bridge_events WHERE time >= now() - make_interval(hours => $1)
             GROUP BY 1, 2 ORDER BY sum(amount) DESC",
        )
        .bind(q.hours.unwrap_or(24).clamp(1, 2160))
        .fetch_all(&s.db.pool)
        .await?,
    ))
}
