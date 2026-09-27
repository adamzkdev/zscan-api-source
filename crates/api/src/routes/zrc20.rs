//! ZRC-20 tokens carried by inscriptions, under zord's rules: lowercased 4–5 byte tickers, first
//! deploy wins, mints up to the per-mint limit and never past the max supply, transfers commit
//! the amount until the transfer inscription's output is spent.

use axum::{
    extract::{Path, Query, State},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use zscan_chain::inscriptions::format_amount;

use crate::error::{ApiError, ApiResult};
use crate::AppState;

/// The rule set these figures follow.
pub const RULES: &str = "zord";

fn amount(v: &str, dec: i16) -> String {
    format_amount(v.parse().unwrap_or(0), dec as u8)
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Token {
    pub tick: String,
    /// Amounts are decimal strings.
    pub max: String,
    pub lim: String,
    pub dec: i16,
    pub minted: String,
    /// Share of `max` minted, 0–100.
    pub progress: f64,
    /// Sent to an output without an address, or into a transaction without a successor.
    pub burned: String,
    /// Transfers delivered into shielded transactions (tracking ends there).
    pub shielded: String,
    pub mints: i64,
    pub holders: i32,
    pub deployer: String,
    pub deploy_inscription: String,
    pub deploy_txid: String,
    pub height: i32,
    pub time: DateTime<Utc>,
}

type TokenRow = (
    String,
    String,
    String,
    i16,
    String,
    String,
    String,
    i64,
    i32,
    String,
    String,
    String,
    i32,
    DateTime<Utc>,
    f64,
);

const TOKEN_COLUMNS: &str = "tick, max::text, lim::text, dec, minted::text, burned::text,
    shielded::text, mints, holders, deployer, deploy_inscription, deploy_txid, height, time,
    CASE WHEN max > 0 THEN (minted * 100 / max)::float8 ELSE 0 END";

fn token(r: TokenRow) -> Token {
    Token {
        tick: r.0,
        max: amount(&r.1, r.3),
        lim: amount(&r.2, r.3),
        minted: amount(&r.4, r.3),
        burned: amount(&r.5, r.3),
        shielded: amount(&r.6, r.3),
        dec: r.3,
        mints: r.7,
        holders: r.8,
        deployer: r.9,
        deploy_inscription: r.10,
        deploy_txid: r.11,
        height: r.12,
        time: r.13,
        progress: r.14,
    }
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct TokensQuery {
    /// `holders` (default), `mints`, `progress` or `recent` (deploy time).
    pub sort: Option<String>,
    /// Ticker prefix.
    pub q: Option<String>,
    /// Zero-based page.
    pub page: Option<i64>,
    /// Max items (default 50, max 200).
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TokenList {
    /// The ZRC-20 rule set applied (`zord`).
    pub rules: String,
    pub total: i64,
    pub tokens: Vec<Token>,
}

/// Deployed ZRC-20 tokens.
#[utoipa::path(get, path = "/v1/zrc20", params(TokensQuery),
    responses((status = 200, body = TokenList)), tag = "inscriptions")]
pub async fn list(State(s): State<AppState>, Query(q): Query<TokensQuery>) -> ApiResult<TokenList> {
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let offset = q.page.unwrap_or(0).clamp(0, 10_000) * limit;
    let order = match q.sort.as_deref().unwrap_or("holders") {
        "holders" => "holders DESC, mints DESC",
        "mints" => "mints DESC",
        "progress" => "minted * 1.0 / NULLIF(max, 0) DESC NULLS LAST, holders DESC",
        "recent" => "height DESC",
        other => {
            return Err(ApiError::BadRequest(
                "invalid_sort",
                format!("Unknown sort `{other}`; expected holders, mints, progress or recent"),
            ))
        }
    };
    let prefix = q.q.as_deref().unwrap_or("").trim().to_lowercase();
    let rows: Vec<TokenRow> = sqlx::query_as(&format!(
        "SELECT {TOKEN_COLUMNS} FROM zrc20_tokens WHERE tick LIKE $1 || '%'
         ORDER BY {order}, tick LIMIT $2 OFFSET $3"
    ))
    .bind(&prefix)
    .bind(limit)
    .bind(offset)
    .fetch_all(&s.db.pool)
    .await?;
    let total: i64 =
        sqlx::query_scalar("SELECT count(*) FROM zrc20_tokens WHERE tick LIKE $1 || '%'")
            .bind(&prefix)
            .fetch_one(&s.db.pool)
            .await?;
    Ok(Json(TokenList {
        rules: RULES.into(),
        total,
        tokens: rows.into_iter().map(token).collect(),
    }))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Holding {
    pub tick: String,
    pub balance: String,
    pub available: String,
    /// Committed by transfer inscriptions not yet sent.
    pub transferable: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Holder {
    pub address: String,
    pub balance: String,
    pub available: String,
    pub transferable: String,
    /// Share of the minted supply, 0–100.
    pub share: f64,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct ZrcEvent {
    pub tick: String,
    /// deploy, mint, transfer (inscribed), send, shielded, burn (settled), invalid.
    pub op: String,
    pub inscription_id: String,
    pub inscription_number: Option<i64>,
    pub txid: String,
    pub height: i32,
    pub time: DateTime<Utc>,
    pub from_addr: Option<String>,
    pub to_addr: Option<String>,
    /// Decimal string.
    pub amount: Option<String>,
    pub valid: bool,
    /// Why an operation did not count.
    pub reason: Option<String>,
}

/// Events matching `cond` (with `$1` bound to `arg`), newest first.
pub async fn events_where(
    s: &AppState,
    cond: &str,
    arg: &str,
    limit: i64,
) -> anyhow::Result<Vec<ZrcEvent>> {
    let rows: Vec<(ZrcEvent, Option<i16>)> = sqlx::query_as::<_, EventRow>(&format!(
        "SELECT e.tick, e.op, e.inscription_id, i.number AS inscription_number, e.txid, e.height,
                e.time, e.from_addr, e.to_addr, e.amount::text AS amount, e.valid, e.reason,
                t.dec
         FROM zrc20_events e
         LEFT JOIN inscriptions i ON i.id = e.inscription_id
         LEFT JOIN zrc20_tokens t ON t.tick = e.tick
         WHERE {cond} ORDER BY e.id DESC LIMIT {limit}"
    ))
    .bind(arg)
    .fetch_all(&s.db.pool)
    .await?
    .into_iter()
    .map(|r| (r.event, r.dec))
    .collect();
    Ok(rows
        .into_iter()
        .map(|(mut e, dec)| {
            e.amount = e.amount.map(|a| amount(&a, dec.unwrap_or(0)));
            e
        })
        .collect())
}

#[derive(sqlx::FromRow)]
struct EventRow {
    #[sqlx(flatten)]
    event: ZrcEvent,
    dec: Option<i16>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct TokenQuery {
    /// Holders page (zero-based, 50 per page).
    pub page: Option<i64>,
    /// Include operations that did not count.
    pub invalid: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TokenDetail {
    pub rules: String,
    #[serde(flatten)]
    pub token: Token,
    pub holders_list: Vec<Holder>,
    /// Latest 100 operations.
    pub events: Vec<ZrcEvent>,
    /// Operations that did not count.
    pub invalid_count: i64,
}

/// One ZRC-20 token: supply, holders and activity.
#[utoipa::path(get, path = "/v1/zrc20/{tick}", params(("tick" = String, Path), TokenQuery),
    responses((status = 200, body = TokenDetail), (status = 404)), tag = "inscriptions")]
pub async fn detail(
    State(s): State<AppState>,
    Path(tick): Path<String>,
    Query(q): Query<TokenQuery>,
) -> ApiResult<TokenDetail> {
    let tick = tick.to_lowercase();
    let db = &s.db.pool;
    let row: Option<TokenRow> = sqlx::query_as(&format!(
        "SELECT {TOKEN_COLUMNS} FROM zrc20_tokens WHERE tick = $1"
    ))
    .bind(&tick)
    .fetch_optional(db)
    .await?;
    let token = token(row.ok_or_else(|| ApiError::NotFound(format!("Token {tick} not found")))?);
    let page = q.page.unwrap_or(0).clamp(0, 10_000);
    let holders: Vec<(String, String, String, f64)> = sqlx::query_as(
        "SELECT b.address, b.available::text, b.transferable::text,
                CASE WHEN t.minted > 0
                     THEN ((b.available + b.transferable) * 100 / t.minted)::float8 ELSE 0 END
         FROM zrc20_balances b JOIN zrc20_tokens t USING (tick)
         WHERE b.tick = $1 ORDER BY b.available + b.transferable DESC, b.address
         LIMIT 50 OFFSET $2",
    )
    .bind(&tick)
    .bind(page * 50)
    .fetch_all(db)
    .await?;
    let holders_list = holders
        .into_iter()
        .map(|(address, a, t, share)| {
            let (av, tr): (u128, u128) = (a.parse().unwrap_or(0), t.parse().unwrap_or(0));
            Holder {
                address,
                balance: format_amount(av + tr, token.dec as u8),
                available: format_amount(av, token.dec as u8),
                transferable: format_amount(tr, token.dec as u8),
                share,
            }
        })
        .collect();
    let cond = if q.invalid.unwrap_or(false) {
        "e.tick = $1"
    } else {
        "e.tick = $1 AND e.valid"
    };
    let events = events_where(&s, cond, &tick, 100).await?;
    let invalid_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM zrc20_events WHERE tick = $1 AND NOT valid")
            .bind(&tick)
            .fetch_one(db)
            .await?;
    Ok(Json(TokenDetail {
        rules: RULES.into(),
        token,
        holders_list,
        events,
        invalid_count,
    }))
}

/// ZRC-20 operations in a transaction.
pub async fn for_tx(s: &AppState, txid: &str) -> anyhow::Result<Vec<ZrcEvent>> {
    events_where(s, "e.txid = $1", txid, 100).await
}
