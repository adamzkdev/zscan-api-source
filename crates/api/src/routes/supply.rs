//! Supply, issuance, the independent supply audit, on-chain funding and anonymity sets.

use axum::{
    extract::{Query, State},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use zscan_chain::economics::{block_economics, funding_stream_addresses, next_halving};

use crate::error::{ApiError, ApiResult};
use crate::AppState;

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct IssuanceDay {
    pub day: DateTime<Utc>,
    pub blocks: i64,
    /// New ZEC created (zatoshis).
    pub issued: i64,
    pub to_miners: i64,
    pub to_funding: i64,
    pub to_lockbox: i64,
    pub fees: i64,
    pub burned: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AuditSummary {
    pub checks: i64,
    pub failed: i64,
    pub last_height: Option<i32>,
    pub last_checked: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Supply {
    pub height: i32,
    /// Total ZEC in existence per the node (zatoshis).
    pub supply: i64,
    /// Hard cap: 21M ZEC.
    pub max_supply: i64,
    pub lockbox: i64,
    /// Current per-block subsidy split.
    pub block_subsidy: i64,
    pub block_to_miner: i64,
    pub block_to_funding: i64,
    pub block_to_lockbox: i64,
    /// Issuance over the last 24h of blocks.
    pub issued_24h: i64,
    /// Annualized issuance / supply, 0–1.
    pub inflation_rate: f64,
    /// ZEC removed from supply (fee removal under NU7), since the index start.
    pub burned_total: i64,
    pub next_halving_height: Option<u32>,
    pub next_halving_eta: Option<DateTime<Utc>>,
    pub avg_block_time: Option<f64>,
    pub audit: AuditSummary,
    pub daily: Vec<IssuanceDay>,
}

const MAX_SUPPLY: i64 = 21_000_000 * 100_000_000;

/// Supply, issuance schedule and fee burn, computed from Zcash consensus rules.
#[utoipa::path(get, path = "/v1/supply", responses((status = 200, body = Supply)), tag = "supply")]
pub async fn supply(State(s): State<AppState>) -> ApiResult<Supply> {
    let snap: Option<(i32, i64, i64)> = sqlx::query_as(
        "SELECT height, supply, lockbox FROM pool_snapshots ORDER BY height DESC LIMIT 1",
    )
    .fetch_optional(&s.db.pool)
    .await?;
    let Some((height, supply, lockbox)) = snap else {
        return Err(ApiError::NotFound("No supply snapshot yet".into()));
    };
    let net = zscan_chain::network();
    let econ = block_economics(height as u32 + 1, net);

    let (issued_24h, burned_total): (Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT (SELECT sum(subsidy - COALESCE(burned, 0))::bigint FROM blocks
                 WHERE time > (SELECT max(time) FROM blocks) - interval '24 hours'),
                (SELECT sum(burned)::bigint FROM blocks)",
    )
    .fetch_one(&s.db.pool)
    .await?;
    let avg_block_time: Option<f64> = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM (max(time) - min(time)))::float8 / NULLIF(count(*) - 1, 0)
         FROM (SELECT time FROM blocks ORDER BY height DESC LIMIT 200) b",
    )
    .fetch_one(&s.db.pool)
    .await?;
    let next = next_halving(height as u32, net);
    let eta = next.zip(avg_block_time).map(|(h, t)| {
        Utc::now() + chrono::Duration::seconds(((h - height as u32) as f64 * t) as i64)
    });

    let daily = sqlx::query_as::<_, IssuanceDay>(
        "SELECT date_trunc('day', time) AS day, count(*) AS blocks,
                sum(subsidy)::bigint AS issued,
                sum(subsidy - funding_paid - deferred)::bigint AS to_miners,
                sum(funding_paid)::bigint AS to_funding,
                sum(deferred)::bigint AS to_lockbox,
                COALESCE(sum(fees), 0)::bigint AS fees,
                COALESCE(sum(burned), 0)::bigint AS burned
         FROM blocks WHERE subsidy IS NOT NULL GROUP BY 1 ORDER BY 1",
    )
    .fetch_all(&s.db.pool)
    .await?;

    let issued = issued_24h.unwrap_or(0);
    Ok(Json(Supply {
        height,
        supply,
        max_supply: MAX_SUPPLY,
        lockbox,
        block_subsidy: econ.subsidy,
        block_to_miner: econ.miner_subsidy,
        block_to_funding: econ.funding_streams,
        block_to_lockbox: econ.deferred,
        issued_24h: issued,
        inflation_rate: if supply > 0 {
            issued as f64 * 365.0 / supply as f64
        } else {
            0.0
        },
        burned_total: burned_total.unwrap_or(0),
        next_halving_height: next,
        next_halving_eta: eta,
        avg_block_time,
        audit: audit_summary(&s).await?,
        daily,
    }))
}

async fn audit_summary(s: &AppState) -> Result<AuditSummary, ApiError> {
    let (checks, failed, last_height, last_checked): (i64, i64, Option<i32>, Option<DateTime<Utc>>) = sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE NOT ok), max(height), max(checked_at) FROM audit_checks",
    )
    .fetch_one(&s.db.pool)
    .await?;
    Ok(AuditSummary {
        checks,
        failed,
        last_height,
        last_checked,
    })
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct AuditCheck {
    pub height: i32,
    pub prev_height: i32,
    pub checked_at: DateTime<Utc>,
    pub diff_transparent: i64,
    pub diff_sprout: i64,
    pub diff_sapling: i64,
    pub diff_orchard: i64,
    pub diff_ironwood: i64,
    pub diff_lockbox: i64,
    pub diff_supply: i64,
    pub ok: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Audit {
    pub summary: AuditSummary,
    /// How the check works, for independent reproduction.
    pub method: &'static str,
    pub checks: Vec<AuditCheck>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct AuditQuery {
    /// Max checks (default 50, max 500).
    pub limit: Option<i64>,
    /// Only failed checks.
    pub failed: Option<bool>,
}

/// Independent supply audit: node pool balances vs. decoded block-by-block deltas.
#[utoipa::path(get, path = "/v1/supply/audit", params(AuditQuery), responses((status = 200, body = Audit)), tag = "supply")]
pub async fn audit(State(s): State<AppState>, Query(q): Query<AuditQuery>) -> ApiResult<Audit> {
    let checks = sqlx::query_as::<_, AuditCheck>(
        "SELECT * FROM audit_checks WHERE ($1::bool IS NOT TRUE OR NOT ok) ORDER BY height DESC LIMIT $2",
    )
    .bind(q.failed)
    .bind(q.limit.unwrap_or(50).clamp(1, 500))
    .fetch_all(&s.db.pool)
    .await?;
    Ok(Json(Audit {
        summary: audit_summary(&s).await?,
        method: "For each pair of consecutive node snapshots (getblockchaininfo.valuePools), the change of \
                 every pool must equal the sum of that pool's deltas decoded from each block in between \
                 (transparent outputs minus resolved inputs; minus each shielded value balance). Lockbox change \
                 must equal the deferred funding-stream amounts, and supply change must equal block subsidies \
                 minus removed fees, both from Zcash consensus rules. Any non-zero difference fails the check.",
        checks,
    }))
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct FundingDay {
    pub day: DateTime<Utc>,
    /// Paid to funding-stream addresses (Zcash Community Grants).
    pub funding: i64,
    /// Deferred to the lockbox / coinholder-controlled fund.
    pub deferred: i64,
    pub blocks: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Funding {
    pub height: i32,
    pub lockbox_balance: i64,
    /// Current recipient addresses of the ZCG funding stream.
    pub recipients: Vec<String>,
    pub per_block_funding: i64,
    pub per_block_deferred: i64,
    pub total_funding: i64,
    pub total_deferred: i64,
    pub indexed_from: Option<i32>,
    pub daily: Vec<FundingDay>,
}

/// Development fund flows decoded from coinbase transactions (ZIP 1015 / 1016).
#[utoipa::path(get, path = "/v1/funding", responses((status = 200, body = Funding)), tag = "supply")]
pub async fn funding(State(s): State<AppState>) -> ApiResult<Funding> {
    let (height, lockbox): (i32, i64) =
        sqlx::query_as("SELECT height, lockbox FROM pool_snapshots ORDER BY height DESC LIMIT 1")
            .fetch_optional(&s.db.pool)
            .await?
            .ok_or_else(|| ApiError::NotFound("No snapshot yet".into()))?;
    let net = zscan_chain::network();
    let econ = block_economics(height as u32, net);
    let mut recipients: Vec<String> = funding_stream_addresses(height as u32, net)
        .into_iter()
        .collect();
    recipients.sort();
    let daily = sqlx::query_as::<_, FundingDay>(
        "SELECT date_trunc('day', time) AS day, sum(funding_paid)::bigint AS funding,
                sum(deferred)::bigint AS deferred, count(*) AS blocks
         FROM blocks WHERE subsidy IS NOT NULL GROUP BY 1 ORDER BY 1",
    )
    .fetch_all(&s.db.pool)
    .await?;
    let low: Option<i32> = sqlx::query_scalar("SELECT low FROM sync_state")
        .fetch_one(&s.db.pool)
        .await?;
    Ok(Json(Funding {
        height,
        lockbox_balance: lockbox,
        recipients,
        per_block_funding: econ.funding_streams,
        per_block_deferred: econ.deferred,
        total_funding: daily.iter().map(|d| d.funding).sum(),
        total_deferred: daily.iter().map(|d| d.deferred).sum(),
        indexed_from: low,
        daily,
    }))
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct AnonymityDay {
    pub day: DateTime<Utc>,
    /// Notes in each shielded pool's commitment tree at the end of the day.
    pub sapling: Option<i64>,
    pub orchard: Option<i64>,
    pub ironwood: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Anonymity {
    pub height: i32,
    pub sapling: i64,
    pub orchard: i64,
    pub ironwood: i64,
    /// Notes added in the last 24h, per pool.
    pub sapling_24h: i64,
    pub orchard_24h: i64,
    pub ironwood_24h: i64,
    pub daily: Vec<AnonymityDay>,
}

/// Anonymity sets: how many notes each shielded pool holds, and how fast they grow.
#[utoipa::path(get, path = "/v1/privacy/anonymity-sets", responses((status = 200, body = Anonymity)), tag = "supply")]
pub async fn anonymity(State(s): State<AppState>) -> ApiResult<Anonymity> {
    let now: Option<(i32, i64, i64, i64)> = sqlx::query_as(
        "SELECT height, sapling_tree_size::bigint, orchard_tree_size::bigint, ironwood_tree_size::bigint
         FROM blocks WHERE ironwood_tree_size IS NOT NULL ORDER BY height DESC LIMIT 1",
    )
    .fetch_optional(&s.db.pool)
    .await?;
    let (height, sapling, orchard, ironwood) =
        now.ok_or_else(|| ApiError::NotFound("No blocks yet".into()))?;
    let day_ago: Option<(i64, i64, i64)> = sqlx::query_as(
        "SELECT sapling_tree_size::bigint, orchard_tree_size::bigint, ironwood_tree_size::bigint FROM blocks
         WHERE time <= (SELECT max(time) FROM blocks) - interval '24 hours' ORDER BY height DESC LIMIT 1",
    )
    .fetch_optional(&s.db.pool)
    .await?;
    let daily = sqlx::query_as::<_, AnonymityDay>(
        "SELECT date_trunc('day', time) AS day, max(sapling_tree_size)::bigint AS sapling,
                max(orchard_tree_size)::bigint AS orchard, max(ironwood_tree_size)::bigint AS ironwood
         FROM blocks GROUP BY 1 ORDER BY 1",
    )
    .fetch_all(&s.db.pool)
    .await?;
    let (ps, po, pi) = day_ago.unwrap_or((sapling, orchard, ironwood));
    Ok(Json(Anonymity {
        height,
        sapling,
        orchard,
        ironwood,
        sapling_24h: sapling - ps,
        orchard_24h: orchard - po,
        ironwood_24h: ironwood - pi,
        daily,
    }))
}
