//! Value pools and the Orchard → Ironwood migration.
//!
//! Historical balances are derived exactly: starting from the latest node snapshot inside
//! the indexed range, we subtract the per-block pool deltas computed by the indexer.

use axum::{
    extract::{Query, State},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use zscan_chain::consts::nu6_3_height;

use crate::error::{ApiError, ApiResult};
use crate::models::PoolValues;
use crate::AppState;

#[derive(Debug, sqlx::FromRow)]
struct Snapshot {
    height: i32,
    time: DateTime<Utc>,
    transparent: i64,
    sprout: i64,
    sapling: i64,
    orchard: i64,
    ironwood: i64,
    lockbox: i64,
    supply: i64,
}

/// Latest node snapshot at or below the indexed tip, so derived history lines up with it.
const ANCHOR: &str = "SELECT * FROM pool_snapshots
    WHERE height <= COALESCE((SELECT high FROM sync_state), 0) ORDER BY height DESC LIMIT 1";

async fn anchor(s: &AppState) -> Result<Snapshot, ApiError> {
    let snap = sqlx::query_as::<_, Snapshot>(ANCHOR)
        .fetch_optional(&s.db.pool)
        .await?;
    match snap {
        Some(a) => Ok(a),
        None => sqlx::query_as::<_, Snapshot>(
            "SELECT * FROM pool_snapshots ORDER BY height DESC LIMIT 1",
        )
        .fetch_optional(&s.db.pool)
        .await?
        .ok_or_else(|| ApiError::NotFound("No pool snapshot yet".into())),
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PoolsCurrent {
    pub height: i32,
    pub time: DateTime<Utc>,
    pub pools: PoolValues,
    /// Deferred development fund lockbox (NU6).
    pub lockbox: i64,
    pub supply: i64,
    pub shielded: i64,
    /// Share of supply held in shielded pools, 0–1.
    pub shielded_ratio: f64,
    /// Pool changes over the last 24 hours.
    pub change_24h: PoolValues,
}

/// Current balances of every value pool.
#[utoipa::path(get, path = "/v1/pools", responses((status = 200, body = PoolsCurrent)), tag = "pools")]
pub async fn current(State(s): State<AppState>) -> ApiResult<PoolsCurrent> {
    let a = anchor(&s).await?;
    let (dt, dsp, dsa, dor, diw): (Option<i64>, Option<i64>, Option<i64>, Option<i64>, Option<i64>) =
        sqlx::query_as(
            "SELECT sum(d_transparent)::bigint, sum(d_sprout)::bigint, sum(d_sapling)::bigint,
                    sum(d_orchard)::bigint, sum(d_ironwood)::bigint
             FROM blocks WHERE height <= $1 AND time > (SELECT time FROM blocks WHERE height = $1) - interval '24 hours'",
        )
        .bind(a.height)
        .fetch_one(&s.db.pool)
        .await?;
    let shielded = a.sprout + a.sapling + a.orchard + a.ironwood;
    Ok(Json(PoolsCurrent {
        height: a.height,
        time: a.time,
        pools: PoolValues {
            transparent: a.transparent,
            sprout: a.sprout,
            sapling: a.sapling,
            orchard: a.orchard,
            ironwood: a.ironwood,
        },
        lockbox: a.lockbox,
        supply: a.supply,
        shielded,
        shielded_ratio: if a.supply > 0 {
            shielded as f64 / a.supply as f64
        } else {
            0.0
        },
        change_24h: PoolValues {
            transparent: dt.unwrap_or(0),
            sprout: dsp.unwrap_or(0),
            sapling: dsa.unwrap_or(0),
            orchard: dor.unwrap_or(0),
            ironwood: diw.unwrap_or(0),
        },
    }))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct HistoryQuery {
    /// `hour` or `day` (default).
    pub interval: Option<String>,
    /// Only points after this time.
    pub since: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct PoolPoint {
    /// End of the bucket.
    pub time: DateTime<Utc>,
    /// Last block height in the bucket.
    pub height: i32,
    pub transparent: i64,
    pub sprout: i64,
    pub sapling: i64,
    pub orchard: i64,
    pub ironwood: i64,
}

/// Pool balances over time, derived block by block over the indexed range.
#[utoipa::path(get, path = "/v1/pools/history", params(HistoryQuery),
    responses((status = 200, body = [PoolPoint])), tag = "pools")]
pub async fn history(
    State(s): State<AppState>,
    Query(q): Query<HistoryQuery>,
) -> ApiResult<Vec<PoolPoint>> {
    let interval = match q.interval.as_deref() {
        None | Some("day") => "day",
        Some("hour") => "hour",
        Some(_) => {
            return Err(ApiError::BadRequest(
                "invalid_interval",
                "interval must be hour or day".into(),
            ))
        }
    };
    let a = anchor(&s).await?;
    let rows = sqlx::query_as::<_, PoolPoint>(
        "WITH per AS (
            SELECT date_trunc($1, time) AS bucket, max(height) AS height, max(time) AS time,
                   sum(d_transparent) dt, sum(d_sprout) dsp, sum(d_sapling) dsa,
                   sum(d_orchard) dor, sum(d_ironwood) diw
            FROM blocks WHERE height <= $2 GROUP BY 1
         ), cum AS (
            SELECT bucket, height, time,
                   COALESCE(sum(dt) OVER w, 0) lt, COALESCE(sum(dsp) OVER w, 0) lsp,
                   COALESCE(sum(dsa) OVER w, 0) lsa, COALESCE(sum(dor) OVER w, 0) lor,
                   COALESCE(sum(diw) OVER w, 0) liw
            FROM per
            WINDOW w AS (ORDER BY bucket DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
         )
         SELECT time, height,
                ($3 - lt)::bigint AS transparent, ($4 - lsp)::bigint AS sprout,
                ($5 - lsa)::bigint AS sapling, ($6 - lor)::bigint AS orchard, ($7 - liw)::bigint AS ironwood
         FROM cum WHERE ($8::timestamptz IS NULL OR time >= $8) ORDER BY bucket",
    )
    .bind(interval)
    .bind(a.height)
    .bind(a.transparent)
    .bind(a.sprout)
    .bind(a.sapling)
    .bind(a.orchard)
    .bind(a.ironwood)
    .bind(q.since)
    .fetch_all(&s.db.pool)
    .await?;
    Ok(Json(rows))
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct MigrationDay {
    pub day: DateTime<Utc>,
    /// Value that left Orchard (zatoshis).
    pub orchard_out: i64,
    /// Net value that entered Ironwood (zatoshis).
    pub ironwood_in: i64,
    pub migration_txs: i64,
    /// Where value leaving Orchard went (zatoshis).
    pub exit_to_ironwood: i64,
    pub exit_to_sapling: i64,
    pub exit_to_transparent: i64,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct MigrationTx {
    pub txid: String,
    pub height: i32,
    pub time: DateTime<Utc>,
    /// Value leaving Orchard in this transaction.
    pub amount: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Migration {
    pub activation_height: u32,
    pub snapshot_height: i32,
    pub orchard_now: i64,
    pub ironwood_now: i64,
    /// Orchard balance at activation, derived from indexed deltas (null until backfill completes).
    pub orchard_at_activation: Option<i64>,
    /// Derived Ironwood balance just before activation; must be exactly 0 (integrity check).
    pub ironwood_before_activation: Option<i64>,
    pub migrated: Option<i64>,
    /// Share of the Orchard balance at activation that has left Orchard, 0–1.
    pub migrated_ratio: Option<f64>,
    /// Lowest indexed height; history below it is not covered.
    pub indexed_from: Option<i32>,
    pub daily: Vec<MigrationDay>,
    pub top: Vec<MigrationTx>,
    pub exits: OrchardExits,
    pub denominations: Vec<Denomination>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OrchardExits {
    /// Totals since activation (within the indexed range), zatoshis.
    pub to_ironwood: i64,
    pub to_sapling: i64,
    pub to_transparent: i64,
    pub fees: i64,
    pub txs: i64,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct Denomination {
    /// Amount entering Ironwood in a migration transaction (zatoshis).
    pub amount: i64,
    pub count: i64,
}

/// Orchard → Ironwood migration progress (NU6.3).
#[utoipa::path(get, path = "/v1/ironwood/migration", responses((status = 200, body = Migration)), tag = "pools")]
pub async fn migration(State(s): State<AppState>) -> ApiResult<Migration> {
    let a = anchor(&s).await?;
    let low: Option<i32> = sqlx::query_scalar("SELECT low FROM sync_state")
        .fetch_one(&s.db.pool)
        .await?;
    let complete = low.is_some_and(|l| l <= nu6_3_height() as i32);

    let (dor, diw): (Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT sum(d_orchard)::bigint, sum(d_ironwood)::bigint FROM blocks WHERE height >= $1 AND height <= $2",
    )
    .bind(nu6_3_height() as i32)
    .bind(a.height)
    .fetch_one(&s.db.pool)
    .await?;
    let orchard_at_activation = complete.then(|| a.orchard - dor.unwrap_or(0));
    let ironwood_before_activation = complete.then(|| a.ironwood - diw.unwrap_or(0));
    let migrated = orchard_at_activation.map(|o| o - a.orchard);

    let daily = sqlx::query_as::<_, MigrationDay>(
        "SELECT date_trunc('day', b.time) AS day,
                (-sum(b.d_orchard))::bigint AS orchard_out,
                sum(b.d_ironwood)::bigint AS ironwood_in,
                COALESCE(sum(m.n), 0)::bigint AS migration_txs,
                COALESCE(sum(m.iw), 0)::bigint AS exit_to_ironwood,
                COALESCE(sum(m.sa), 0)::bigint AS exit_to_sapling,
                COALESCE(sum(m.tr), 0)::bigint AS exit_to_transparent
         FROM blocks b
         LEFT JOIN (
            SELECT height, count(*) FILTER (WHERE kind = 'migration') n,
                   sum(GREATEST(-ironwood_vb, 0)) iw, sum(GREATEST(-sapling_vb, 0)) sa,
                   sum(GREATEST(transparent_out - transparent_in, 0)) tr
            FROM transactions WHERE orchard_vb > 0 GROUP BY height) m
           ON m.height = b.height
         WHERE b.height >= $1 GROUP BY 1 ORDER BY 1",
    )
    .bind(nu6_3_height() as i32)
    .fetch_all(&s.db.pool)
    .await?;

    let top = sqlx::query_as::<_, MigrationTx>(
        "SELECT txid, height, time, orchard_vb AS amount FROM transactions
         WHERE kind = 'migration' ORDER BY orchard_vb DESC LIMIT 10",
    )
    .fetch_all(&s.db.pool)
    .await?;

    let (to_ironwood, to_sapling, to_transparent, fees, exit_txs): (Option<i64>, Option<i64>, Option<i64>, Option<i64>, i64) =
        sqlx::query_as(
            "SELECT sum(GREATEST(-ironwood_vb, 0))::bigint, sum(GREATEST(-sapling_vb, 0))::bigint,
                    sum(GREATEST(transparent_out - transparent_in, 0))::bigint, sum(fee)::bigint, count(*)
             FROM transactions WHERE orchard_vb > 0 AND height >= $1",
        )
        .bind(nu6_3_height() as i32)
        .fetch_one(&s.db.pool)
        .await?;
    // Wallets split migrations into fixed amounts; the visible amount at the turnstile is public.
    let denominations = sqlx::query_as::<_, Denomination>(
        "SELECT -ironwood_vb AS amount, count(*) AS count FROM transactions
         WHERE kind = 'migration' GROUP BY 1 ORDER BY 2 DESC LIMIT 8",
    )
    .fetch_all(&s.db.pool)
    .await?;

    Ok(Json(Migration {
        exits: OrchardExits {
            to_ironwood: to_ironwood.unwrap_or(0),
            to_sapling: to_sapling.unwrap_or(0),
            to_transparent: to_transparent.unwrap_or(0),
            fees: fees.unwrap_or(0),
            txs: exit_txs,
        },
        denominations,
        activation_height: nu6_3_height(),
        snapshot_height: a.height,
        orchard_now: a.orchard,
        ironwood_now: a.ironwood,
        orchard_at_activation,
        ironwood_before_activation,
        migrated,
        migrated_ratio: orchard_at_activation
            .zip(migrated)
            .filter(|(o, _)| *o > 0)
            .map(|(o, m)| m as f64 / o as f64),
        indexed_from: low,
        daily,
        top,
    }))
}
