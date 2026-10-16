//! The Sprout evacuation, for ZStreet: what is still in the oldest shielded pool, which NU7 leaves
//! unspendable, and the last transactions that got out of it.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use axum::{extract::State, Json};
use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use tokio::sync::Mutex;

use crate::error::ApiError;
use crate::AppState;

#[derive(Debug, Clone, Serialize)]
pub struct Sprout {
    pub height: i32,
    /// ZEC still in Sprout (zatoshis).
    pub pool: i64,
    /// Height NU7 activates at, once announced.
    pub nu7_height: Option<u32>,
    /// Net zatoshis that left Sprout over the last 30 days.
    pub out_30d: i64,
    /// Pool change per day (negative = left Sprout), last 120 days with any movement.
    pub daily: Vec<Day>,
    /// The latest transactions that took ZEC out of Sprout, newest first.
    pub exits: Vec<Exit>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Day {
    pub day: NaiveDate,
    pub delta: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Exit {
    pub txid: String,
    pub height: i32,
    pub time: DateTime<Utc>,
    /// Zatoshis that left Sprout.
    pub amount: i64,
    /// Where they went: sapling, orchard, ironwood or transparent.
    pub to: &'static str,
}

/// GET /v1/street/sprout. Finding the exits reads every block's Sprout delta, so the answer is
/// kept five minutes (Sprout moves a few times a month).
pub async fn sprout(State(s): State<AppState>) -> Result<Json<Sprout>, ApiError> {
    static CACHE: OnceLock<Mutex<Option<(Instant, Sprout)>>> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    let mut slot = cache.lock().await;
    if let Some((t, v)) = slot.as_ref() {
        if t.elapsed() < Duration::from_secs(300) {
            return Ok(Json(v.clone()));
        }
    }
    let v = compute(&s).await?;
    *slot = Some((Instant::now(), v.clone()));
    Ok(Json(v))
}

async fn compute(s: &AppState) -> Result<Sprout, ApiError> {
    let (height, pool): (i32, i64) =
        sqlx::query_as("SELECT height, sprout FROM pool_snapshots ORDER BY height DESC LIMIT 1")
            .fetch_one(&s.db.pool)
            .await?;
    let daily: Vec<(NaiveDate, i64)> = sqlx::query_as(
        "SELECT day::date, d_sprout FROM daily_stats
         WHERE d_sprout <> 0 AND day > now() - interval '120 days' ORDER BY day",
    )
    .fetch_all(&s.db.pool)
    .await?;
    let out_30d: Option<i64> = sqlx::query_scalar(
        "SELECT -sum(d_sprout)::bigint FROM daily_stats WHERE day > now() - interval '30 days'",
    )
    .fetch_one(&s.db.pool)
    .await?;
    // Blocks where Sprout shrank, then their transactions (by the height index).
    let heights: Vec<i32> = sqlx::query_scalar(
        "SELECT height FROM blocks WHERE d_sprout < 0 ORDER BY height DESC LIMIT 40",
    )
    .fetch_all(&s.db.pool)
    .await?;
    let rows: Vec<(String, i32, DateTime<Utc>, i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT txid, height, time, sprout_vb, sapling_vb, orchard_vb, ironwood_vb
         FROM transactions WHERE height = ANY($1) AND sprout_vb > 0
         ORDER BY height DESC, block_index LIMIT 40",
    )
    .bind(&heights)
    .fetch_all(&s.db.pool)
    .await?;
    let exits = rows
        .into_iter()
        .map(|(txid, height, time, sprout, sapling, orchard, ironwood)| {
            // value balance < 0 means value went into that pool
            let to = [("sapling", sapling), ("orchard", orchard), ("ironwood", ironwood)]
                .into_iter()
                .filter(|(_, v)| *v < 0)
                .min_by_key(|(_, v)| *v)
                .map_or("transparent", |(p, _)| p);
            Exit {
                txid,
                height,
                time,
                amount: sprout,
                to,
            }
        })
        .collect();
    Ok(Sprout {
        height,
        pool,
        nu7_height: zscan_chain::upgrades::nu7_height(),
        out_30d: out_30d.unwrap_or(0),
        daily: daily
            .into_iter()
            .map(|(day, delta)| Day { day, delta })
            .collect(),
        exits,
    })
}
