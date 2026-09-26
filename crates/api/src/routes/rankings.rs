//! Top miners and top accounts.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Query, State},
    Json,
};
use chrono::{DateTime, Utc};
use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use utoipa::{IntoParams, ToSchema};

use crate::error::{ApiError, ApiResult};
use crate::AppState;

/// Pool name for a miner address: the most common pool tag across all its blocks.
pub const MINER_POOL_SQL: &str =
    "(SELECT mode() WITHIN GROUP (ORDER BY b2.miner_pool) FROM blocks b2 \
    WHERE b2.miner = blocks.miner AND b2.miner_pool IS NOT NULL)";

#[derive(Debug, Deserialize, IntoParams)]
pub struct MinersQuery {
    /// `24h`, `7d` (default) or `30d`.
    pub period: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct Miner {
    pub address: String,
    /// Mining pool, identified from coinbase signatures.
    pub pool: Option<String>,
    /// Most common coinbase text for this miner, when present.
    pub tag: Option<String>,
    pub blocks: i64,
    /// Total rewards (subsidy + fees) in zatoshis.
    pub rewards: i64,
    pub fees: i64,
    pub last_block: i32,
    pub last_time: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Miners {
    pub period: String,
    pub total_blocks: i64,
    /// Smallest number of mining entities that together found more than 50% of blocks.
    pub nakamoto_coefficient: usize,
    /// Share of blocks found by the largest entity, 0–1.
    pub top_share: f64,
    pub miners: Vec<Miner>,
}

/// Top miners by blocks found over a period.
#[utoipa::path(get, path = "/v1/miners", params(MinersQuery), responses((status = 200, body = Miners)), tag = "network")]
pub async fn miners(State(s): State<AppState>, Query(q): Query<MinersQuery>) -> ApiResult<Miners> {
    let (period, interval) = match q.period.as_deref() {
        Some("24h") => ("24h", "24 hours"),
        None | Some("7d") => ("7d", "7 days"),
        Some("30d") => ("30d", "30 days"),
        Some(_) => {
            return Err(ApiError::BadRequest(
                "invalid_period",
                "period must be 24h, 7d or 30d".into(),
            ))
        }
    };
    let miners = sqlx::query_as::<_, Miner>(&format!(
        "SELECT miner AS address, {MINER_POOL_SQL} AS pool,
                (SELECT mode() WITHIN GROUP (ORDER BY b3.miner_tag) FROM blocks b3
                  WHERE b3.miner = blocks.miner AND b3.miner_tag <> '') AS tag,
                count(*) AS blocks,
                sum(coinbase_value)::bigint AS rewards, COALESCE(sum(fees), 0)::bigint AS fees,
                max(height) AS last_block, max(time) AS last_time
         FROM blocks
         WHERE miner IS NOT NULL AND time > (SELECT max(time) FROM blocks) - $1::interval
         GROUP BY miner ORDER BY blocks DESC LIMIT 50"
    ))
    .bind(interval)
    .fetch_all(&s.db.pool)
    .await?;
    let total_blocks: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM blocks WHERE time > (SELECT max(time) FROM blocks) - $1::interval",
    )
    .bind(interval)
    .fetch_one(&s.db.pool)
    .await?;
    // Group payout addresses by pool so one pool with several addresses counts once.
    let mut by_entity: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for m in &miners {
        *by_entity
            .entry(m.pool.clone().unwrap_or_else(|| m.address.clone()))
            .or_default() += m.blocks;
    }
    let mut shares: Vec<i64> = by_entity.into_values().collect();
    shares.sort_unstable_by(|a, b| b.cmp(a));
    let (mut acc, mut nakamoto) = (0i64, 0usize);
    for s in &shares {
        acc += s;
        nakamoto += 1;
        if acc * 2 > total_blocks {
            break;
        }
    }
    let top_share = if total_blocks > 0 {
        *shares.first().unwrap_or(&0) as f64 / total_blocks as f64
    } else {
        0.0
    };
    Ok(Json(Miners {
        period: period.into(),
        total_blocks,
        nakamoto_coefficient: nakamoto,
        top_share,
        miners,
    }))
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Account {
    pub rank: usize,
    pub address: String,
    /// Current balance from the node, zatoshis.
    pub balance: i64,
    /// Share of the transparent pool, 0–1.
    pub share: f64,
    /// Transactions within the indexed range.
    pub tx_count: i64,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Accounts {
    pub updated_at: Option<DateTime<Utc>>,
    /// Accounts are ranked among addresses active since this height.
    pub indexed_from: Option<i32>,
    pub accounts: Vec<Account>,
}

pub type AccountsCache = Arc<RwLock<Accounts>>;

pub fn empty_accounts() -> AccountsCache {
    Arc::new(RwLock::new(Accounts {
        updated_at: None,
        indexed_from: None,
        accounts: vec![],
    }))
}

/// Top transparent accounts by balance, among addresses active in the indexed range.
#[utoipa::path(get, path = "/v1/accounts", responses((status = 200, body = Accounts)), tag = "addresses")]
pub async fn accounts(State(s): State<AppState>) -> ApiResult<Accounts> {
    Ok(Json(s.accounts.read().await.clone()))
}

/// Refreshes the top-accounts cache every 10 minutes. Candidates are the most active
/// addresses by received volume; balances come from the node, so they cover full history.
pub async fn refresh_accounts(state: AppState) {
    loop {
        if let Err(e) = refresh_once(&state).await {
            tracing::warn!("accounts refresh: {e:#}");
        }
        tokio::time::sleep(Duration::from_secs(600)).await;
    }
}

async fn refresh_once(s: &AppState) -> anyhow::Result<()> {
    // The busiest addresses, plus every labeled one (exchanges, bridges and treasuries can hold
    // a lot while moving little).
    let candidates: Vec<(String, i64)> = sqlx::query_as(
        "WITH x AS (
            SELECT address, txid, value FROM tx_outputs WHERE address IS NOT NULL
            UNION ALL
            SELECT address, txid, value FROM tx_inputs WHERE address IS NOT NULL
         ), busy AS (
            SELECT address FROM x GROUP BY address ORDER BY sum(value) DESC LIMIT 200
         ), c AS (SELECT address FROM busy UNION SELECT address FROM labels)
         SELECT c.address, count(DISTINCT x.txid) FROM c LEFT JOIN x USING (address) GROUP BY c.address",
    )
    .fetch_all(&s.db.pool)
    .await?;
    let labels: Vec<(String, Option<String>)> = sqlx::query_as(&format!(
        "SELECT DISTINCT miner, {MINER_POOL_SQL} FROM blocks WHERE miner IS NOT NULL"
    ))
    .fetch_all(&s.db.pool)
    .await?;
    let transparent: i64 =
        sqlx::query_scalar("SELECT transparent FROM pool_snapshots ORDER BY height DESC LIMIT 1")
            .fetch_optional(&s.db.pool)
            .await?
            .unwrap_or(0);
    let indexed_from: Option<i32> = sqlx::query_scalar("SELECT low FROM sync_state")
        .fetch_one(&s.db.pool)
        .await?;

    let tags = super::labels::lookup(
        &s.db,
        &candidates
            .iter()
            .map(|(a, _)| a.clone())
            .collect::<Vec<_>>(),
    )
    .await?;
    let lwd = s.lwd.clone();
    let mut rows: Vec<(String, i64, i64)> = stream::iter(candidates)
        .map(|(addr, txs)| {
            let lwd = lwd.clone();
            async move { lwd.taddr_balance(&addr).await.ok().map(|b| (addr, b, txs)) }
        })
        .buffer_unordered(8)
        .filter_map(|r| async move { r })
        .collect()
        .await;
    rows.sort_by_key(|r| std::cmp::Reverse(r.1));

    let accounts = rows
        .into_iter()
        .take(100)
        .enumerate()
        .map(|(i, (address, balance, tx_count))| {
            let label = tags.get(&address).map(|t| t.name.clone()).or_else(|| {
                labels.iter().find(|(m, _)| *m == address).map(|(_, p)| {
                    format!(
                        "Miner{}",
                        p.as_ref().map(|p| format!(": {p}")).unwrap_or_default()
                    )
                })
            });
            Account {
                rank: i + 1,
                share: if transparent > 0 {
                    balance as f64 / transparent as f64
                } else {
                    0.0
                },
                address,
                balance,
                tx_count,
                label,
            }
        })
        .collect();
    *s.accounts.write().await = Accounts {
        updated_at: Some(Utc::now()),
        indexed_from,
        accounts,
    };
    tracing::info!("top accounts refreshed");
    Ok(())
}
