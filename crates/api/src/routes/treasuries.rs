//! Treasuries made public with a viewing key (`labels/treasuries.json`): balance, inflows,
//! outflows and memos, as the indexer finds them by scanning each account. The viewing key is
//! returned too, so anyone can reproduce the figures (proof.zscan.cash does it in the browser).

use std::collections::BTreeMap;

use axum::{
    extract::{Path, State},
    Json,
};
use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use utoipa::ToSchema;
use zscan_chain::treasuries::{self, Treasury};

use super::nft::Link;
use crate::error::{ApiError, ApiResult};
use crate::AppState;

#[derive(Debug, Serialize, ToSchema)]
pub struct PoolBalances {
    pub sapling: i64,
    pub orchard: i64,
    pub ironwood: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TreasuryRow {
    pub slug: String,
    pub name: String,
    pub description: String,
    pub website: Option<String>,
    pub x: Option<String>,
    /// Unspent notes, zatoshis.
    pub balance: i64,
    /// Payments received (change excluded), zatoshis.
    pub received: i64,
    /// Payments sent to others, zatoshis.
    pub sent: i64,
    pub payments_in: i64,
    pub payments_out: i64,
    pub last_activity: Option<DateTime<Utc>>,
    pub birthday_height: u32,
    /// Last block scanned; null before the first scan.
    pub scanned_height: Option<i32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TreasuryDay {
    pub day: NaiveDate,
    pub inflow: i64,
    pub outflow: i64,
    /// Balance at the end of the day.
    pub balance: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TreasuryActivity {
    pub txid: String,
    pub height: i32,
    pub time: DateTime<Utc>,
    /// received | sent
    pub kind: &'static str,
    pub pool: String,
    pub index: i32,
    pub value: i64,
    pub memo: Option<String>,
    /// Recipient of a payment sent.
    pub recipient: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TreasuryDetail {
    #[serde(flatten)]
    pub row: TreasuryRow,
    pub statement: Option<String>,
    /// The published unified full viewing key, to reproduce everything here.
    pub ufvk: String,
    pub sources: Vec<Link>,
    pub pools: PoolBalances,
    pub daily: Vec<TreasuryDay>,
    /// Newest first, at most 500.
    pub activity: Vec<TreasuryActivity>,
}

async fn row(s: &AppState, t: &Treasury) -> anyhow::Result<TreasuryRow> {
    let (balance, received, payments_in, last_in): (i64, i64, i64, Option<DateTime<Utc>>) =
        sqlx::query_as(
            "SELECT COALESCE(SUM(value) FILTER (WHERE spent_txid IS NULL), 0)::BIGINT,
                COALESCE(SUM(value) FILTER (WHERE scope = 'external'), 0)::BIGINT,
                COUNT(*) FILTER (WHERE scope = 'external'),
                GREATEST(MAX(time), MAX(spent_time))
         FROM treasury_notes WHERE slug = $1",
        )
        .bind(&t.slug)
        .fetch_one(&s.db.pool)
        .await?;
    let (sent, payments_out, last_out): (i64, i64, Option<DateTime<Utc>>) = sqlx::query_as(
        "SELECT COALESCE(SUM(value), 0)::BIGINT, COUNT(*), MAX(time) FROM treasury_sends WHERE slug = $1",
    )
    .bind(&t.slug)
    .fetch_one(&s.db.pool)
    .await?;
    let next: Option<i32> =
        sqlx::query_scalar("SELECT next_height FROM treasury_sync WHERE slug = $1 AND ufvk = $2")
            .bind(&t.slug)
            .bind(&t.ufvk)
            .fetch_optional(&s.db.pool)
            .await?;
    Ok(TreasuryRow {
        slug: t.slug.clone(),
        name: t.name.clone(),
        description: t.description.clone(),
        website: t.website.clone(),
        x: t.x.clone(),
        balance,
        received,
        sent,
        payments_in,
        payments_out,
        last_activity: last_in.max(last_out),
        birthday_height: t.birthday_height,
        scanned_height: next
            .map(|n| n - 1)
            .filter(|&h| h >= t.birthday_height as i32),
    })
}

/// Every public treasury, largest balance first.
#[utoipa::path(get, path = "/v1/treasuries", responses((status = 200, body = [TreasuryRow])), tag = "treasuries")]
pub async fn list(State(s): State<AppState>) -> ApiResult<Vec<TreasuryRow>> {
    let mut rows = Vec::new();
    for t in treasuries::list() {
        rows.push(row(&s, t).await?);
    }
    rows.sort_by(|a, b| b.balance.cmp(&a.balance));
    Ok(Json(rows))
}

/// One public treasury: balance by pool, daily flows and every payment with its memo.
#[utoipa::path(get, path = "/v1/treasuries/{slug}", params(("slug" = String, Path)),
    responses((status = 200, body = TreasuryDetail)), tag = "treasuries")]
pub async fn detail(
    State(s): State<AppState>,
    Path(slug): Path<String>,
) -> ApiResult<TreasuryDetail> {
    let t =
        treasuries::get(&slug).ok_or_else(|| ApiError::NotFound(format!("No treasury {slug}")))?;
    let row = row(&s, t).await?;

    let pools: Vec<(String, i64)> = sqlx::query_as(
        "SELECT pool, SUM(value)::BIGINT FROM treasury_notes WHERE slug = $1 AND spent_txid IS NULL GROUP BY pool",
    )
    .bind(&slug)
    .fetch_all(&s.db.pool)
    .await?;
    let pool = |p: &str| pools.iter().find(|(k, _)| k == p).map_or(0, |(_, v)| *v);

    // Balance moves by day: notes arriving (any scope) and notes spent; inflow and outflow are
    // payments received and sent (change and fees excluded).
    let moves: Vec<(NaiveDate, i64, i64, i64)> = sqlx::query_as(
        "WITH m AS (
            SELECT time::date AS day, value AS delta,
                   CASE WHEN scope = 'external' THEN value ELSE 0 END AS inflow, 0::BIGINT AS outflow
              FROM treasury_notes WHERE slug = $1
            UNION ALL
            SELECT spent_time::date, -value, 0, 0 FROM treasury_notes WHERE slug = $1 AND spent_time IS NOT NULL
            UNION ALL
            SELECT time::date, 0, 0, value FROM treasury_sends WHERE slug = $1
         )
         SELECT day, SUM(delta)::BIGINT, SUM(inflow)::BIGINT, SUM(outflow)::BIGINT FROM m GROUP BY day ORDER BY day",
    )
    .bind(&slug)
    .fetch_all(&s.db.pool)
    .await?;
    let mut by_day: BTreeMap<NaiveDate, (i64, i64, i64)> = BTreeMap::new();
    for (day, delta, inflow, outflow) in moves {
        by_day.insert(day, (delta, inflow, outflow));
    }
    let mut balance = 0;
    let daily = by_day
        .into_iter()
        .map(|(day, (delta, inflow, outflow))| {
            balance += delta;
            TreasuryDay {
                day,
                inflow,
                outflow,
                balance,
            }
        })
        .collect();

    let received: Vec<(String, i32, DateTime<Utc>, String, i32, i64, Option<String>)> =
        sqlx::query_as(
            "SELECT txid, height, time, pool, idx, value, memo FROM treasury_notes
         WHERE slug = $1 AND scope = 'external' ORDER BY height DESC LIMIT 500",
        )
        .bind(&slug)
        .fetch_all(&s.db.pool)
        .await?;
    let sent: Vec<(
        String,
        i32,
        DateTime<Utc>,
        String,
        i32,
        i64,
        Option<String>,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT txid, height, time, pool, idx, value, memo, recipient FROM treasury_sends
         WHERE slug = $1 ORDER BY height DESC LIMIT 500",
    )
    .bind(&slug)
    .fetch_all(&s.db.pool)
    .await?;
    let mut activity: Vec<TreasuryActivity> = received
        .into_iter()
        .map(
            |(txid, height, time, pool, index, value, memo)| TreasuryActivity {
                txid,
                height,
                time,
                kind: "received",
                pool,
                index,
                value,
                memo,
                recipient: None,
            },
        )
        .chain(sent.into_iter().map(
            |(txid, height, time, pool, index, value, memo, recipient)| TreasuryActivity {
                txid,
                height,
                time,
                kind: "sent",
                pool,
                index,
                value,
                memo,
                recipient,
            },
        ))
        .collect();
    activity.sort_by(|a, b| {
        b.height
            .cmp(&a.height)
            .then(a.kind.cmp(b.kind))
            .then(a.index.cmp(&b.index))
    });
    activity.truncate(500);

    Ok(Json(TreasuryDetail {
        row,
        statement: t.statement.clone(),
        ufvk: t.ufvk.clone(),
        sources: t
            .sources
            .iter()
            .map(|l| Link {
                label: l.label.clone(),
                url: l.url.clone(),
            })
            .collect(),
        pools: PoolBalances {
            sapling: pool("sapling"),
            orchard: pool("orchard"),
            ironwood: pool("ironwood"),
        },
        daily,
        activity,
    }))
}
