//! ZStreet's living city, written from real chain data: the residents' gossip about each block,
//! the morning paper, the mayor's decree every 40 blocks, live events and what characters
//! remember of each visitor. The language model only writes the words, from facts built here;
//! every endpoint has a template to fall back on when it is off or fails.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::{
    extract::{ConnectInfo, Query, State},
    http::{header, HeaderMap},
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, NaiveDate, NaiveTime, Timelike, Utc, Weekday};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::OnceCell;

use crate::error::ApiError;
use crate::street_llm::{self as llm, field, fit};
use crate::AppState;

const SHIELDED_KINDS: &str = "('shielded', 'shielding', 'deshielding', 'migration', 'mixed')";

/// One computation per key, shared by concurrent requests, for the last `keep` keys.
pub(crate) struct Flights<V> {
    map: Mutex<BTreeMap<u64, Arc<OnceCell<V>>>>,
    keep: usize,
}

impl<V> Flights<V> {
    pub(crate) const fn new(keep: usize) -> Self {
        Self {
            map: Mutex::new(BTreeMap::new()),
            keep,
        }
    }

    pub(crate) fn cell(&self, key: u64) -> Arc<OnceCell<V>> {
        let mut m = self.map.lock().unwrap();
        let cell = m.entry(key).or_default().clone();
        while m.len() > self.keep {
            m.pop_first();
        }
        cell
    }
}

pub(crate) fn per_minute(n: u32) -> DefaultKeyedRateLimiter<IpAddr> {
    RateLimiter::keyed(Quota::per_minute(NonZeroU32::new(n).unwrap()))
}

pub(crate) fn ip_of(peer: SocketAddr, headers: &HeaderMap) -> IpAddr {
    crate::client_ip(peer.ip(), headers).unwrap_or(peer.ip())
}

/// Visitors are identified by a client-side secret only: 32–64 hex characters, stored hashed.
pub(crate) fn token_hash(token: &str) -> Option<Vec<u8>> {
    ((32..=64).contains(&token.len()) && token.bytes().all(|b| b.is_ascii_hexdigit()))
        .then(|| Sha256::digest(token.as_bytes()).to_vec())
}

pub(crate) fn bad_request(message: &str) -> ApiError {
    ApiError::BadRequest("bad_request", message.to_string())
}

/// Zatoshis as ZEC, without trailing zeros.
fn zec(zat: i64) -> String {
    let s = format!("{:.8}", zat as f64 / 1e8);
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn pct(x: f64) -> String {
    format!("{:.0}", x * 100.0)
}

async fn mempool_count(s: &AppState) -> Result<(i64, i64), ApiError> {
    Ok(sqlx::query_as(
        "SELECT count(*), COALESCE(sum(fee), 0)::int8 FROM mempool m
         WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.txid = m.txid)",
    )
    .fetch_one(&s.db.pool)
    .await?)
}

async fn shielded_supply(s: &AppState) -> Result<Option<f64>, ApiError> {
    let row: Option<(i64, i64)> = sqlx::query_as(
        "SELECT (sprout + sapling + orchard + ironwood)::int8, supply FROM pool_snapshots
         ORDER BY height DESC LIMIT 1",
    )
    .fetch_optional(&s.db.pool)
    .await?;
    Ok(row
        .filter(|r| r.1 > 0)
        .map(|(sh, sup)| sh as f64 / sup as f64))
}

pub(crate) async fn tip(s: &AppState) -> Result<u64, ApiError> {
    crate::routes::tip(&s.db)
        .await?
        .map(|h| h.max(0) as u64)
        .ok_or_else(|| ApiError::NotFound("No blocks indexed yet".into()))
}

// ---------------------------------------------------------------------------------------------
// Gossip

#[derive(Clone, Serialize)]
pub(crate) struct Line {
    pub(crate) text: String,
    tone: &'static str,
}

pub(crate) struct BlockFacts {
    pub(crate) height: i64,
    pub(crate) txs: i64,
    size: i64,
    fees: Option<i64>,
    pool: Option<String>,
    gap: Option<i64>,
    kinds: Vec<(String, i64)>,
    mempool: i64,
    price: Option<(f64, f64)>,
    shielded_supply: Option<f64>,
}

impl BlockFacts {
    #[cfg(test)]
    pub(crate) fn sample() -> Self {
        Self {
            height: 3_101_240,
            txs: 12,
            size: 45_000,
            fees: Some(120_000),
            pool: Some("ViaBTC".into()),
            gap: Some(80),
            kinds: vec![("shielded".into(), 8), ("transparent".into(), 4)],
            mempool: 3,
            price: Some((41.5, -2.25)),
            shielded_supply: Some(0.31),
        }
    }

    pub(crate) fn kind(&self, k: &str) -> i64 {
        self.kinds
            .iter()
            .find(|(n, _)| n == k)
            .map_or(0, |(_, c)| *c)
    }

    pub(crate) fn text(&self) -> String {
        let mut f = vec![format!(
            "Block {} was just mined by {}.",
            self.height,
            self.pool.as_deref().unwrap_or("an unknown miner")
        )];
        f.push(format!(
            "It carried {} transactions, not counting the miner's reward.",
            self.txs
        ));
        if !self.kinds.is_empty() {
            let parts: Vec<_> = self.kinds.iter().map(|(k, c)| format!("{c} {k}")).collect();
            f.push(format!("By kind: {}.", parts.join(", ")));
        }
        if let Some(fees) = self.fees {
            f.push(format!("Fees paid in the block: {} ZEC.", zec(fees)));
        }
        f.push(format!("Block size: {} KB.", self.size / 1000));
        if let Some(g) = self.gap {
            f.push(format!("It came {g} seconds after the previous block."));
        }
        f.push(format!(
            "Transactions still waiting in the mempool: {}.",
            self.mempool
        ));
        if let Some((usd, ch)) = self.price {
            f.push(format!(
                "ZEC price: {usd:.2} USD, {} {:.1}% in 24 hours.",
                if ch >= 0.0 { "up" } else { "down" },
                ch.abs()
            ));
        }
        if let Some(s) = self.shielded_supply {
            f.push(format!("Share of all ZEC held shielded: {}%.", pct(s)));
        }
        f.join("\n")
    }
}

fn talk(text: String) -> Line {
    Line {
        text: fit(&text, 60),
        tone: "talk",
    }
}

fn shout(text: String) -> Line {
    Line {
        text: fit(&text, 60),
        tone: "shout",
    }
}

pub(crate) fn gossip_template(f: &BlockFacts) -> Vec<Line> {
    let shielded =
        f.kind("shielded") + f.kind("shielding") + f.kind("deshielding") + f.kind("mixed");
    let mut lines = vec![shout(format!("All aboard! Block {} is leaving!", f.height))];
    lines.push(talk(match f.txs {
        0 => "Empty bus, only the miner's reward aboard.".into(),
        1 => "Just one of us rode that one.".into(),
        n => format!("{n} of us rode that one."),
    }));
    if !f.kinds.is_empty() {
        lines.push(talk(match shielded {
            0 => "Not one shielded rider this time.".into(),
            1 => "One shielded rider. Nobody saw a thing.".into(),
            n => format!("{n} shielded riders. Nobody saw a thing."),
        }));
        let t = f.kind("transparent");
        if t > 0 {
            lines.push(talk(format!("{t} rode fully transparent, everyone saw.")));
        }
    }
    lines.push(talk(match &f.pool {
        Some(p) => format!("{p} drove this bus."),
        None => "A mystery miner drove this bus.".into(),
    }));
    if let Some(g) = f.gap {
        lines.push(talk(match g {
            g if g > 150 => format!("{g} seconds wait, felt like forever."),
            g if g < 30 => format!("Only {g} seconds after the last bus!"),
            g => format!("Right on time, {g} seconds after the last."),
        }));
    }
    lines.push(talk(match f.mempool {
        0 => "The MEMPOOL shop is empty now.".into(),
        m => format!("{m} still waiting at the MEMPOOL shop."),
    }));
    if let Some(fees) = f.fees.filter(|x| *x > 0) {
        lines.push(shout(format!("{} ZEC in fees for the driver!", zec(fees))));
    }
    if let Some((usd, ch)) = f.price {
        lines.push(talk(format!(
            "ZEC at {usd:.2} USD, {} {:.1}% today.",
            if ch >= 0.0 { "up" } else { "down" },
            ch.abs()
        )));
    }
    if let Some(s) = f.shielded_supply {
        lines.push(talk(format!("{}% of all ZEC is shielded now.", pct(s))));
    }
    for filler in [
        "Shh. Shielded means private.",
        "Next bus soon, grab your ticket!",
        "I love this city at block time.",
    ] {
        if lines.len() >= 6 {
            break;
        }
        lines.push(talk(filler.into()));
    }
    lines.truncate(10);
    lines
}

pub(crate) async fn block_facts(s: &AppState, h: i64) -> Result<Option<BlockFacts>, ApiError> {
    /// height, tx count, size, fees, pool, seconds since the previous block
    type Row = (i64, i32, i32, Option<i64>, Option<String>, Option<i64>);
    let row: Option<Row> = sqlx::query_as(
        "SELECT b.height::int8, b.tx_count, b.size, b.fees,
                COALESCE(b.miner_pool, (SELECT mp.pool FROM miner_pools mp WHERE mp.miner = b.miner)),
                EXTRACT(EPOCH FROM b.time - p.time)::int8
         FROM blocks b LEFT JOIN blocks p ON p.height = b.height - 1 WHERE b.height = $1",
    )
    .bind(h as i32)
    .fetch_optional(&s.db.pool)
    .await?;
    let Some((height, tx_count, size, fees, pool, gap)) = row else {
        return Ok(None);
    };
    let kinds: Vec<(String, i64)> = sqlx::query_as(
        "SELECT kind, count(*) FROM transactions WHERE height = $1 AND kind <> 'coinbase'
         GROUP BY kind ORDER BY count(*) DESC",
    )
    .bind(h as i32)
    .fetch_all(&s.db.pool)
    .await?;
    let price = s
        .price
        .lock()
        .await
        .latest()
        .map(|p| (p.usd, p.usd_24h_change));
    Ok(Some(BlockFacts {
        height,
        txs: (tx_count as i64 - 1).max(0),
        size: size as i64,
        fees,
        pool,
        gap,
        kinds,
        mempool: mempool_count(s).await?.0,
        price,
        shielded_supply: shielded_supply(s).await?,
    }))
}

async fn write_gossip(s: &AppState, f: &BlockFacts, use_llm: bool) -> Vec<Line> {
    let template = gossip_template(f);
    if !use_llm {
        return template;
    }
    let facts = f.text();
    let system = "You write street chatter for ZStreet, a voxel city where zkSNARK residents \
        (pixel-art characters) carry real Zcash transactions to the bus stop; each mined block is a \
        bus that takes them away. Residents gossip to each other about the block that just left. \
        Write in English only. Return strict JSON only, nothing else: \
        {\"lines\":[{\"text\":\"...\",\"tone\":\"talk\"}]} with 8 lines. Each text is at most 60 \
        characters, playful, varied, like game NPCs talking. tone is \"talk\" or \"shout\" (at most \
        2 shouts, for excited lines). Use only the facts given: never invent numbers, names, amounts \
        or events, and copy any number exactly as written in the facts. No price predictions or \
        financial advice, no hashtags, no emojis.";
    let Some(v) = llm::complete_json(&s.http, system, &facts, 600).await else {
        return template;
    };
    let mut lines: Vec<Line> = v["lines"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|l| {
            Some(Line {
                text: field(&l["text"], 60, &facts)?,
                tone: if l["tone"] == "shout" {
                    "shout"
                } else {
                    "talk"
                },
            })
        })
        .take(10)
        .collect();
    for t in template {
        if lines.len() >= 6 {
            break;
        }
        lines.push(t);
    }
    lines
}

#[derive(Deserialize)]
pub struct GossipQuery {
    h: Option<u64>,
}

/// `GET /v1/street/gossip?h=`: what the residents say about a block (the tip by default).
/// Written once per block and shared by every visitor.
pub async fn gossip(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(q): Query<GossipQuery>,
) -> Result<Response, ApiError> {
    static LIMIT: OnceLock<DefaultKeyedRateLimiter<IpAddr>> = OnceLock::new();
    static CACHE: Flights<Value> = Flights::new(64);
    if let Some(wait) =
        crate::limited_for(LIMIT.get_or_init(|| per_minute(30)), &ip_of(peer, &headers))
    {
        return Ok(crate::too_many_requests(wait, "Too much gossip, slow down"));
    }
    let tip = tip(&s).await?;
    let h = q.h.unwrap_or(tip);
    if h > tip {
        return Err(ApiError::NotFound(format!(
            "Block {h} hasn't been seen yet"
        )));
    }
    let cell = CACHE.cell(h);
    let v = cell
        .get_or_try_init(|| async {
            let f = block_facts(&s, h as i64)
                .await?
                .ok_or_else(|| ApiError::NotFound(format!("Block {h} isn't indexed")))?;
            // old blocks get the template: nobody gets to spend the model on history
            let lines = write_gossip(&s, &f, h + 64 >= tip).await;
            Ok::<_, ApiError>(json!({ "height": h, "lines": lines }))
        })
        .await?;
    Ok(Json(v.clone()).into_response())
}

// ---------------------------------------------------------------------------------------------
// The mayor

const TERM: u64 = 40;

/// Whether the average block fee of the last window lands in the top quartile of the previous
/// window's block fees.
fn fee_spike(cur: &[i64], prev: &[i64]) -> bool {
    if cur.is_empty() || prev.is_empty() {
        return false;
    }
    let mut p = prev.to_vec();
    p.sort_unstable();
    let q3 = p[(p.len() * 3 / 4).min(p.len() - 1)];
    let avg = cur.iter().sum::<i64>() / cur.len() as i64;
    avg > 0 && avg > q3
}

/// What the decree does, decided from the facts (the model only words it).
fn decree_effect(empty: usize, shielded_share: f64, cur: &[i64], prev: &[i64]) -> &'static str {
    if !cur.is_empty() && empty * 10 >= cur.len() * 3 {
        "curfew"
    } else if shielded_share >= 0.6 {
        "festival"
    } else if fee_spike(cur, prev) {
        "treasure"
    } else {
        "quiet"
    }
}

fn decree_template(
    effect: &str,
    empty: usize,
    share: f64,
    fees: i64,
    until: u64,
) -> (String, String) {
    let (title, text) = match effect {
        "curfew" => ("Curfew on Bus Lane".to_string(), format!("{empty} of the last 40 buses left empty. The Mayor orders a curfew until block {until}: rest up, residents.")),
        "festival" => ("Festival of Shadows".to_string(), format!("{}% of recent riders went shielded. The Mayor declares a street festival until block {until}. Lanterns up, ledgers private!", pct(share))),
        "treasure" => ("Treasure in the Fees".to_string(), format!("Fees ran hot over the last 40 blocks ({} ZEC). The Mayor declares a treasure hunt until block {until}!", zec(fees))),
        _ => ("A Quiet Decree".to_string(), format!("Nothing unusual in the last 40 blocks. The Mayor asks everyone to keep calm and carry transactions until block {until}.")),
    };
    (fit(&title, 40), fit(&text, 200))
}

async fn issue_decree(s: &AppState, at: u64) -> Result<Value, ApiError> {
    let until = at + TERM;
    let blocks: Vec<(i32, i32, i64)> = sqlx::query_as(
        "SELECT height, tx_count, COALESCE(fees, 0)::int8 FROM blocks
         WHERE height BETWEEN $1 AND $2 ORDER BY height",
    )
    .bind(at.saturating_sub(2 * TERM - 1) as i32)
    .bind(at as i32)
    .fetch_all(&s.db.pool)
    .await?;
    let from = at.saturating_sub(TERM - 1) as i32;
    let (cur, prev): (Vec<&(i32, i32, i64)>, Vec<_>) = blocks.iter().partition(|b| b.0 >= from);
    let empty = cur.iter().filter(|b| b.1 <= 1).count();
    let cur_fees: Vec<i64> = cur.iter().map(|b| b.2).collect();
    let prev_fees: Vec<i64> = prev.iter().map(|b| b.2).collect();
    let (shielded, txs): (i64, i64) = sqlx::query_as(&format!(
        "SELECT count(*) FILTER (WHERE kind IN {SHIELDED_KINDS}), count(*) FILTER (WHERE kind <> 'coinbase')
         FROM transactions WHERE height BETWEEN $1 AND $2"
    ))
    .bind(from)
    .bind(at as i32)
    .fetch_one(&s.db.pool)
    .await?;
    let share = if txs > 0 {
        shielded as f64 / txs as f64
    } else {
        0.0
    };
    let effect = decree_effect(empty, share, &cur_fees, &prev_fees);
    let fees: i64 = cur_fees.iter().sum();
    let prev_total: i64 = prev_fees.iter().sum();
    let (mempool, _) = mempool_count(s).await?;
    let (mut title, mut text) = decree_template(effect, empty, share, fees, until);

    let facts = format!(
        "The decree is issued at block {at} and lasts until block {until}.\n\
         In the last 40 blocks: {empty} empty blocks (only the miner's reward), {txs} transactions, \
         {}% of them shielded, {} ZEC in fees (the 40 blocks before: {} ZEC).\n\
         Transactions waiting in the mempool: {mempool}.\n\
         The decree's effect: {}.",
        pct(share),
        zec(fees),
        zec(prev_total),
        match effect {
            "curfew" => "curfew (too many buses left empty, the streets go quiet)",
            "festival" => "festival (most riders went shielded, the city celebrates privacy)",
            "treasure" => "treasure (fees spiked, a treasure hunt for the fees)",
            _ => "quiet (an ordinary stretch, keep calm)",
        }
    );
    let system = "You are the Mayor of ZStreet, a voxel city where zkSNARK residents carry real \
        Zcash transactions and each mined block is a bus. Write a short, playful decree announcing \
        the given effect, in English. Return strict JSON only: {\"title\": \"...\", \"text\": \"...\"}. \
        title at most 40 characters, text at most 200 characters. Use only the facts given: never \
        invent numbers or events, copy any number exactly as written. No markdown, no emojis, no \
        financial advice.";
    if let Some(v) = llm::complete_json(&s.http, system, &facts, 300).await {
        if let (Some(t), Some(x)) = (
            field(&v["title"], 40, &facts),
            field(&v["text"], 200, &facts),
        ) {
            title = t;
            text = x;
        }
    }
    Ok(json!({
        "height": at,
        "until": until,
        "decree": { "title": title, "text": text, "effect": effect },
    }))
}

/// `GET /v1/street/mayor`: the decree in force, a new one every 40 blocks.
pub async fn mayor(State(s): State<AppState>) -> Result<Response, ApiError> {
    static CACHE: Flights<Value> = Flights::new(4);
    let at = tip(&s).await? / TERM * TERM;
    let cell = CACHE.cell(at);
    let v = cell.get_or_try_init(|| issue_decree(&s, at)).await?;
    Ok(Json(v.clone()).into_response())
}

// ---------------------------------------------------------------------------------------------
// The ZStreet Daily

/// Every 10 minutes: prints today's paper (UTC) once it's past 06:00, or right away when there
/// has never been one.
pub async fn paper_loop(s: AppState) {
    loop {
        if let Err(e) = print_if_due(&s).await {
            tracing::warn!("street paper: {e:#}");
        }
        tokio::time::sleep(Duration::from_secs(600)).await;
    }
}

async fn print_if_due(s: &AppState) -> anyhow::Result<()> {
    let now = Utc::now();
    let today = now.date_naive();
    let (done, any): (bool, bool) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM street_papers WHERE day = $1),
                EXISTS (SELECT 1 FROM street_papers)",
    )
    .bind(today)
    .fetch_one(&s.db.pool)
    .await?;
    if done || (any && now.hour() < 6) {
        return Ok(());
    }
    let Some(data) = write_paper(s, today).await? else {
        return Ok(());
    };
    sqlx::query("INSERT INTO street_papers (day, data) VALUES ($1, $2) ON CONFLICT DO NOTHING")
        .bind(today)
        .bind(&data)
        .execute(&s.db.pool)
        .await?;
    tracing::info!(%today, "street paper printed");
    Ok(())
}

struct DayStats {
    blocks: i64,
    txs: i64,
    fees: i64,
    empty: i64,
    shielded_share: Option<f64>,
    biggest: Option<(i32, i32, i32)>,
    top_pool: Option<(String, i64)>,
    mempool: i64,
    mempool_fees: i64,
}

async fn day_stats(s: &AppState, day: NaiveDate) -> anyhow::Result<DayStats> {
    let from = day.and_time(NaiveTime::MIN).and_utc();
    let to = from + chrono::Duration::days(1);
    let (blocks, txs, fees, empty): (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT count(*), COALESCE(sum(tx_count - 1), 0)::int8, COALESCE(sum(fees), 0)::int8,
                count(*) FILTER (WHERE tx_count <= 1)
         FROM blocks WHERE time >= $1 AND time < $2",
    )
    .bind(from)
    .bind(to)
    .fetch_one(&s.db.pool)
    .await?;
    let biggest: Option<(i32, i32, i32)> = sqlx::query_as(
        "SELECT height, size, tx_count - 1 FROM blocks WHERE time >= $1 AND time < $2
         ORDER BY size DESC LIMIT 1",
    )
    .bind(from)
    .bind(to)
    .fetch_optional(&s.db.pool)
    .await?;
    let top_pool: Option<(String, i64)> = sqlx::query_as(
        "SELECT COALESCE(b.miner_pool, mp.pool) AS pool, count(*) FROM blocks b
         LEFT JOIN miner_pools mp ON mp.miner = b.miner
         WHERE b.time >= $1 AND b.time < $2 AND COALESCE(b.miner_pool, mp.pool) IS NOT NULL
         GROUP BY 1 ORDER BY 2 DESC LIMIT 1",
    )
    .bind(from)
    .bind(to)
    .fetch_optional(&s.db.pool)
    .await?;
    let shielded: Option<(i64, i64)> = sqlx::query_as(
        "SELECT sum(txs)::int8, sum(shielded + shielding + deshielding + migration + mixed)::int8
         FROM daily_stats WHERE day >= $1 AND day < $2 HAVING count(*) > 0",
    )
    .bind(from)
    .bind(to)
    .fetch_optional(&s.db.pool)
    .await?;
    let (mempool, mempool_fees) = mempool_count(s)
        .await
        .map_err(|_| anyhow::anyhow!("mempool query failed"))?;
    Ok(DayStats {
        blocks,
        txs,
        fees,
        empty,
        shielded_share: shielded
            .filter(|(t, _)| *t > 0)
            .map(|(t, sh)| sh as f64 / t as f64),
        biggest,
        top_pool,
        mempool,
        mempool_fees,
    })
}

fn story(title: &str, body: String) -> Value {
    json!({ "title": fit(title, 60), "body": fit(&body, 280) })
}

fn weather_template(st: &DayStats) -> String {
    match st.mempool {
        0 => "Clear skies: the MEMPOOL shop is empty and the buses run light.".into(),
        m if m < 20 => {
            format!("Light traffic with {m} riders waiting. Fees mild, umbrellas optional.")
        }
        m => format!("Crowded streets: {m} riders queue at the MEMPOOL. Expect busy buses."),
    }
}

fn paper_template(st: &DayStats, news: &[String]) -> (String, String, Vec<Value>) {
    let headline = format!("{} buses left the station yesterday", st.blocks);
    let subhead = match st.shielded_share {
        Some(sh) => format!(
            "{} transactions rode the chain, {}% of them with shielded parts.",
            st.txs,
            pct(sh)
        ),
        None => format!("{} transactions rode the chain.", st.txs),
    };
    let mut stories = Vec::new();
    if let Some((h, size, n)) = st.biggest {
        stories.push(story(
            "Busiest bus of the day",
            format!(
                "Block {h} was the biggest yesterday: {} KB with {n} transactions aboard.",
                size / 1000
            ),
        ));
    }
    if let Some((pool, n)) = &st.top_pool {
        stories.push(story(
            "Top driver",
            format!(
                "{pool} mined {n} of yesterday's {} blocks, more than any other pool.",
                st.blocks
            ),
        ));
    }
    if !news.is_empty() {
        stories.push(story(
            "From the wires",
            format!(
                "In the Zcash news: {}.",
                news.iter().take(3).cloned().collect::<Vec<_>>().join("; ")
            ),
        ));
    }
    stories.push(story(
        "Fares and empty seats",
        format!(
            "Riders paid {} ZEC in fees in total. {}",
            zec(st.fees),
            match st.empty {
                0 => "Every bus carried passengers.".to_string(),
                e => format!("{e} buses left with nobody but the miner's reward."),
            }
        ),
    ));
    stories.truncate(4);
    (fit(&headline, 80), fit(&subhead, 160), stories)
}

async fn write_paper(s: &AppState, today: NaiveDate) -> anyhow::Result<Option<Value>> {
    let yesterday = today - chrono::Duration::days(1);
    let st = day_stats(s, yesterday).await?;
    if st.blocks == 0 {
        return Ok(None);
    }
    let news: Vec<String> =
        sqlx::query_scalar("SELECT title FROM news_items ORDER BY published_at DESC LIMIT 5")
            .fetch_all(&s.db.pool)
            .await?;

    let mut facts = format!(
        "Edition of {today}. Yesterday ({yesterday}, UTC) on the Zcash chain:\n\
         - {} blocks mined, {} of them empty (only the miner's reward).\n\
         - {} transactions, not counting miner rewards.\n\
         - Fees paid: {} ZEC.\n",
        st.blocks,
        st.empty,
        st.txs,
        zec(st.fees)
    );
    if let Some(sh) = st.shielded_share {
        facts += &format!("- {}% of transactions had shielded parts.\n", pct(sh));
    }
    if let Some((h, size, n)) = st.biggest {
        facts += &format!(
            "- Biggest block: {h}, {} KB, {n} transactions.\n",
            size / 1000
        );
    }
    if let Some((pool, n)) = &st.top_pool {
        facts += &format!("- Top mining pool: {pool}, {n} blocks.\n");
    }
    facts += &format!(
        "Right now: {} transactions waiting in the mempool, {} ZEC in fees.\n",
        st.mempool,
        zec(st.mempool_fees)
    );
    if !news.is_empty() {
        facts += "Latest Zcash news headlines:\n";
        for t in &news {
            facts += &format!("- {}\n", llm::clip(t, 160));
        }
    }

    let (mut headline, mut subhead, mut stories) = paper_template(&st, &news);
    let mut weather = weather_template(&st);
    let system = "You are the editor of The ZStreet Daily, the newspaper of ZStreet, a voxel city \
        where zkSNARK residents carry real Zcash transactions and each mined block is a bus. Write \
        today's edition in English from the facts. Return strict JSON only: {\"headline\": str (at \
        most 80 characters), \"subhead\": str (at most 160 characters), \"stories\": [{\"title\": \
        str (at most 60 characters), \"body\": str (at most 280 characters)}] with 3 or 4 stories, \
        \"weather\": str (at most 140 characters)}. Stories cover yesterday's chain activity and the \
        news headlines, playful but accurate. weather is a playful one-line forecast for the city \
        derived from the mempool and fees (a crowded mempool means crowded streets). Use only the \
        facts given: never invent numbers, names, prices or events, and copy any number exactly as \
        written. No price predictions or financial advice, no markdown, no emojis.";
    if let Some(v) = llm::complete_json(&s.http, system, &facts, 1200).await {
        headline = field(&v["headline"], 80, &facts).unwrap_or(headline);
        subhead = field(&v["subhead"], 160, &facts).unwrap_or(subhead);
        weather = field(&v["weather"], 140, &facts).unwrap_or(weather);
        let mut written: Vec<Value> = v["stories"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|x| {
                Some(json!({
                    "title": field(&x["title"], 60, &facts)?,
                    "body": field(&x["body"], 280, &facts)?,
                }))
            })
            .take(4)
            .collect();
        for t in &stories {
            if written.len() >= 3 {
                break;
            }
            written.push(t.clone());
        }
        stories = written;
    }

    let (bh, bsize, btxs) = st
        .biggest
        .map_or((None, None, None), |(h, s, n)| (Some(h), Some(s), Some(n)));
    Ok(Some(json!({
        "day": today.to_string(),
        "headline": headline,
        "subhead": subhead,
        "stories": stories,
        "weather": weather,
        "stats": {
            "stats_day": yesterday.to_string(),
            "blocks": st.blocks,
            "empty_blocks": st.empty,
            "txs": st.txs,
            "shielded_share": st.shielded_share,
            "fees_zat": st.fees,
            "biggest_block_height": bh,
            "biggest_block_size": bsize,
            "biggest_block_txs": btxs,
            "top_pool": st.top_pool.as_ref().map(|p| p.0.clone()),
            "top_pool_blocks": st.top_pool.as_ref().map(|p| p.1),
            "mempool": st.mempool,
            "mempool_fees_zat": st.mempool_fees,
        },
    })))
}

#[derive(Deserialize)]
pub struct PaperQuery {
    day: Option<String>,
}

/// `GET /v1/street/paper?day=YYYY-MM-DD`: an edition of the ZStreet Daily (the latest by default).
pub async fn paper(
    State(s): State<AppState>,
    Query(q): Query<PaperQuery>,
) -> Result<Json<Value>, ApiError> {
    let data: Option<Value> = match q.day.as_deref().filter(|d| !d.is_empty()) {
        Some(d) => {
            let day = NaiveDate::parse_from_str(d, "%Y-%m-%d")
                .map_err(|_| bad_request("day must be YYYY-MM-DD"))?;
            sqlx::query_scalar("SELECT data FROM street_papers WHERE day = $1")
                .bind(day)
                .fetch_optional(&s.db.pool)
                .await?
        }
        None => {
            sqlx::query_scalar("SELECT data FROM street_papers ORDER BY day DESC LIMIT 1")
                .fetch_optional(&s.db.pool)
                .await?
        }
    };
    data.map(Json)
        .ok_or_else(|| ApiError::NotFound("No paper for that day".into()))
}

// ---------------------------------------------------------------------------------------------
// Live events

#[derive(Clone, Deserialize)]
struct EventDef {
    key: String,
    title: String,
    kind: String,
    #[serde(default)]
    place: Option<String>,
    #[serde(default)]
    weekly: Option<Weekly>,
    #[serde(default)]
    start_height: Option<u64>,
    #[serde(default)]
    start_time: Option<DateTime<Utc>>,
    duration_s: u32,
}

#[derive(Clone, Deserialize)]
struct Weekly {
    weekday: String,
    time: String,
}

#[derive(Deserialize)]
struct EventsFile {
    events: Vec<EventDef>,
}

#[derive(Serialize)]
pub struct Event {
    key: String,
    title: String,
    kind: String,
    place: Option<String>,
    start_height: Option<u64>,
    start_time: Option<DateTime<Utc>>,
    duration_s: u32,
}

/// `labels/street_events.json`, re-read from disk (`STREET_EVENTS_FILE`) at most every 5
/// minutes so events can change without a deploy; the copy built in is the fallback.
fn event_defs() -> Vec<EventDef> {
    static CACHE: Mutex<Option<(Instant, Vec<EventDef>)>> = Mutex::new(None);
    let mut c = CACHE.lock().unwrap();
    if let Some((at, defs)) = c.as_ref() {
        if at.elapsed() < Duration::from_secs(300) {
            return defs.clone();
        }
    }
    let path =
        std::env::var("STREET_EVENTS_FILE").unwrap_or_else(|_| "labels/street_events.json".into());
    let defs = std::fs::read_to_string(&path)
        .ok()
        .and_then(|t| match serde_json::from_str::<EventsFile>(&t) {
            Ok(f) => Some(f.events),
            Err(e) => {
                tracing::warn!("{path}: {e}");
                None
            }
        })
        .unwrap_or_else(|| {
            serde_json::from_str::<EventsFile>(include_str!("../../../labels/street_events.json"))
                .expect("labels/street_events.json is valid")
                .events
        });
    *c = Some((Instant::now(), defs.clone()));
    defs
}

/// The start of the current or next occurrence of a weekly event.
fn next_weekly(w: &Weekly, duration_s: u32, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let day: Weekday = w.weekday.parse().ok()?;
    let time = NaiveTime::parse_from_str(&w.time, "%H:%M").ok()?;
    let today = now.date_naive();
    (-7..=7)
        .map(|d| today + chrono::Duration::days(d))
        .filter(|d| chrono::Datelike::weekday(d) == day)
        .map(|d| d.and_time(time).and_utc())
        .find(|start| *start + chrono::Duration::seconds(duration_s as i64) > now)
}

/// `GET /v1/street/events`: concerts, tournaments, races and the NU7 party, next ones first.
pub async fn events(State(s): State<AppState>) -> Result<Json<Vec<Event>>, ApiError> {
    let now = Utc::now();
    let mut list: Vec<Event> = event_defs()
        .into_iter()
        .filter(|e| {
            matches!(
                e.kind.as_str(),
                "nu7" | "concert" | "tournament" | "race" | "party"
            )
        })
        .filter_map(|e| {
            let start_time = match &e.weekly {
                Some(w) => Some(next_weekly(w, e.duration_s, now)?),
                None => e.start_time,
            };
            Some(Event {
                key: e.key,
                title: e.title,
                kind: e.kind,
                place: e.place,
                start_height: e.start_height,
                start_time,
                duration_s: e.duration_s,
            })
        })
        .collect();
    // The NU7 party: from the activation height, for a day after it (25 s blocks).
    let tip = crate::routes::tip(&s.db).await?.unwrap_or(0).max(0) as u32;
    if let Some(h) = zscan_chain::upgrades::upgrades()
        .into_iter()
        .find(|u| u.name == "Nu7")
        .and_then(|u| u.activation_height)
        .filter(|h| tip < h + 3456)
    {
        let start_time = if tip < h {
            Some(
                now + chrono::Duration::seconds(
                    zscan_chain::upgrades::expected_seconds(tip, h) as i64
                ),
            )
        } else {
            sqlx::query_scalar("SELECT time FROM blocks WHERE height = $1")
                .bind(h as i32)
                .fetch_optional(&s.db.pool)
                .await?
        };
        list.push(Event {
            key: "nu7".into(),
            title: "NU7 activation party".into(),
            kind: "nu7".into(),
            place: None,
            start_height: Some(h as u64),
            start_time,
            duration_s: 86_400,
        });
    }
    list.sort_by_key(|e| e.start_time.unwrap_or(DateTime::<Utc>::MAX_UTC));
    Ok(Json(list))
}

// ---------------------------------------------------------------------------------------------
// Character memory

#[derive(Deserialize)]
pub struct Turn {
    role: String,
    content: String,
}

#[derive(Deserialize)]
pub struct MemoryPost {
    snark: i32,
    token: String,
    messages: Vec<Turn>,
}

#[derive(Deserialize)]
pub struct MemoryQuery {
    snark: Option<i32>,
    token: Option<String>,
}

/// Drops what looks like secrets or contact details: emails, long keys/addresses/hashes, phone
/// numbers. Seed phrases are left to the model's instructions.
fn scrub(text: &str) -> String {
    text.split_whitespace()
        .map(|w| {
            let alnum = w.chars().filter(char::is_ascii_alphanumeric).count();
            let digits = w.chars().filter(char::is_ascii_digit).count();
            if w.contains('@') && w.contains('.') {
                "[email]"
            } else if alnum >= 20 || digits >= 9 {
                "[redacted]"
            } else {
                w
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn snark_ok(n: i32) -> bool {
    (1..=10_000).contains(&n)
}

/// `GET /v1/street/memory?snark=&token=`: what a character remembers of this visitor.
pub async fn memory(
    State(s): State<AppState>,
    Query(q): Query<MemoryQuery>,
) -> Result<Response, ApiError> {
    let (Some(snark), Some(hash)) = (
        q.snark.filter(|n| snark_ok(*n)),
        q.token.as_deref().and_then(token_hash),
    ) else {
        return Err(bad_request(
            "snark (1-10000) and token (32-64 hex) are required",
        ));
    };
    let summary: Option<String> = sqlx::query_scalar(
        "SELECT summary FROM street_memories WHERE snark_id = $1 AND token_hash = $2",
    )
    .bind(snark)
    .bind(hash)
    .fetch_optional(&s.db.pool)
    .await?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({ "summary": summary })),
    )
        .into_response())
}

/// `POST /v1/street/memory`: folds a conversation into what the character remembers.
pub async fn remember(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<MemoryPost>,
) -> Result<Response, ApiError> {
    static LIMIT: OnceLock<DefaultKeyedRateLimiter<IpAddr>> = OnceLock::new();
    let Some(hash) = token_hash(&req.token).filter(|_| snark_ok(req.snark)) else {
        return Err(bad_request(
            "snark (1-10000) and token (32-64 hex) are required",
        ));
    };
    let turns: Vec<(&str, String)> = req
        .messages
        .iter()
        .rev()
        .take(12)
        .rev()
        .filter(|t| t.role == "user" || t.role == "assistant")
        .map(|t| (t.role.as_str(), scrub(&llm::clip(&t.content, 500))))
        .filter(|(_, c)| !c.is_empty())
        .collect();
    if !turns.iter().any(|(r, _)| *r == "user") {
        return Err(bad_request("Nothing to remember"));
    }
    if let Some(wait) =
        crate::limited_for(LIMIT.get_or_init(|| per_minute(6)), &ip_of(peer, &headers))
    {
        return Ok(crate::too_many_requests(
            wait,
            "Give them a moment to remember",
        ));
    }
    let previous: Option<String> = sqlx::query_scalar(
        "SELECT summary FROM street_memories WHERE snark_id = $1 AND token_hash = $2",
    )
    .bind(req.snark)
    .bind(&hash)
    .fetch_optional(&s.db.pool)
    .await?;

    let convo: Vec<String> = turns
        .iter()
        .map(|(r, c)| {
            format!(
                "{}: {c}",
                if *r == "user" { "visitor" } else { "character" }
            )
        })
        .collect();
    let user = format!(
        "Previous memory: {}\n\nNew conversation:\n{}",
        previous.as_deref().unwrap_or("(none)"),
        convo.join("\n")
    );
    let system = format!(
        "You keep the memory of zkSNARK #{}, a resident of ZStreet (a voxel city of Zcash \
         transactions). Merge the previous memory and the new conversation into one short note, in \
         English, at most 300 characters, of what this visitor talked about with the character \
         (topics, questions, preferences they shared), written like \"The visitor asked about…\". \
         Keep nothing personal beyond what was said, and never keep keys, seed words, passwords, \
         addresses, emails or phone numbers. Reply with the note only.",
        req.snark
    );
    let written = llm::complete(&s.http, &system, &user, 200)
        .await
        .map(|t| fit(&scrub(t.trim_matches('"')), 300))
        .filter(|t| !t.is_empty());
    let summary = written.unwrap_or_else(|| {
        let last = turns
            .iter()
            .rev()
            .find(|(r, _)| *r == "user")
            .map_or("", |(_, c)| c.as_str());
        let now = format!("The visitor talked about: {}", fit(last, 120));
        match &previous {
            Some(p) => fit(&format!("{now}. Earlier: {p}"), 300),
            None => fit(&now, 300),
        }
    });
    sqlx::query(
        "INSERT INTO street_memories (snark_id, token_hash, summary) VALUES ($1, $2, $3)
         ON CONFLICT (snark_id, token_hash) DO UPDATE SET summary = EXCLUDED.summary, updated_at = now()",
    )
    .bind(req.snark)
    .bind(&hash)
    .bind(&summary)
    .execute(&s.db.pool)
    .await?;
    Ok(Json(json!({ "summary": summary })).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decree_effects() {
        let flat = vec![1000; 40];
        // 12 of 40 empty = 30%
        assert_eq!(decree_effect(12, 0.9, &flat, &flat), "curfew");
        assert_eq!(decree_effect(11, 0.6, &flat, &flat), "festival");
        assert_eq!(decree_effect(0, 0.2, &flat, &flat), "quiet");
        let prev: Vec<i64> = (1..=40).map(|i| i * 100).collect();
        // top quartile of prev starts at 3100
        assert_eq!(decree_effect(0, 0.2, &vec![3200; 40], &prev), "treasure");
        assert_eq!(decree_effect(0, 0.2, &vec![3000; 40], &prev), "quiet");
        assert_eq!(decree_effect(0, 0.2, &[], &prev), "quiet");
        assert_eq!(decree_effect(0, 0.2, &flat, &[]), "quiet");
    }

    #[test]
    fn decree_templates_fit() {
        for e in ["curfew", "festival", "treasure", "quiet"] {
            let (t, x) = decree_template(e, 39, 0.99, 123_456_789, 3_200_040);
            assert!(t.chars().count() <= 40 && x.chars().count() <= 200, "{e}");
        }
    }

    #[test]
    fn weekly_next_occurrence() {
        let w = Weekly {
            weekday: "sat".into(),
            time: "20:00".into(),
        };
        // Thursday 2026-10-08
        let now = "2026-10-08T12:00:00Z".parse().unwrap();
        assert_eq!(
            next_weekly(&w, 3600, now).unwrap().to_rfc3339(),
            "2026-10-10T20:00:00+00:00"
        );
        // during the concert: the current one
        let now = "2026-10-10T20:30:00Z".parse().unwrap();
        assert_eq!(
            next_weekly(&w, 3600, now).unwrap().to_rfc3339(),
            "2026-10-10T20:00:00+00:00"
        );
        let now = "2026-10-10T21:00:00Z".parse().unwrap();
        assert_eq!(
            next_weekly(&w, 3600, now).unwrap().to_rfc3339(),
            "2026-10-17T20:00:00+00:00"
        );
    }

    #[test]
    fn embedded_events_parse() {
        let f: EventsFile =
            serde_json::from_str(include_str!("../../../labels/street_events.json")).unwrap();
        assert_eq!(f.events.len(), 3);
    }

    #[test]
    fn scrub_secrets() {
        let s = scrub("mail me at bob@example.com, my key is zxviews1qqqqqqqqqqqqqqqqqqqqqqqq ok");
        assert_eq!(s, "mail me at [email] my key is [redacted] ok");
    }

    #[test]
    fn tokens() {
        assert!(token_hash(&"ab".repeat(16)).is_some());
        assert!(token_hash(&"ab".repeat(33)).is_none());
        assert!(token_hash(&"zz".repeat(16)).is_none());
        assert!(token_hash("abc").is_none());
    }

    #[test]
    fn gossip_template_shape() {
        let f = BlockFacts {
            height: 3_101_240,
            txs: 12,
            size: 45_000,
            fees: Some(120_000),
            pool: Some("ViaBTC".into()),
            gap: Some(80),
            kinds: vec![("shielded".into(), 8), ("transparent".into(), 4)],
            mempool: 3,
            price: Some((41.5, -2.25)),
            shielded_supply: Some(0.31),
        };
        let lines = gossip_template(&f);
        assert!((6..=10).contains(&lines.len()));
        assert!(lines.iter().all(|l| l.text.chars().count() <= 60));
        let facts = f.text();
        assert!(
            lines.iter().all(|l| llm::numbers_from(&l.text, &facts)),
            "{facts}"
        );
    }
}
