//! ZStreet's city works: the city builds itself. Every round of blocks three of the day's agents
//! (see [`crate::street_agents`]) come up as architects, each with a building for the next site,
//! and the rest of the cast back one of them. Visitors vote during the round (later only zkSNARK
//! holders, with a proof); at the round's end the winner goes up on the next site of the agents'
//! quarter, a little more with every mined block: an empty block stops the works, a packed one
//! counts double. One model call per round writes the proposals; templates stand in without it.

use std::net::{IpAddr, SocketAddr};
use std::sync::OnceLock;
use std::time::Duration;

use axum::{
    extract::{ConnectInfo, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    Json,
};
use chrono::Utc;
use governor::DefaultKeyedRateLimiter;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::ApiError;
use crate::street::{self, bad_request, Flights};
use crate::street_agents::{cast_of, mix, persona};
use crate::street_llm::{self as llm, field};
use crate::AppState;

/// Blocks per round (about twelve hours).
pub(crate) const ROUND: u64 = 576;
/// Sites in the agents' quarter; once all are built the oldest is rebuilt.
const SITES: i64 = 10;
const STYLES: &[&str] = &["tower", "deco", "pagoda", "market", "garden", "twist"];
const WALLS: &[&str] = &[
    "white", "stone", "concrete", "slate", "night", "brick", "wood", "red", "gold", "green",
    "blue", "violet", "pink", "teal",
];
const GLASSES: &[&str] = &["sky", "dark", "gold", "green", "violet"];
const NEONS: &[&str] = &["cyan", "pink", "violet", "green", "gold", "white", "red"];
const CROWNS: &[&str] = &[
    "spire",
    "antenna",
    "dome",
    "garden",
    "helipad",
    "billboard",
    "flat",
];
const NOUNS: &[&str] = &[
    "Tower", "Hall", "Market", "Pagoda", "Terraces", "Exchange", "Heights", "Arcade", "House",
    "Spire",
];

/// Floors a style may have, within the round's budget.
fn floors_for(style: &str, budget: i64) -> (i64, i64) {
    match style {
        "market" => (2, 5),
        "pagoda" => (3, 9.min(budget)),
        _ => (5, budget.max(5)),
    }
}

/// Work units a building needs: three per floor and four for its crown.
fn need(plan: &Value) -> i64 {
    plan["floors"].as_i64().unwrap_or(5) * 3 + 4
}

fn pick<'a>(list: &[&'a str], seed: u64, salt: u64) -> &'a str {
    list[(mix(seed.wrapping_mul(31).wrapping_add(salt)) % list.len() as u64) as usize]
}

fn one_of(v: &Value, list: &[&str]) -> Option<String> {
    let s = v.as_str()?.trim().to_ascii_lowercase();
    list.contains(&s.as_str()).then_some(s)
}

struct Stats {
    blocks: i64,
    txs: i64,
    empty: i64,
    packed: i64,
}

impl Stats {
    /// The tallest a building may go this round: busier chain, bigger works.
    fn budget(&self) -> i64 {
        (6 + self.txs / 250 - self.empty / 20).clamp(6, 24)
    }

    fn text(&self, from: u64, to: u64) -> String {
        format!(
            "Blocks {from} to {to}: {} blocks, {} transactions, {} empty blocks, {} packed blocks \
             (15 or more transactions).",
            self.blocks, self.txs, self.empty, self.packed
        )
    }
}

async fn stats(s: &AppState, from: u64, to: u64) -> anyhow::Result<Stats> {
    let (blocks, txs, empty, packed): (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT count(*), COALESCE(sum(tx_count - 1), 0)::int8,
                count(*) FILTER (WHERE tx_count <= 1), count(*) FILTER (WHERE tx_count - 1 >= 15)
         FROM blocks WHERE height >= $1 AND height < $2",
    )
    .bind(from as i32)
    .bind(to as i32)
    .fetch_one(&s.db.pool)
    .await?;
    Ok(Stats {
        blocks,
        txs,
        empty,
        packed,
    })
}

struct Project {
    architect: i32,
    name: String,
    pitch: String,
    plan: Value,
}

/// A building drawn from the seed alone.
fn template_project(round: u64, idx: u64, architect: i32, budget: i64) -> Project {
    let seed = round * 3 + idx;
    let style = pick(STYLES, seed, 1);
    let (lo, hi) = floors_for(style, budget);
    let floors = lo + (mix(seed + 7) % (hi - lo + 1) as u64) as i64;
    let who = persona(architect);
    let noun = match style {
        "market" => "Market",
        "pagoda" => "Pagoda",
        "garden" => "Terraces",
        _ => pick(NOUNS, seed, 2),
    };
    Project {
        architect,
        name: format!("{}'s {noun}", who.name),
        pitch: format!(
            "A {style} of {floors} floors by {}, {}, for the agents' quarter.",
            who.name, who.job
        ),
        plan: json!({
            "style": style,
            "floors": floors,
            "wall": pick(WALLS, seed, 3),
            "glass": pick(GLASSES, seed, 4),
            "neon": pick(NEONS, seed, 5),
            "crown": pick(CROWNS, seed, 6),
            "sign": who.name.to_uppercase(),
            "seed": mix(seed + 11) % 1_000_000,
        }),
    }
}

/// A proposal from the model's answer, checked and clamped.
fn model_project(
    x: &Value,
    architect: i32,
    seed: u64,
    budget: i64,
    facts: &str,
) -> Option<Project> {
    let name = field(&x["name"], 28, facts)?;
    let pitch = field(&x["pitch"], 150, facts)?;
    let style = one_of(&x["style"], STYLES)?;
    let (lo, hi) = floors_for(&style, budget);
    let floors = x["floors"].as_i64().unwrap_or(hi).clamp(lo, hi);
    let sign: String = x["sign"]
        .as_str()
        .unwrap_or("")
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == ' ' || *c == '-')
        .take(14)
        .collect::<String>()
        .trim()
        .to_uppercase();
    Some(Project {
        architect,
        name,
        pitch,
        plan: json!({
            "style": style,
            "floors": floors,
            "wall": one_of(&x["wall"], WALLS).unwrap_or_else(|| pick(WALLS, seed, 3).into()),
            "glass": one_of(&x["glass"], GLASSES).unwrap_or_else(|| pick(GLASSES, seed, 4).into()),
            "neon": one_of(&x["neon"], NEONS).unwrap_or_else(|| pick(NEONS, seed, 5).into()),
            "crown": one_of(&x["crown"], CROWNS).unwrap_or_else(|| pick(CROWNS, seed, 6).into()),
            "sign": if sign.is_empty() { persona(architect).name.to_uppercase() } else { sign },
            "seed": mix(seed + 11) % 1_000_000,
        }),
    })
}

/// Writes round `round`'s three proposals and who in the cast backs which.
async fn propose(s: &AppState, round: u64) -> anyhow::Result<()> {
    let from = round.saturating_sub(1) * ROUND;
    let st = stats(s, from, from + ROUND).await?;
    let budget = st.budget();
    let day = Utc::now().timestamp() / 86_400;
    let cast = cast_of(day);
    let architects: Vec<i32> = (0..3)
        .map(|k| cast[((round * 3 + k) % cast.len() as u64) as usize])
        .collect();
    let built: Vec<String> = sqlx::query_scalar(
        "SELECT p.name FROM street_sites s JOIN street_projects p USING (round, idx)
         ORDER BY s.round DESC LIMIT 12",
    )
    .fetch_all(&s.db.pool)
    .await?;
    let facts = format!(
        "{}\nThe tallest anyone may build this round: {budget} floors.\nAlready built in the \
         quarter: {}.\n\nArchitects (one project each, in this order):\n{}\n\nThe rest of the \
         cast:\n{}",
        st.text(from, from + ROUND),
        if built.is_empty() {
            "nothing yet".into()
        } else {
            built.join(", ")
        },
        architects
            .iter()
            .map(|a| persona(*a).line())
            .collect::<Vec<_>>()
            .join("\n"),
        cast.iter()
            .filter(|c| !architects.contains(c))
            .map(|c| persona(*c).line())
            .collect::<Vec<_>>()
            .join("\n"),
    );
    let system = format!(
        "ZStreet is a voxel city of pixel-art zkSNARK characters, run by the Zcash chain. The city \
        builds itself: every round three citizens propose a building for the next site of the \
        agents' quarter (a plot on Central Park's east side), visitors vote, and the winner goes \
        up block by block. Write each architect's proposal in their own character, drawing on \
        their job, temperament and the round's chain facts. Make the three clearly different. \
        name: at most 28 characters, a real building name. pitch: at most 150 characters, why \
        the city needs it, in the architect's voice. style: one of {styles}. floors: a number \
        (market 2-5, pagoda 3-9, others 5 up to the round's limit). wall: one of {walls}. glass: \
        one of {glasses}. neon: one of {neons}. crown: one of {crowns}. sign: the neon sign on \
        it, at most 14 letters. Then for every other cast member, which project they back (0, 1 \
        or 2) and one line (at most 80 characters) saying why, in character. English only. Use \
        only facts given, copy numbers exactly, no prices, no financial advice, no hashtags, no \
        emojis. Return strict JSON only: {{\"projects\":[{{\"name\":\"...\",\"pitch\":\"...\",\
        \"style\":\"tower\",\"floors\":12,\"wall\":\"...\",\"glass\":\"...\",\"neon\":\"...\",\
        \"crown\":\"...\",\"sign\":\"...\"}}],\"backers\":[{{\"id\":0,\"project\":0,\"line\":\"...\"}}]}}",
        styles = STYLES.join(", "),
        walls = WALLS.join(", "),
        glasses = GLASSES.join(", "),
        neons = NEONS.join(", "),
        crowns = CROWNS.join(", "),
    );
    let v = llm::complete_json(&s.http, &system, &facts, 1800).await;
    let answers = v.as_ref().and_then(|v| v["projects"].as_array());
    let projects: Vec<Project> = architects
        .iter()
        .enumerate()
        .map(|(k, a)| {
            answers
                .and_then(|l| l.get(k))
                .and_then(|x| model_project(x, *a, round * 3 + k as u64, budget, &facts))
                .unwrap_or_else(|| template_project(round, k as u64, *a, budget))
        })
        .collect();
    let mut backers: Vec<Vec<Value>> = vec![Vec::new(); 3];
    for b in v
        .as_ref()
        .and_then(|v| v["backers"].as_array())
        .into_iter()
        .flatten()
    {
        let (Some(id), Some(p)) = (b["id"].as_i64(), b["project"].as_u64()) else {
            continue;
        };
        let id = id as i32;
        if p > 2 || !cast.contains(&id) || architects.contains(&id) {
            continue;
        }
        if backers.iter().flatten().any(|x| x["id"] == id) {
            continue;
        }
        if let Some(line) = field(&b["line"], 90, &facts) {
            backers[p as usize].push(json!({ "id": id, "line": line }));
        }
    }
    if backers.iter().all(Vec::is_empty) {
        for c in cast.iter().filter(|c| !architects.contains(c)) {
            let p = (mix(round + *c as u64) % 3) as usize;
            let line = format!("{} gets my vote.", projects[p].name);
            backers[p].push(json!({ "id": c, "line": line }));
        }
    }
    let mut tx = s.db.pool.begin().await?;
    for (k, (p, b)) in projects.iter().zip(&backers).enumerate() {
        sqlx::query(
            "INSERT INTO street_projects (round, idx, architect, name, pitch, plan, backers)
             VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING",
        )
        .bind(round as i32)
        .bind(k as i16)
        .bind(p.architect)
        .bind(&p.name)
        .bind(&p.pitch)
        .bind(&p.plan)
        .bind(Value::from(b.clone()))
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    tracing::info!(round, "street works: proposals written");
    Ok(())
}

/// Picks round `round`'s winner (most votes, then most backers, then the first) and opens its
/// site at `start`.
async fn award(s: &AppState, round: u64, start: u64) -> anyhow::Result<()> {
    let rows: Vec<(i16, i64, i64)> = sqlx::query_as(
        "SELECT p.idx, (SELECT count(*) FROM street_votes v WHERE v.round = p.round AND v.idx = p.idx)
                + (SELECT count(*) FROM street_sealed_votes v WHERE v.round = p.round AND v.idx = p.idx),
                jsonb_array_length(p.backers)::int8
         FROM street_projects p WHERE p.round = $1 ORDER BY p.idx",
    )
    .bind(round as i32)
    .fetch_all(&s.db.pool)
    .await?;
    let Some(&(idx, votes, _)) = rows
        .iter()
        .max_by(|a, b| (a.1, a.2, -a.0).cmp(&(b.1, b.2, -b.0)))
    else {
        return Ok(());
    };
    let built: i64 = sqlx::query_scalar("SELECT count(*) FROM street_sites")
        .fetch_one(&s.db.pool)
        .await?;
    sqlx::query(
        "INSERT INTO street_sites (round, idx, site, start_height, votes) VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT DO NOTHING",
    )
    .bind(round as i32)
    .bind(idx)
    .bind((built % SITES) as i16)
    .bind(start as i32)
    .bind(votes as i32)
    .execute(&s.db.pool)
    .await?;
    crate::presence::broadcast(json!({ "type": "works" }).to_string());
    tracing::info!(round, idx, votes, "street works: site opened");
    Ok(())
}

async fn tick(s: &AppState) -> anyhow::Result<()> {
    let Ok(tip) = street::tip(s).await else {
        return Ok(());
    };
    let round = tip / ROUND;
    let any: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM street_sites)")
        .fetch_one(&s.db.pool)
        .await?;
    if !any && round > 0 {
        // the very first works: last round's proposals, already under way
        propose(s, round - 1).await?;
        award(s, round - 1, tip.saturating_sub(8)).await?;
    }
    let have: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM street_projects WHERE round = $1)")
            .bind(round as i32)
            .fetch_one(&s.db.pool)
            .await?;
    if !have {
        propose(s, round).await?;
    }
    let due: Vec<i32> = sqlx::query_scalar(
        "SELECT DISTINCT p.round FROM street_projects p
         WHERE p.round < $1 AND NOT EXISTS (SELECT 1 FROM street_sites s WHERE s.round = p.round)
         ORDER BY p.round",
    )
    .bind(round as i32)
    .fetch_all(&s.db.pool)
    .await?;
    for r in due {
        award(s, r as u64, (r as u64 + 1) * ROUND).await?;
    }
    Ok(())
}

/// Every 30 seconds: proposals for the round under way, and the sites of rounds that ended.
pub async fn run(s: AppState) {
    loop {
        if let Err(e) = tick(&s).await {
            tracing::warn!("street works: {e:#}");
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

async fn snapshot(s: &AppState) -> Result<Value, ApiError> {
    let tip = street::tip(s).await?;
    let round = tip / ROUND;
    type P = (i16, i32, String, String, Value, Value, i64, i64);
    let projects: Vec<P> = sqlx::query_as(
        "SELECT p.idx, p.architect, p.name, p.pitch, p.plan, p.backers,
                (SELECT count(*) FROM street_votes v WHERE v.round = p.round AND v.idx = p.idx),
                (SELECT count(*) FROM street_sealed_votes v WHERE v.round = p.round AND v.idx = p.idx)
         FROM street_projects p WHERE p.round = $1 ORDER BY p.idx",
    )
    .bind(round as i32)
    .fetch_all(&s.db.pool)
    .await?;
    type S = (
        i32,
        i16,
        i16,
        i32,
        i32,
        i32,
        String,
        String,
        Value,
        Value,
        i64,
    );
    let sites: Vec<S> = sqlx::query_as(
        "SELECT * FROM (
             SELECT DISTINCT ON (s.site) s.round, s.idx, s.site, s.start_height, s.votes,
                    p.architect, p.name, p.pitch, p.plan, p.backers,
                    (SELECT COALESCE(sum(CASE WHEN b.tx_count <= 1 THEN 0
                                              WHEN b.tx_count - 1 >= 15 THEN 2 ELSE 1 END), 0)::int8
                     FROM blocks b WHERE b.height > s.start_height AND b.height <= $1)
             FROM street_sites s JOIN street_projects p USING (round, idx)
             ORDER BY s.site, s.round DESC) x ORDER BY site",
    )
    .bind(tip as i32)
    .fetch_all(&s.db.pool)
    .await?;
    let tip_empty: bool = sqlx::query_scalar(
        "SELECT COALESCE(bool_or(tx_count <= 1), false) FROM blocks WHERE height = $1",
    )
    .bind(tip as i32)
    .fetch_one(&s.db.pool)
    .await?;
    let who = |id: i32| {
        let p = persona(id);
        json!({ "id": id, "name": p.name, "job": p.job })
    };
    let backed = |backers: &Value| -> Vec<Value> {
        backers
            .as_array()
            .into_iter()
            .flatten()
            .map(|b| {
                let mut w = who(b["id"].as_i64().unwrap_or(0) as i32);
                w["line"] = b["line"].clone();
                w
            })
            .collect()
    };
    Ok(json!({
        "tip": tip,
        "round": round,
        "start": round * ROUND,
        "end": (round + 1) * ROUND,
        "stopped": tip_empty,
        "proposals": projects.into_iter().map(|(idx, a, name, pitch, plan, backers, votes, sealed)| {
            json!({ "idx": idx, "architect": who(a), "name": name, "pitch": pitch, "plan": plan,
                    "backers": backed(&backers), "votes": votes, "sealed": sealed })
        }).collect::<Vec<_>>(),
        "sites": sites.into_iter().map(|(r, idx, site, start, votes, a, name, pitch, plan, backers, work)| {
            let need = need(&plan);
            json!({ "site": site, "round": r, "idx": idx, "start": start, "votes": votes,
                    "architect": who(a), "name": name, "pitch": pitch, "plan": plan,
                    "crew": backed(&backers), "work": work.min(need), "need": need })
        }).collect::<Vec<_>>(),
    }))
}

/// `GET /v1/street/works`: the round's proposals and votes, and every site with its progress.
pub async fn works(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    static LIMIT: OnceLock<DefaultKeyedRateLimiter<IpAddr>> = OnceLock::new();
    static CACHE: Flights<Value> = Flights::new(4);
    if let Some(wait) = crate::limited_for(
        LIMIT.get_or_init(|| street::per_minute(30)),
        &street::ip_of(peer, &headers),
    ) {
        return Ok(crate::too_many_requests(wait, "Slow down"));
    }
    // shared for a few seconds: votes show up quickly, the database isn't hit per visitor
    let key = (Utc::now().timestamp() / 5) as u64;
    let cell = CACHE.cell(key);
    let v = cell.get_or_try_init(|| snapshot(&s)).await?;
    Ok(Json(v.clone()).into_response())
}

#[derive(Deserialize)]
pub struct Vote {
    round: u64,
    idx: i16,
    token: String,
}

/// `POST /v1/street/works/vote`: one vote per visitor per round (a new one replaces it).
pub async fn vote(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<Vote>,
) -> Result<Response, ApiError> {
    static LIMIT: OnceLock<DefaultKeyedRateLimiter<IpAddr>> = OnceLock::new();
    if let Some(wait) = crate::limited_for(
        LIMIT.get_or_init(|| street::per_minute(10)),
        &street::ip_of(peer, &headers),
    ) {
        return Ok(crate::too_many_requests(wait, "Slow down"));
    }
    let hash =
        street::token_hash(&req.token).ok_or_else(|| bad_request("token must be 32-64 hex"))?;
    let tip = street::tip(&s).await?;
    if req.round != tip / ROUND {
        return Err(bad_request("voting for that round is closed"));
    }
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM street_projects WHERE round = $1 AND idx = $2)",
    )
    .bind(req.round as i32)
    .bind(req.idx)
    .fetch_one(&s.db.pool)
    .await?;
    if !exists {
        return Err(bad_request("no such project"));
    }
    sqlx::query(
        "INSERT INTO street_votes (round, token_hash, idx) VALUES ($1, $2, $3)
         ON CONFLICT (round, token_hash) DO UPDATE SET idx = EXCLUDED.idx, updated_at = now()",
    )
    .bind(req.round as i32)
    .bind(&hash)
    .bind(req.idx)
    .execute(&s.db.pool)
    .await?;
    let votes: Vec<(i16, i64)> = sqlx::query_as(
        "SELECT idx, count(*) FROM street_votes WHERE round = $1 GROUP BY idx ORDER BY idx",
    )
    .bind(req.round as i32)
    .fetch_all(&s.db.pool)
    .await?;
    crate::presence::broadcast(json!({ "type": "works" }).to_string());
    Ok(Json(json!({
        "round": req.round,
        "idx": req.idx,
        "votes": votes.into_iter().map(|(i, n)| json!({ "idx": i, "votes": n })).collect::<Vec<_>>(),
    }))
    .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn templates_fit_their_style() {
        for round in 0..200 {
            for idx in 0..3 {
                let p = template_project(round, idx, 42, 24);
                let style = p.plan["style"].as_str().unwrap();
                let (lo, hi) = floors_for(style, 24);
                let f = p.plan["floors"].as_i64().unwrap();
                assert!((lo..=hi).contains(&f), "{style} {f}");
                assert!(p.name.chars().count() <= 28);
            }
        }
    }

    #[test]
    fn model_answers_are_clamped() {
        let x = json!({ "name": "Night Market", "pitch": "Food after dark.", "style": "MARKET",
                        "floors": 40, "wall": "plaid", "glass": "sky", "neon": "pink",
                        "crown": "flat", "sign": "yatai!! 屋台" });
        let p = model_project(&x, 7, 1, 24, "").unwrap();
        assert_eq!(p.plan["style"], "market");
        assert_eq!(p.plan["floors"], 5);
        assert!(WALLS.contains(&p.plan["wall"].as_str().unwrap()));
        assert_eq!(p.plan["sign"], "YATAI");
        assert!(model_project(
            &json!({ "name": "X", "pitch": "y", "style": "castle" }),
            7,
            1,
            24,
            ""
        )
        .is_none());
    }

    #[test]
    fn budget_follows_the_chain() {
        let quiet = Stats {
            blocks: 576,
            txs: 300,
            empty: 200,
            packed: 0,
        };
        let busy = Stats {
            blocks: 576,
            txs: 9000,
            empty: 0,
            packed: 40,
        };
        assert_eq!(quiet.budget(), 6);
        assert_eq!(busy.budget(), 24);
    }
}
