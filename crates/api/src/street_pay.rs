//! ZStreet's wallet layer. A visitor asks for an intent: what to do (a message left on the ground
//! where they stand, a guestbook line, a sealed City Works vote, a tip) and gets a ZIP-321 URI to
//! ZStreet's shielded address with `ZS1:<ref>` and their text in the memo. They pay from any
//! wallet (QR code, or address and memo copied); the indexer reads the memo with the address's
//! viewing key (`street_payments`), and here every few seconds the payments are played for
//! everyone: the message appears, the vote counts, fireworks go up. What is shown is the memo
//! as it arrived on chain, never the payer.

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::Engine;
use chrono::{DateTime, Utc};
use governor::DefaultKeyedRateLimiter;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::error::ApiError;
use crate::street::{self, bad_request, token_hash};
use crate::street_works::ROUND;
use crate::AppState;

const MARK_ZAT: i64 = 10_000;
const VOTE_ZAT: i64 = 100_000;
const TIP_ZAT: i64 = 10_000;
/// A tip at least this large puts its name on the buses for a day.
const SPONSOR_ZAT: i64 = 10_000_000;
const TEXT_MAX: usize = 300;
const NAME_MAX: usize = 24;
const REPORTS_TO_HIDE: i32 = 5;
const INTENT_HOURS: i64 = 24;
const PLACE_MAX: usize = 32;

fn address() -> Option<String> {
    std::env::var("STREET_PAY_ADDRESS")
        .ok()
        .map(|a| a.trim().to_string())
        .filter(|a| !a.is_empty())
}

fn min_zat(action: &str) -> Option<i64> {
    match action {
        "mark" | "guestbook" => Some(MARK_ZAT),
        "vote" => Some(VOTE_ZAT),
        "tip" => Some(TIP_ZAT),
        _ => None,
    }
}

/// Zatoshis as a ZIP-321 amount: decimal ZEC, no trailing zeros.
fn zec(zat: i64) -> String {
    let s = format!("{}.{:08}", zat / 100_000_000, zat % 100_000_000);
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

/// A fresh 8-character base32 reference (A-Z, 2-7), as the indexer reads it in memos.
fn new_ref(seed: &[u8]) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    const ABC: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut h = Sha256::new();
    h.update(seed);
    h.update(Utc::now().timestamp_nanos_opt().unwrap_or(0).to_le_bytes());
    h.update(N.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    let d = h.finalize();
    let bits = u64::from_le_bytes(d[..8].try_into().unwrap());
    (0..8)
        .map(|i| ABC[((bits >> (i * 5)) & 31) as usize] as char)
        .collect()
}

/// The visitor's text: printable, one blank line at most, trimmed to `max` characters.
fn clean(text: &str, max: usize) -> String {
    let t: String = text
        .chars()
        .filter(|c| *c == '\n' || !c.is_control())
        .collect::<String>()
        .split('\n')
        .map(str::trim)
        .collect::<Vec<_>>()
        .join("\n");
    let mut t = t.trim().to_string();
    while t.contains("\n\n\n") {
        t = t.replace("\n\n\n", "\n\n");
    }
    t.chars().take(max).collect()
}

/// The text of a paid memo: everything but the reference.
fn memo_text(memo: &str, r: &str, max: usize) -> String {
    clean(&memo.replacen(&format!("ZS1:{r}"), "", 1), max)
}

#[derive(Deserialize)]
pub struct IntentReq {
    token: String,
    action: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    place: Option<String>,
    #[serde(default)]
    floor: Option<i16>,
    #[serde(default)]
    x: Option<f32>,
    #[serde(default)]
    y: Option<f32>,
    #[serde(default)]
    z: Option<f32>,
    #[serde(default)]
    round: Option<u64>,
    #[serde(default)]
    idx: Option<i16>,
}

/// `POST /v1/street/pay/intent`: the reference and the ZIP-321 URI to pay for an action.
pub async fn intent(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<IntentReq>,
) -> Result<Response, ApiError> {
    static LIMIT: OnceLock<DefaultKeyedRateLimiter<IpAddr>> = OnceLock::new();
    let Some(addr) = address() else {
        return Err(ApiError::NotFound("ZStreet payments are off".into()));
    };
    let hash = token_hash(&req.token).ok_or_else(|| bad_request("token must be 32-64 hex"))?;
    let min = min_zat(&req.action).ok_or_else(|| bad_request("unknown action"))?;
    if let Some(wait) = crate::limited_for(
        LIMIT.get_or_init(|| street::per_minute(12)),
        &street::ip_of(peer, &headers),
    ) {
        return Ok(crate::too_many_requests(wait, "Slow down"));
    }
    let finite = |v: Option<f32>| {
        v.filter(|v| v.is_finite() && v.abs() < 10_000.0)
            .unwrap_or(0.0)
    };
    let (payload, text) = match req.action.as_str() {
        "mark" | "guestbook" => {
            let text = clean(&req.text, TEXT_MAX);
            if text.is_empty() {
                return Err(bad_request("write a message first"));
            }
            let place = req.place.as_deref().unwrap_or("street");
            if place.is_empty()
                || place.len() > PLACE_MAX
                || !place
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            {
                return Err(bad_request("bad place"));
            }
            (
                json!({ "place": place, "floor": req.floor.unwrap_or(0),
                        "x": finite(req.x), "y": finite(req.y), "z": finite(req.z) }),
                text,
            )
        }
        "vote" => {
            let (Some(round), Some(idx)) = (req.round, req.idx) else {
                return Err(bad_request("round and idx are required"));
            };
            let tip = street::tip(&s).await?;
            if round != tip / ROUND {
                return Err(bad_request("voting for that round is closed"));
            }
            let name: Option<String> = sqlx::query_scalar(
                "SELECT name FROM street_projects WHERE round = $1 AND idx = $2",
            )
            .bind(round as i32)
            .bind(idx)
            .fetch_optional(&s.db.pool)
            .await?;
            let Some(name) = name else {
                return Err(bad_request("no such project"));
            };
            (
                json!({ "round": round, "idx": idx }),
                clean(&format!("City Works vote: {name}"), TEXT_MAX),
            )
        }
        _ => (json!({}), clean(&req.text, NAME_MAX)),
    };
    let open: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM street_intents
         WHERE token_hash = $1 AND created_at > now() - interval '1 hour'",
    )
    .bind(&hash)
    .fetch_one(&s.db.pool)
    .await?;
    if open >= 30 {
        return Ok(crate::too_many_requests(
            Duration::from_secs(600),
            "Too many payment requests this hour",
        ));
    }
    let r = new_ref(&hash);
    let expires = Utc::now() + chrono::Duration::hours(INTENT_HOURS);
    sqlx::query(
        "INSERT INTO street_intents (ref, action, payload, token_hash, min_zat, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(&r)
    .bind(&req.action)
    .bind(&payload)
    .bind(&hash)
    .bind(min)
    .bind(expires)
    .execute(&s.db.pool)
    .await?;
    let memo = if text.is_empty() {
        format!("ZS1:{r}")
    } else {
        format!("ZS1:{r}\n{text}")
    };
    let memo64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(memo.as_bytes());
    let amount = zec(min);
    Ok(Json(json!({
        "ref": r,
        "action": req.action,
        "address": addr,
        "amount": amount,
        "memo": memo,
        "uri": format!("zcash:{addr}?amount={amount}&memo={memo64}"),
        "expires_at": expires,
    }))
    .into_response())
}

/// `GET /v1/street/pay/{ref}`: waiting, confirmed (with what it became), or short.
pub async fn status(
    State(s): State<AppState>,
    Path(r): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let intent: Option<(String, i64, DateTime<Utc>)> =
        sqlx::query_as("SELECT action, min_zat, expires_at FROM street_intents WHERE ref = $1")
            .bind(&r)
            .fetch_optional(&s.db.pool)
            .await?;
    let Some((action, min, expires)) = intent else {
        return Err(ApiError::NotFound("No such payment request".into()));
    };
    let paid: Option<(String, i32, i64, bool, Option<String>)> = sqlx::query_as(
        "SELECT txid, height, value, applied, outcome FROM street_payments
         WHERE ref = $1 ORDER BY height LIMIT 1",
    )
    .bind(&r)
    .fetch_optional(&s.db.pool)
    .await?;
    let mark: Option<i64> = sqlx::query_scalar("SELECT id FROM street_marks WHERE ref = $1")
        .bind(&r)
        .fetch_optional(&s.db.pool)
        .await?;
    let state = match &paid {
        None if expires < Utc::now() => "expired",
        None => "waiting",
        Some((_, _, _, false, _)) => "seen",
        Some((_, _, _, true, Some(o))) if o == "short" => "short",
        Some(_) => "confirmed",
    };
    Ok(Json(json!({
        "ref": r,
        "action": action,
        "min": zec(min),
        "state": state,
        "txid": paid.as_ref().map(|p| &p.0),
        "height": paid.as_ref().map(|p| p.1),
        "amount": paid.as_ref().map(|p| zec(p.2)),
        "mark": mark,
    })))
}

/// `GET /v1/street/pay`: the address, the minimums, whether payments are on.
pub async fn info() -> Json<Value> {
    let addr = address();
    // the phones' static QR code: a plain tip, no reference needed
    let tip_uri = addr.as_ref().map(|a| {
        let memo = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode("Tip for ZStreet");
        format!("zcash:{a}?amount={}&memo={memo}", zec(TIP_ZAT * 10))
    });
    Json(json!({
        "enabled": addr.is_some(),
        "address": addr,
        "tip_uri": tip_uri,
        "min": { "mark": zec(MARK_ZAT), "guestbook": zec(MARK_ZAT), "vote": zec(VOTE_ZAT), "tip": zec(TIP_ZAT) },
        "sponsor": zec(SPONSOR_ZAT),
    }))
}

#[derive(Serialize, sqlx::FromRow)]
pub struct Mark {
    id: i64,
    kind: String,
    place: String,
    floor: i16,
    x: f32,
    y: f32,
    z: f32,
    text: String,
    txid: String,
    height: i32,
    likes: i32,
    created_at: DateTime<Utc>,
}

#[derive(Deserialize)]
pub struct MarksQ {
    place: Option<String>,
}

/// `GET /v1/street/marks?place=`: the messages on the ground (all places when not given).
pub async fn marks(
    State(s): State<AppState>,
    Query(q): Query<MarksQ>,
) -> Result<Json<Vec<Mark>>, ApiError> {
    Ok(Json(
        sqlx::query_as(
            "SELECT id, kind, place, floor, x, y, z, text, txid, height, likes, created_at
             FROM street_marks WHERE NOT hidden AND kind = 'mark' AND ($1::text IS NULL OR place = $1)
             ORDER BY id DESC LIMIT 600",
        )
        .bind(&q.place)
        .fetch_all(&s.db.pool)
        .await?,
    ))
}

/// `GET /v1/street/guestbook`: the newest lines of the Reserve's guestbook.
pub async fn guestbook(State(s): State<AppState>) -> Result<Json<Vec<Mark>>, ApiError> {
    Ok(Json(
        sqlx::query_as(
            "SELECT id, kind, place, floor, x, y, z, text, txid, height, likes, created_at
             FROM street_marks WHERE NOT hidden AND kind = 'guestbook' ORDER BY id DESC LIMIT 40",
        )
        .fetch_all(&s.db.pool)
        .await?,
    ))
}

/// `GET /v1/street/tips`: the latest tips and today's sponsors (names for the buses).
pub async fn tips(State(s): State<AppState>) -> Result<Json<Value>, ApiError> {
    let recent: Vec<(Option<String>, i64, i32, DateTime<Utc>)> = sqlx::query_as(
        "SELECT name, zat, height, created_at FROM street_tips ORDER BY created_at DESC LIMIT 20",
    )
    .fetch_all(&s.db.pool)
    .await?;
    let sponsors: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM street_tips WHERE zat >= $1 AND name IS NOT NULL AND name <> ''
         AND created_at > now() - interval '1 day' ORDER BY zat DESC LIMIT 12",
    )
    .bind(SPONSOR_ZAT)
    .fetch_all(&s.db.pool)
    .await?;
    Ok(Json(json!({
        "recent": recent.into_iter().map(|(n, z, h, t)| json!({ "name": n, "amount": zec(z), "height": h, "at": t })).collect::<Vec<_>>(),
        "sponsors": sponsors,
    })))
}

#[derive(Deserialize)]
pub struct Who {
    token: String,
}

/// `POST /v1/street/marks/{id}/like` and `/report`, once per visitor; reports hide a message.
pub async fn rate(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path((id, kind)): Path<(i64, String)>,
    Json(req): Json<Who>,
) -> Result<Response, ApiError> {
    static LIMIT: OnceLock<DefaultKeyedRateLimiter<IpAddr>> = OnceLock::new();
    if kind != "like" && kind != "report" {
        return Err(ApiError::NotFound("Not found".into()));
    }
    let hash = token_hash(&req.token).ok_or_else(|| bad_request("token must be 32-64 hex"))?;
    if let Some(wait) = crate::limited_for(
        LIMIT.get_or_init(|| street::per_minute(30)),
        &street::ip_of(peer, &headers),
    ) {
        return Ok(crate::too_many_requests(wait, "Slow down"));
    }
    let new = sqlx::query(
        "INSERT INTO street_mark_votes (mark_id, token_hash, kind)
         SELECT $1, $2, $3 WHERE EXISTS (SELECT 1 FROM street_marks WHERE id = $1)
         ON CONFLICT DO NOTHING",
    )
    .bind(id)
    .bind(&hash)
    .bind(&kind)
    .execute(&s.db.pool)
    .await?
    .rows_affected()
        > 0;
    if new {
        let col = if kind == "like" { "likes" } else { "reports" };
        let (likes, hidden): (i32, bool) = sqlx::query_as(&format!(
            "UPDATE street_marks SET {col} = {col} + 1,
                 hidden = hidden OR (reports + CASE WHEN $2 THEN 1 ELSE 0 END) >= $3
             WHERE id = $1 RETURNING likes, hidden"
        ))
        .bind(id)
        .bind(kind == "report")
        .bind(REPORTS_TO_HIDE)
        .fetch_one(&s.db.pool)
        .await?;
        crate::presence::broadcast(
            json!({ "type": "mark", "id": id, "likes": likes, "hidden": hidden }).to_string(),
        );
    }
    Ok(Json(json!({ "ok": true })).into_response())
}

/// `POST /v1/street/marks/{id}/hide`: moderation, with `x-admin-key: $STREET_ADMIN_KEY`.
pub async fn hide(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Response, ApiError> {
    let admin = std::env::var("STREET_ADMIN_KEY").unwrap_or_default();
    let got = headers
        .get("x-admin-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let same = !admin.is_empty()
        && got.len() == admin.len()
        && got
            .bytes()
            .zip(admin.bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0;
    if !same {
        return Ok((StatusCode::FORBIDDEN, Json(json!({ "error": "forbidden" }))).into_response());
    }
    sqlx::query("UPDATE street_marks SET hidden = true WHERE id = $1")
        .bind(id)
        .execute(&s.db.pool)
        .await?;
    crate::presence::broadcast(json!({ "type": "mark", "id": id, "hidden": true }).to_string());
    Ok(Json(json!({ "ok": true })).into_response())
}

// ---------------------------------------------------------------------------------------------
// Playing the payments

type Payment = (
    String,
    String,
    i32,
    i32,
    i64,
    Option<String>,
    Option<String>,
);

/// Plays one payment; returns its outcome.
async fn play(s: &AppState, p: &Payment) -> anyhow::Result<&'static str> {
    let (txid, pool, idx, height, value, memo, r) = p;
    let Some(r) = r else {
        // a plain payment (the static QR codes on the phones): an anonymous tip
        if *value < TIP_ZAT {
            return Ok("no_ref");
        }
        let key = format!("tx:{txid}:{pool}:{idx}");
        sqlx::query(
            "INSERT INTO street_tips (ref, name, txid, height, zat) VALUES ($1, NULL, $2, $3, $4)
             ON CONFLICT DO NOTHING",
        )
        .bind(&key)
        .bind(txid)
        .bind(height)
        .bind(value)
        .execute(&s.db.pool)
        .await?;
        crate::presence::broadcast(
            json!({ "type": "pay", "action": "tip", "name": null, "amount": zec(*value),
                    "sponsor": false, "height": height })
            .to_string(),
        );
        return Ok("tip");
    };
    let intent: Option<(String, Value, i64)> =
        sqlx::query_as("SELECT action, payload, min_zat FROM street_intents WHERE ref = $1")
            .bind(r)
            .fetch_optional(&s.db.pool)
            .await?;
    let Some((action, payload, min)) = intent else {
        return Ok("unknown_ref");
    };
    if *value < min {
        return Ok("short");
    }
    let memo = memo.as_deref().unwrap_or("");
    let event = match action.as_str() {
        "mark" | "guestbook" => {
            let text = memo_text(memo, r, TEXT_MAX);
            if text.is_empty() {
                return Ok("empty");
            }
            let id: Option<i64> = sqlx::query_scalar(
                "INSERT INTO street_marks (ref, kind, place, floor, x, y, z, text, txid, height, zat)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                 ON CONFLICT (ref) DO NOTHING RETURNING id",
            )
            .bind(r)
            .bind(&action)
            .bind(payload["place"].as_str().unwrap_or("street"))
            .bind(payload["floor"].as_i64().unwrap_or(0) as i16)
            .bind(payload["x"].as_f64().unwrap_or(0.0) as f32)
            .bind(payload["y"].as_f64().unwrap_or(0.0) as f32)
            .bind(payload["z"].as_f64().unwrap_or(0.0) as f32)
            .bind(&text)
            .bind(txid)
            .bind(height)
            .bind(value)
            .fetch_optional(&s.db.pool)
            .await?;
            let Some(id) = id else { return Ok("duplicate") };
            json!({ "type": "pay", "action": action, "ref": r, "mark": id, "text": text,
                    "place": payload["place"], "floor": payload["floor"],
                    "x": payload["x"], "y": payload["y"], "z": payload["z"], "height": height })
        }
        "vote" => {
            let (round, idx) = (
                payload["round"].as_i64().unwrap_or(-1),
                payload["idx"].as_i64().unwrap_or(-1),
            );
            let done = sqlx::query(
                "INSERT INTO street_sealed_votes (ref, round, idx, txid, zat)
                 VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
            )
            .bind(r)
            .bind(round as i32)
            .bind(idx as i16)
            .bind(txid)
            .bind(value)
            .execute(&s.db.pool)
            .await?;
            if done.rows_affected() == 0 {
                return Ok("duplicate");
            }
            crate::presence::broadcast(json!({ "type": "works" }).to_string());
            json!({ "type": "pay", "action": "vote", "ref": r, "round": round, "idx": idx, "height": height })
        }
        _ => {
            let name = memo_text(memo, r, NAME_MAX);
            let name = (!name.is_empty()).then_some(name);
            sqlx::query(
                "INSERT INTO street_tips (ref, name, txid, height, zat) VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (ref) DO UPDATE SET zat = street_tips.zat + EXCLUDED.zat",
            )
            .bind(r)
            .bind(&name)
            .bind(txid)
            .bind(height)
            .bind(value)
            .execute(&s.db.pool)
            .await?;
            json!({ "type": "pay", "action": "tip", "ref": r, "name": name, "amount": zec(*value),
                    "sponsor": *value >= SPONSOR_ZAT, "height": height })
        }
    };
    crate::presence::broadcast(event.to_string());
    Ok("ok")
}

async fn tick(s: &AppState) -> anyhow::Result<()> {
    let due: Vec<Payment> = sqlx::query_as(
        "SELECT txid, pool, idx, height, value, memo, ref FROM street_payments
         WHERE NOT applied ORDER BY height, txid, idx LIMIT 100",
    )
    .fetch_all(&s.db.pool)
    .await?;
    for p in &due {
        let outcome = play(s, p).await?;
        sqlx::query(
            "UPDATE street_payments SET applied = true, outcome = $4
             WHERE txid = $1 AND pool = $2 AND idx = $3",
        )
        .bind(&p.0)
        .bind(&p.1)
        .bind(p.2)
        .bind(outcome)
        .execute(&s.db.pool)
        .await?;
        tracing::info!(txid = p.0, outcome, "street pay: payment played");
    }
    Ok(())
}

/// Every few seconds: the payments the indexer found, played for everyone.
pub async fn run(s: AppState) {
    if address().is_none() {
        return;
    }
    loop {
        if let Err(e) = tick(&s).await {
            tracing::warn!("street pay: {e:#}");
        }
        tokio::time::sleep(Duration::from_secs(4)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amounts() {
        assert_eq!(zec(10_000), "0.0001");
        assert_eq!(zec(100_000_000), "1");
        assert_eq!(zec(150_000_000), "1.5");
    }

    #[test]
    fn refs_are_base32() {
        let r = new_ref(b"x");
        assert_eq!(r.len(), 8);
        assert!(r
            .bytes()
            .all(|b| b.is_ascii_uppercase() || (b'2'..=b'7').contains(&b)));
        assert_ne!(r, new_ref(b"x"));
    }

    #[test]
    fn memo_texts() {
        assert_eq!(
            memo_text("ZS1:ABCDEFGH\nPraise the sun", "ABCDEFGH", 300),
            "Praise the sun"
        );
        assert_eq!(memo_text("ZS1:ABCDEFGH", "ABCDEFGH", 300), "");
        assert_eq!(
            memo_text("hi ZS1:ABCDEFGH there", "ABCDEFGH", 300),
            "hi  there"
        );
        assert_eq!(clean("a\u{7}b\n\n\n\nc  ", 300), "ab\n\nc");
        assert_eq!(clean("x".repeat(400).as_str(), 300).len(), 300);
    }
}
