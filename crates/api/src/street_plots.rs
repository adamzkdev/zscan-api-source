//! Player plots: 24 free lots in ZStreet where visitors put up their own voxel buildings. A
//! building belongs to the client-side secret that saved it first; anyone can report one, and
//! five reports hide it until its owner frees the lot.

use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::{Mutex, OnceLock};

use axum::{
    extract::{ConnectInfo, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::Engine;
use chrono::{DateTime, Utc};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::error::ApiError;
use crate::street::{bad_request, ip_of, token_hash};
use crate::AppState;

const PLOTS: i32 = 24;
const MODEL_MAX: usize = 64 * 1024;
const REPORTS_TO_HIDE: i32 = 5;

/// Checks a building and returns it with only the known fields. `data` is base64 of runs of
/// (u16 LE length ≥ 1, u8 palette index, 0 = empty) covering exactly w × h × d cells, x fastest,
/// then z, then y.
pub fn validate_model(m: &Value) -> Result<Value, &'static str> {
    if serde_json::to_string(m).map_or(true, |s| s.len() > MODEL_MAX) {
        return Err("model is larger than 64 KB");
    }
    let dim = |k: &str, max: u64| m[k].as_u64().filter(|v| (1..=max).contains(v));
    let (Some(w), Some(h), Some(d)) = (dim("w", 32), dim("h", 48), dim("d", 32)) else {
        return Err("w and d must be 1-32, h 1-48");
    };
    let palette = m["palette"].as_array().ok_or("palette is required")?;
    if !(1..=32).contains(&palette.len()) {
        return Err("palette must have 1-32 colors");
    }
    let mut colors = Vec::with_capacity(palette.len());
    for c in palette {
        let hex = c["hex"].as_str().unwrap_or("");
        let hex_ok = hex.len() == 7
            && hex.starts_with('#')
            && hex[1..].bytes().all(|b| b.is_ascii_hexdigit());
        let kind = c["kind"].as_str().unwrap_or("");
        if !hex_ok || !matches!(kind, "solid" | "glass" | "emit") {
            return Err("palette entries are {hex: \"#rrggbb\", kind: solid|glass|emit}");
        }
        colors.push(json!({ "hex": hex.to_ascii_lowercase(), "kind": kind }));
    }
    let data = m["data"].as_str().ok_or("data is required")?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|_| "data is not base64")?;
    if bytes.is_empty() || bytes.len() % 3 != 0 {
        return Err("data must be runs of 3 bytes");
    }
    let cells = w * h * d;
    let mut total = 0u64;
    for run in bytes.as_chunks::<3>().0 {
        let len = u16::from_le_bytes([run[0], run[1]]) as u64;
        if len == 0 {
            return Err("runs must be at least 1 long");
        }
        if run[2] as usize > colors.len() {
            return Err("run uses a color outside the palette");
        }
        total += len;
        if total > cells {
            return Err("runs cover more than w × h × d cells");
        }
    }
    if total != cells {
        return Err("runs must cover exactly w × h × d cells");
    }
    Ok(json!({ "w": w, "h": h, "d": d, "palette": colors, "data": data }))
}

fn name_ok(name: &str) -> bool {
    let n = name.chars().count();
    (1..=24).contains(&n) && name.chars().all(|c| !c.is_control())
}

fn forbidden(message: &str) -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({ "error": "forbidden", "message": message })),
    )
        .into_response()
}

fn plot_ok(plot: i32) -> Result<(), ApiError> {
    (0..PLOTS)
        .contains(&plot)
        .then_some(())
        .ok_or_else(|| bad_request("plot must be 0-23"))
}

/// Tells every connected visitor to refetch the plots.
fn announce(plot: i32) {
    crate::presence::broadcast(json!({ "type": "plot", "plot": plot }).to_string());
}

#[derive(Serialize, sqlx::FromRow)]
pub struct Plot {
    plot: i32,
    name: String,
    model: Value,
    updated_at: DateTime<Utc>,
}

/// `GET /v1/street/plots`: every visible building.
pub async fn list(State(s): State<AppState>) -> Result<Json<Vec<Plot>>, ApiError> {
    Ok(Json(
        sqlx::query_as(
            "SELECT plot, name, model, updated_at FROM street_plots WHERE NOT hidden ORDER BY plot",
        )
        .fetch_all(&s.db.pool)
        .await?,
    ))
}

#[derive(Deserialize)]
pub struct SavePlot {
    plot: i32,
    token: String,
    name: String,
    model: Value,
}

/// `POST /v1/street/plots`: builds on a free plot, or rebuilds one's own.
pub async fn save(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<SavePlot>,
) -> Result<Response, ApiError> {
    static LIMIT: OnceLock<DefaultKeyedRateLimiter<IpAddr>> = OnceLock::new();
    plot_ok(req.plot)?;
    let hash = token_hash(&req.token).ok_or_else(|| bad_request("token must be 32-64 hex"))?;
    let name = req.name.trim();
    if !name_ok(name) {
        return Err(bad_request("name must be 1-24 printable characters"));
    }
    let model = validate_model(&req.model).map_err(bad_request)?;
    let owner: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT token_hash FROM street_plots WHERE plot = $1")
            .bind(req.plot)
            .fetch_optional(&s.db.pool)
            .await?;
    if owner.is_some_and(|o| o != hash) {
        return Ok(forbidden("Someone else built on this plot"));
    }
    let limiter =
        LIMIT.get_or_init(|| RateLimiter::keyed(Quota::per_minute(NonZeroU32::new(1).unwrap())));
    if let Some(wait) = crate::limited_for(limiter, &ip_of(peer, &headers)) {
        return Ok(crate::too_many_requests(
            wait,
            "One save a minute, the builders need a break",
        ));
    }
    // the owner check again, atomically, in case someone claimed the plot meanwhile
    let saved: Option<Plot> = sqlx::query_as(
        "INSERT INTO street_plots (plot, token_hash, name, model) VALUES ($1, $2, $3, $4)
         ON CONFLICT (plot) DO UPDATE SET name = EXCLUDED.name, model = EXCLUDED.model,
             reports = 0, updated_at = now()
         WHERE street_plots.token_hash = EXCLUDED.token_hash
         RETURNING plot, name, model, updated_at",
    )
    .bind(req.plot)
    .bind(&hash)
    .bind(name)
    .bind(&model)
    .fetch_optional(&s.db.pool)
    .await?;
    let Some(saved) = saved else {
        return Ok(forbidden("Someone else built on this plot"));
    };
    announce(req.plot);
    Ok(Json(saved).into_response())
}

/// `POST /v1/street/plots/{plot}/report`: one report per visitor per plot per day.
pub async fn report(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(plot): Path<i32>,
) -> Result<Response, ApiError> {
    /// Day number and the (visitor, plot) pairs reported that day.
    type Seen = (i64, Option<HashSet<(IpAddr, i32)>>);
    static SEEN: Mutex<Seen> = Mutex::new((0, None));
    plot_ok(plot)?;
    let key = (ip_of(peer, &headers), plot);
    let day = Utc::now().timestamp() / 86_400;
    {
        let mut seen = SEEN.lock().unwrap();
        if seen.0 != day {
            *seen = (day, Some(HashSet::new()));
        }
        if seen.1.as_ref().is_some_and(|set| set.contains(&key)) {
            return Ok(Json(json!({ "ok": true })).into_response());
        }
    }
    let row: Option<(bool, i32)> = sqlx::query_as(
        "UPDATE street_plots SET reports = reports + 1, hidden = hidden OR reports + 1 >= $2
         WHERE plot = $1 RETURNING hidden, reports",
    )
    .bind(plot)
    .bind(REPORTS_TO_HIDE)
    .fetch_optional(&s.db.pool)
    .await?;
    let Some((hidden, reports)) = row else {
        return Err(ApiError::NotFound("Nothing is built on this plot".into()));
    };
    if let Some(set) = SEEN.lock().unwrap().1.as_mut() {
        set.insert(key);
    }
    if hidden && reports == REPORTS_TO_HIDE {
        announce(plot);
    }
    Ok(Json(json!({ "ok": true })).into_response())
}

/// `POST /v1/street/plots/{plot}/hide`: moderation, with `x-admin-key: $STREET_ADMIN_KEY`.
pub async fn hide(
    State(s): State<AppState>,
    headers: HeaderMap,
    Path(plot): Path<i32>,
) -> Result<Response, ApiError> {
    let Some(admin) = std::env::var("STREET_ADMIN_KEY")
        .ok()
        .filter(|k| !k.is_empty())
    else {
        return Err(ApiError::NotFound("Not found".into()));
    };
    let got = headers
        .get("x-admin-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let same = got.len() == admin.len()
        && got
            .bytes()
            .zip(admin.bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0;
    if !same {
        return Ok(forbidden("Wrong admin key"));
    }
    let done = sqlx::query("UPDATE street_plots SET hidden = true WHERE plot = $1")
        .bind(plot)
        .execute(&s.db.pool)
        .await?;
    if done.rows_affected() == 0 {
        return Err(ApiError::NotFound("Nothing is built on this plot".into()));
    }
    announce(plot);
    Ok(Json(json!({ "ok": true })).into_response())
}

#[derive(Deserialize)]
pub struct Owner {
    token: String,
}

/// `DELETE /v1/street/plots/{plot}` with the owner's `{token}`: frees the plot.
pub async fn free(
    State(s): State<AppState>,
    Path(plot): Path<i32>,
    Json(req): Json<Owner>,
) -> Result<Response, ApiError> {
    plot_ok(plot)?;
    let hash = token_hash(&req.token).ok_or_else(|| bad_request("token must be 32-64 hex"))?;
    let owner: Option<Vec<u8>> = sqlx::query_scalar(
        "DELETE FROM street_plots WHERE plot = $1 AND token_hash = $2 RETURNING token_hash",
    )
    .bind(plot)
    .bind(&hash)
    .fetch_optional(&s.db.pool)
    .await?;
    if owner.is_none() {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM street_plots WHERE plot = $1)")
                .bind(plot)
                .fetch_one(&s.db.pool)
                .await?;
        return if exists {
            Ok(forbidden("This plot isn't yours"))
        } else {
            Err(ApiError::NotFound("Nothing is built on this plot".into()))
        };
    }
    announce(plot);
    Ok(Json(json!({ "ok": true })).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rle(runs: &[(u16, u8)]) -> String {
        let bytes: Vec<u8> = runs
            .iter()
            .flat_map(|(n, i)| {
                let [a, b] = n.to_le_bytes();
                [a, b, *i]
            })
            .collect();
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    fn model(w: u32, h: u32, d: u32, runs: &[(u16, u8)]) -> Value {
        json!({
            "w": w, "h": h, "d": d,
            "palette": [{ "hex": "#FF8800", "kind": "solid" }, { "hex": "#00aaff", "kind": "glass" }],
            "data": rle(runs),
            "extra": "dropped",
        })
    }

    #[test]
    fn valid_model_is_normalized() {
        let m = validate_model(&model(2, 3, 4, &[(10, 0), (10, 1), (4, 2)])).unwrap();
        assert_eq!(m["palette"][0]["hex"], "#ff8800");
        assert!(m.get("extra").is_none());
        // the largest building in a single run
        let big = model(32, 48, 32, &[(49_152, 1)]);
        assert!(validate_model(&big).is_ok());
    }

    #[test]
    fn bad_runs_are_rejected() {
        // too few, too many cells, zero-length run, index beyond the palette
        assert!(validate_model(&model(2, 3, 4, &[(23, 1)])).is_err());
        assert!(validate_model(&model(2, 3, 4, &[(20, 1), (5, 0)])).is_err());
        assert!(validate_model(&model(2, 3, 4, &[(0, 1), (24, 1)])).is_err());
        assert!(validate_model(&model(2, 3, 4, &[(24, 3)])).is_err());
        let mut m = model(2, 3, 4, &[(24, 1)]);
        m["data"] = json!("not base64!");
        assert!(validate_model(&m).is_err());
        m["data"] = json!(base64::engine::general_purpose::STANDARD.encode([24u8, 0]));
        assert!(validate_model(&m).is_err());
    }

    #[test]
    fn bad_shapes_and_palettes_are_rejected() {
        assert!(validate_model(&model(33, 1, 1, &[(33, 1)])).is_err());
        assert!(validate_model(&model(1, 49, 1, &[(49, 1)])).is_err());
        assert!(validate_model(&model(0, 1, 1, &[(1, 1)])).is_err());
        let mut m = model(1, 1, 1, &[(1, 1)]);
        m["palette"] = json!([{ "hex": "#12345g", "kind": "solid" }]);
        assert!(validate_model(&m).is_err());
        m["palette"] = json!([{ "hex": "#123456", "kind": "neon" }]);
        assert!(validate_model(&m).is_err());
        m["palette"] = json!([]);
        assert!(validate_model(&m).is_err());
    }

    #[test]
    fn names() {
        assert!(name_ok("My tower"));
        assert!(!name_ok(""));
        assert!(!name_ok("bad\nname"));
        assert!(!name_ok(&"x".repeat(25)));
    }
}
