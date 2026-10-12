//! HashSmash (yukon.org/hashsmash, Eigen Labs with Shielded Labs, Zcash, Project Eleven and the
//! Ethereum Foundation): the open challenge to find collisions in reduced-round SHA-256, SHA3-256,
//! BLAKE3 and Orchard's Poseidon. Every two minutes this reads Yukon's public API (no key; it sends
//! no CORS header, so browsers can't read it themselves) and keeps, per hash, every attack's rounds
//! and cost, the collision frontier, the lanes and the solvers. ZStreet's university has a hall
//! for it. Served by `GET /v1/hashsmash`.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};
use std::time::Duration;

use anyhow::Result;
use axum::{http::header, response::IntoResponse, Json};
use chrono::Utc;
use serde_json::{json, Value};

const API: &str = "https://api.yukon.org/api";
const REFRESH: Duration = Duration::from_secs(120);
const LATEST: usize = 40;
const SOLVERS: usize = 16;

struct Family {
    id: &'static str,
    label: &'static str,
    /// Rounds of the full hash.
    full: u32,
    note: &'static str,
}

const FAMILIES: &[Family] = &[
    Family {
        id: "sha256",
        label: "SHA-256",
        full: 64,
        note: "Bitcoin's hash, and Zcash's transparent addresses and proof of work's outer layer",
    },
    Family {
        id: "sha3-256",
        label: "SHA3-256",
        full: 24,
        note: "Keccak-f[1600], the sponge behind SHA-3 and Ethereum's Keccak-256",
    },
    Family {
        id: "blake3",
        label: "BLAKE3",
        full: 7,
        note: "BLAKE2's successor; Zcash's Equihash and transaction ids use BLAKE2b",
    },
    Family {
        id: "poseidon",
        label: "Poseidon · Orchard",
        full: 64,
        note: "Orchard's own hash (P128Pow5T3, 8 full + 56 partial rounds): nullifiers and the note commitment tree's circuit",
    },
];

fn cache() -> &'static RwLock<Value> {
    static C: OnceLock<RwLock<Value>> = OnceLock::new();
    C.get_or_init(|| RwLock::new(Value::Null))
}

pub async fn run(http: reqwest::Client) {
    loop {
        match snapshot(&http).await {
            Ok(v) => {
                if let Ok(mut c) = cache().write() {
                    *c = v;
                }
            }
            Err(e) => tracing::warn!("hashsmash: {e:#}"),
        }
        tokio::time::sleep(REFRESH).await;
    }
}

/// `GET /v1/hashsmash`: the families with their frontier and attacks, the latest submissions, the
/// solvers.
pub async fn get() -> impl IntoResponse {
    let v = cache().read().map(|c| c.clone()).unwrap_or(Value::Null);
    let v = if v.is_null() {
        json!({ "families": [], "latest": [], "solvers": [], "updated": null })
    } else {
        v
    };
    ([(header::CACHE_CONTROL, "public, max-age=30")], Json(v))
}

async fn fetch(http: &reqwest::Client, url: &str) -> Result<Value> {
    Ok(http
        .get(url)
        .header("User-Agent", "zscan")
        .timeout(Duration::from_secs(25))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

/// "sha256-r37-exploratory" → ("sha256", Some(37)); "sha3-256" → ("sha3-256", None).
fn family_of(track: &str) -> (&str, Option<u32>) {
    let mut cut = None;
    for (i, _) in track.match_indices("-r") {
        let rest = &track[i + 2..];
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        let tail = &rest[digits.len()..];
        if !digits.is_empty() && (tail.is_empty() || tail.starts_with('-')) {
            if let Ok(n) = digits.parse() {
                cut = Some((i, n));
                break;
            }
        }
    }
    match cut {
        Some((i, n)) => (&track[..i], Some(n)),
        None => (track, None),
    }
}

async fn snapshot(http: &reqwest::Client) -> Result<Value> {
    let list = fetch(http, &format!("{API}/benchmarks")).await?;
    let benches: Vec<&Value> = list["benchmarks"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|b| b["name"].as_str().is_some_and(|n| n.contains("/hashsmash")))
        .collect();
    anyhow::ensure!(!benches.is_empty(), "no hashsmash benchmarks");

    let mut points: HashMap<&str, Vec<Value>> = HashMap::new();
    let mut lanes: HashMap<&str, Vec<Value>> = HashMap::new();
    let mut latest: Vec<Value> = Vec::new();
    let mut solvers: HashMap<String, Value> = HashMap::new();
    for b in benches {
        let track = b["trackName"].as_str().unwrap_or_default();
        let (fam, lane_rounds) = family_of(track);
        let Some(f) = FAMILIES.iter().find(|f| f.id == fam) else {
            continue;
        };
        let id = b["id"].as_str().unwrap_or_default();
        let subs = match fetch(
            http,
            &format!("{API}/benchmarks/{id}/submissions?view=comparison"),
        )
        .await
        {
            Ok(v) => v["submissions"].as_array().cloned().unwrap_or_default(),
            Err(e) => {
                tracing::debug!("hashsmash {track}: {e:#}");
                continue;
            }
        };
        let best = &b["currentBestMetrics"];
        lanes.entry(f.id).or_default().push(json!({
            "track": track,
            "rounds": lane_rounds,
            "open": b["status"].as_str() == Some("open"),
            "pareto": lane_rounds.is_none(),
            "submissions": subs.len(),
            "best": best["timeLog2"].as_f64().filter(|_| lane_rounds.is_some()),
        }));
        for s in &subs {
            let m = &s["officialMetrics"];
            let status = s["status"].as_str().unwrap_or("unknown");
            let promoted = s["promotionStatus"].as_str() == Some("promoted");
            let rounds = m["rounds"].as_u64().map(|r| r as u32).or(lane_rounds);
            let solver = s["solverUsername"].as_str().unwrap_or("anon").to_string();
            let p = json!({
                "id": s["id"],
                "family": f.id,
                "rounds": rounds,
                "time": m["timeLog2"].as_f64(),
                "memory": m["memoryLog2Bytes"].as_f64(),
                "status": status,
                "promoted": promoted,
                "solver": solver,
                "avatar": s["solverAvatarUrl"],
                "profile": s["solverProfileUrl"],
                "at": s["createdAt"],
                "updated": s["updatedAt"],
            });
            let entry = solvers.entry(solver.clone()).or_insert_with(|| {
                json!({ "solver": solver, "avatar": s["solverAvatarUrl"], "profile": s["solverProfileUrl"],
                        "submissions": 0, "review": 0, "accepted": 0, "families": [], "best": null })
            });
            entry["submissions"] = json!(entry["submissions"].as_u64().unwrap_or(0) + 1);
            if status == "review" || status == "accepted" {
                let k = if status == "accepted" { "accepted" } else { "review" };
                entry[k] = json!(entry[k].as_u64().unwrap_or(0) + 1);
                if let Some(fams) = entry["families"].as_array_mut() {
                    if !fams.iter().any(|x| x == f.id) {
                        fams.push(json!(f.id));
                    }
                }
                // the deepest attack, the cheapest at that depth
                if let (Some(r), Some(t)) = (rounds, m["timeLog2"].as_f64()) {
                    let depth = r as f64 / f.full as f64;
                    let better = match (entry["best"]["depth"].as_f64(), entry["best"]["time"].as_f64()) {
                        (Some(bd), Some(bt)) => depth > bd || (depth == bd && t < bt),
                        _ => true,
                    };
                    if better {
                        entry["best"] = json!({ "family": f.id, "label": f.label, "rounds": r, "full": f.full,
                                                "time": t, "depth": depth });
                    }
                }
            }
            latest.push(p.clone());
            points.entry(f.id).or_default().push(p);
        }
    }

    let families: Vec<Value> = FAMILIES
        .iter()
        .map(|f| {
            let pts = points.remove(f.id).unwrap_or_default();
            let valid: Vec<&Value> = pts
                .iter()
                .filter(|p| {
                    matches!(p["status"].as_str(), Some("review" | "accepted"))
                        && p["rounds"].is_u64()
                        && p["time"].is_f64()
                })
                .collect();
            let frontier = pareto(&valid);
            let accepted: Vec<&Value> = valid
                .iter()
                .copied()
                .filter(|p| p["status"] == "accepted")
                .collect();
            let published = pareto(&accepted);
            let deepest = frontier
                .iter()
                .filter_map(|p| p["rounds"].as_u64())
                .max();
            let count = |s: &str| pts.iter().filter(|p| p["status"] == s).count();
            let mut lanes = lanes.remove(f.id).unwrap_or_default();
            lanes.sort_by_key(|l| l["rounds"].as_u64().unwrap_or(0));
            json!({
                "id": f.id,
                "label": f.label,
                "full": f.full,
                "note": f.note,
                "zcash": f.id == "poseidon",
                "submissions": pts.len(),
                "review": count("review"),
                "accepted": count("accepted"),
                "rejected": count("rejected") + count("failed"),
                "deepest": deepest,
                "frontier": frontier,
                "published": published,
                "lanes": lanes,
                "points": pts,
            })
        })
        .collect();

    latest.sort_by(|a, b| b["updated"].as_str().cmp(&a["updated"].as_str()));
    latest.truncate(LATEST);
    let mut solvers: Vec<Value> = solvers.into_values().collect();
    solvers.sort_by(|a, b| {
        let key = |v: &Value| {
            (
                v["accepted"].as_u64().unwrap_or(0),
                v["review"].as_u64().unwrap_or(0),
                v["submissions"].as_u64().unwrap_or(0),
            )
        };
        key(b).cmp(&key(a))
    });
    solvers.truncate(SOLVERS);
    Ok(json!({
        "updated": Utc::now(),
        "source": "https://www.yukon.org/hashsmash",
        "families": families,
        "latest": latest,
        "solvers": solvers,
    }))
}

/// The collision frontier: the attacks no other beats on both counts (as many rounds or more, as
/// cheap or cheaper), by rounds.
fn pareto(pts: &[&Value]) -> Vec<Value> {
    let rt = |p: &Value| {
        (
            p["rounds"].as_u64().unwrap_or(0),
            p["time"].as_f64().unwrap_or(f64::MAX),
        )
    };
    let mut out: Vec<Value> = pts
        .iter()
        .filter(|p| {
            let (r, t) = rt(p);
            !pts.iter().any(|q| {
                let (qr, qt) = rt(q);
                qr >= r && qt <= t && (qr > r || qt < t)
            })
        })
        .map(|p| (*p).clone())
        .collect();
    out.sort_by_key(|p| p["rounds"].as_u64().unwrap_or(0));
    out.dedup_by(|a, b| rt(a) == rt(b));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn families_of_tracks() {
        assert_eq!(family_of("sha256-r37-exploratory"), ("sha256", Some(37)));
        assert_eq!(family_of("sha3-256-r6-exploratory"), ("sha3-256", Some(6)));
        assert_eq!(family_of("sha3-256"), ("sha3-256", None));
        assert_eq!(family_of("blake3-r2-exploratory"), ("blake3", Some(2)));
        assert_eq!(family_of("poseidon"), ("poseidon", None));
    }

    #[test]
    fn frontier_keeps_the_unbeaten() {
        let a = json!({"rounds": 37, "time": 124.3});
        let b = json!({"rounds": 37, "time": 125.0});
        let c = json!({"rounds": 38, "time": 124.35});
        let d = json!({"rounds": 31, "time": 130.0});
        let f = pareto(&[&a, &b, &c, &d]);
        assert_eq!(f.len(), 2);
        assert_eq!(f[0]["time"], 124.3);
        assert_eq!(f[1]["rounds"], 38);
    }

    /// Reads the live API: `HASHSMASH_DUMP=/path cargo test -p zscan-api live_snapshot -- --ignored`.
    #[tokio::test]
    #[ignore]
    async fn live_snapshot() {
        let v = snapshot(&reqwest::Client::new()).await.unwrap();
        if let Ok(p) = std::env::var("HASHSMASH_DUMP") {
            std::fs::write(p, serde_json::to_string_pretty(&v).unwrap()).unwrap();
        }
        assert_eq!(v["families"].as_array().unwrap().len(), FAMILIES.len());
    }
}
