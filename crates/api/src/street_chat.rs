//! Talking to ZStreet's characters: a zkSNARK (or a shopkeeper, a miner…) answers in character
//! about what it is doing, from facts the street sends along (its transaction, its block, the
//! network). The model runs on OpenRouter with the server's key; visitors never see it. Each
//! visitor gets a few questions a minute and the whole city a daily budget.

use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::OnceLock;

use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use serde::Deserialize;
use serde_json::json;

use crate::street_llm::clip;
use crate::AppState;

const MAX_TURNS: usize = 10;
const MAX_CHARS: usize = 500;
const MAX_FACTS: usize = 5000;

#[derive(Deserialize)]
pub struct Turn {
    role: String,
    content: String,
}

#[derive(Deserialize)]
pub struct ChatRequest {
    /// Who is talking: "zkSNARK #4521, a carrier", "the MEMPOOL shopkeeper"…
    who: String,
    /// What it knows (its transaction, its block, the network), as plain lines.
    facts: String,
    messages: Vec<Turn>,
}

fn limiter() -> &'static DefaultKeyedRateLimiter<IpAddr> {
    static L: OnceLock<DefaultKeyedRateLimiter<IpAddr>> = OnceLock::new();
    L.get_or_init(|| {
        RateLimiter::keyed(
            Quota::per_minute(NonZeroU32::new(8).unwrap()).allow_burst(NonZeroU32::new(4).unwrap()),
        )
    })
}

/// Questions answered today (UTC), reset at midnight.
static DAY: AtomicU64 = AtomicU64::new(0);
static TODAY: AtomicU32 = AtomicU32::new(0);

fn under_budget() -> bool {
    let cap: u32 = std::env::var("STREET_CHAT_DAILY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5000);
    let day = (chrono::Utc::now().timestamp() / 86_400) as u64;
    if DAY.swap(day, Ordering::Relaxed) != day {
        TODAY.store(0, Ordering::Relaxed);
    }
    TODAY.fetch_add(1, Ordering::Relaxed) < cap
}

/// `POST /v1/street/chat`: one answer from a character, in character.
pub async fn chat(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<ChatRequest>,
) -> Response {
    if crate::street_llm::key().is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "chat_off", "message": "Talking is switched off for now" })),
        )
            .into_response();
    }
    let ip = crate::client_ip(peer.ip(), &headers).unwrap_or(peer.ip());
    if let Some(wait) = crate::limited_for(limiter(), &ip) {
        return crate::too_many_requests(wait, "Give them a moment to catch their breath");
    }
    if !under_budget() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "error": "budget", "message": "Everyone in the city is talked out for today" }))).into_response();
    }
    let turns: Vec<_> = req
        .messages
        .iter()
        .rev()
        .take(MAX_TURNS)
        .rev()
        .filter(|t| t.role == "user" || t.role == "assistant")
        .map(|t| json!({ "role": t.role, "content": clip(&t.content, MAX_CHARS) }))
        .collect();
    if turns.last().and_then(|t| t["role"].as_str()) != Some("user") {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "bad_request", "message": "Say something first" })),
        )
            .into_response();
    }
    let system = format!(
        "You are {who}, a character in ZStreet (zstreet.cash), a voxel city where every real Zcash \
         transaction is carried by a zkSNARK (a pixel-art character from the zkSNARKs NFT collection) \
         from the MEMPOOL shop to a bus stop; each mined block is a bus that takes them away. Trucks \
         bring parcels from the Proof Factory (shielded) and the depot (transparent). Big public \
         amounts ride a taxi or a limousine with bodyguards.\n\
         What you know right now (real data, trust it):\n{facts}\n\n\
         Rules: you are this person, not an assistant and not a tour guide. Keep the voice, \
         temperament and habits the facts give you in every line, even if that means grumpy, shy, \
         odd or blunt; two characters must never sound alike. Talk like a real person: opinions, \
         your own day, the people you know, a question back now and then. Never use assistant \
         phrases ('Great question', 'Absolutely', 'I'd be happy to', 'Feel free', 'Let me know') \
         and never say you are an AI. Answer in English. Usually 1-3 sentences, sometimes just a \
         few words; no lists, no markdown. Don't recite the city's numbers unless asked. Use the \
         facts for numbers; never invent amounts, addresses or heights. Shielded amounts and addresses are private: say \
         you can't see them, that's the point of Zcash. You can explain Zcash simply (shielded pools, \
         Orchard, Ironwood, NU7, zk-proofs, fees). No price predictions or financial advice. If asked \
         about something unrelated, steer back to the city and Zcash. For full details, point to zscan.cash.",
        who = clip(&req.who, 120),
        facts = clip(&req.facts, MAX_FACTS),
    );
    let mut messages = vec![json!({ "role": "system", "content": system })];
    messages.extend(turns);
    match crate::street_llm::chat(&s.http, messages, 220, 0.95).await {
        Some(text) => Json(json!({ "reply": text })).into_response(),
        None => (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "error": "upstream", "message": "They didn't hear you, try again" })),
        )
            .into_response(),
    }
}
