//! ZStreet presence: who is walking around the city right now. Each visitor sends where they are
//! (a point on the map, their zkSNARK avatar, on foot or not) and now and then one of a few
//! emojis; four times a second everyone gets the list. No free text, nothing stored: it lives in
//! memory on the API instance and a visitor disappears when the socket closes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::broadcast;

use crate::AppState;

/// Emojis a visitor can show (by index), the only thing anyone can "say".
const EMOJIS: usize = 6;
/// Most visitors listed to each client, and most in the room.
const LIST_MAX: usize = 150;
const ROOM_MAX: usize = 500;
/// Messages a client may send per second (its position four times, an emoji now and then).
const MSGS_PER_SEC: u32 = 8;
const EMOJI_EVERY: Duration = Duration::from_millis(1500);
const EMOJI_SHOWN: Duration = Duration::from_secs(4);

struct Visitor {
    avatar: u16,
    x: f32,
    z: f32,
    /// 0 looking at the map, 1 on foot, 2 inside a place.
    mode: u8,
    emoji: Option<(u8, Instant)>,
}

struct Room {
    visitors: Mutex<HashMap<u64, Visitor>>,
    tx: broadcast::Sender<String>,
    next: AtomicU64,
}

fn room() -> &'static Room {
    static R: OnceLock<Room> = OnceLock::new();
    R.get_or_init(|| {
        let (tx, _) = broadcast::channel(16);
        Room {
            visitors: Mutex::new(HashMap::new()),
            tx,
            next: AtomicU64::new(1),
        }
    })
}

/// Four times a second: the list, to everyone connected.
pub async fn tick() {
    let r = room();
    let mut every = tokio::time::interval(Duration::from_millis(250));
    loop {
        every.tick().await;
        if r.tx.receiver_count() == 0 {
            continue;
        }
        let now = Instant::now();
        let (n, list) = {
            let v = r.visitors.lock().unwrap();
            let list: Vec<_> = v
                .iter()
                .take(LIST_MAX)
                .map(|(id, p)| {
                    let e = p
                        .emoji
                        .filter(|(_, at)| now.duration_since(*at) < EMOJI_SHOWN)
                        .map_or(-1, |(e, _)| e as i8);
                    json!([
                        id,
                        p.avatar,
                        (p.x * 10.0).round() / 10.0,
                        (p.z * 10.0).round() / 10.0,
                        p.mode,
                        e
                    ])
                })
                .collect();
            (v.len(), list)
        };
        let _ =
            r.tx.send(json!({ "type": "presence", "n": n, "v": list }).to_string());
    }
}

#[derive(Deserialize)]
struct Update {
    /// zkSNARK avatar, 1–10,000.
    a: Option<u16>,
    x: Option<f32>,
    z: Option<f32>,
    m: Option<u8>,
    /// Emoji index.
    e: Option<u8>,
}

/// WebSocket for ZStreet's visitors: send `{"a": avatar, "x", "z", "m": mode, "e": emoji}`,
/// receive `{"type": "hello", "id"}` then `{"type": "presence", "n", "v": [[id, avatar, x, z, mode, emoji], …]}`.
pub async fn handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    let Ok(slot) = state.ws_slots.clone().try_acquire_owned() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "too many live connections").into_response();
    };
    if room().visitors.lock().unwrap().len() >= ROOM_MAX {
        return (StatusCode::SERVICE_UNAVAILABLE, "the city is full").into_response();
    }
    ws.max_message_size(256)
        .on_upgrade(move |socket| async move {
            client(socket).await;
            drop(slot);
        })
}

async fn client(mut socket: WebSocket) {
    let r = room();
    let id = r.next.fetch_add(1, Ordering::Relaxed);
    let mut rx = r.tx.subscribe();
    if socket
        .send(Message::Text(
            json!({ "type": "hello", "id": id }).to_string().into(),
        ))
        .await
        .is_err()
    {
        return;
    }
    let mut window = Instant::now();
    let mut count = 0u32;
    loop {
        tokio::select! {
            ev = rx.recv() => match ev {
                Ok(text) => {
                    if socket.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            },
            msg = socket.recv() => match msg {
                Some(Ok(Message::Text(t))) => {
                    // a few messages a second, the rest dropped
                    if window.elapsed() >= Duration::from_secs(1) {
                        window = Instant::now();
                        count = 0;
                    }
                    count += 1;
                    if count > MSGS_PER_SEC {
                        continue;
                    }
                    if let Ok(u) = serde_json::from_str::<Update>(&t) {
                        apply(id, u);
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                _ => {}
            },
        }
    }
    r.visitors.lock().unwrap().remove(&id);
}

fn apply(id: u64, u: Update) {
    let mut v = room().visitors.lock().unwrap();
    let p = v.entry(id).or_insert(Visitor {
        avatar: 1,
        x: 0.0,
        z: 0.0,
        mode: 0,
        emoji: None,
    });
    if let Some(a) = u.a.filter(|a| (1..=10_000).contains(a)) {
        p.avatar = a;
    }
    // the city is about 100 × 70 m around the origin
    if let (Some(x), Some(z)) = (u.x.filter(|x| x.is_finite()), u.z.filter(|z| z.is_finite())) {
        p.x = x.clamp(-60.0, 60.0);
        p.z = z.clamp(-45.0, 40.0);
    }
    if let Some(m) = u.m.filter(|m| *m <= 2) {
        p.mode = m;
    }
    if let Some(e) = u.e.filter(|e| (*e as usize) < EMOJIS) {
        let now = Instant::now();
        if p.emoji
            .is_none_or(|(_, at)| now.duration_since(at) >= EMOJI_EVERY)
        {
            p.emoji = Some((e, now));
        }
    }
}
