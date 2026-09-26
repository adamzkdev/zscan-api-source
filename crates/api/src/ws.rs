//! Live events: the indexer publishes via Postgres NOTIFY, and we fan out to WebSocket clients.

use std::time::Duration;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::{json, Value};
use sqlx::postgres::PgListener;
use tokio::sync::broadcast::error::RecvError;

use crate::AppState;

pub async fn listen(state: AppState) {
    loop {
        if let Err(e) = listen_once(&state).await {
            tracing::warn!("event listener: {e:#}");
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn listen_once(state: &AppState) -> anyhow::Result<()> {
    let mut l = PgListener::connect_with(&state.db.pool).await?;
    l.listen_all(["zscan_block", "zscan_mempool"]).await?;
    loop {
        let n = l.recv().await?;
        let kind = if n.channel() == "zscan_block" {
            "block"
        } else {
            "mempool"
        };
        let data: Value = serde_json::from_str(n.payload()).unwrap_or(Value::Null);
        // No subscribers is fine.
        let _ = state
            .events
            .send(json!({ "type": kind, "data": data }).to_string());
    }
}

/// WebSocket stream of `{"type": "block" | "mempool", "data": {...}}` events.
#[utoipa::path(get, path = "/v1/ws", responses((status = 101, description = "WebSocket upgrade")), tag = "network")]
pub async fn handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    let Ok(slot) = state.ws_slots.clone().try_acquire_owned() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "too many live connections").into_response();
    };
    ws.on_upgrade(move |socket| async move {
        client(socket, state).await;
        drop(slot);
    })
}

async fn client(mut socket: WebSocket, state: AppState) {
    let mut rx = state.events.subscribe();
    loop {
        tokio::select! {
            ev = rx.recv() => match ev {
                Ok(text) => {
                    if socket.send(Message::Text(text.into())).await.is_err() {
                        return;
                    }
                }
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return,
            },
            msg = socket.recv() => match msg {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                _ => {}
            },
        }
    }
}
