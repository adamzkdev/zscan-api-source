//! Minimal request-spacing rate limiter shared by the gRPC and JSON-RPC clients.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;

#[derive(Clone)]
pub struct Limiter {
    spacing: Option<Duration>,
    next_slot: Arc<Mutex<Instant>>,
}

impl Limiter {
    /// `per_second <= 0` disables limiting.
    pub fn per_second(per_second: f64) -> Self {
        Self {
            spacing: (per_second > 0.0).then(|| Duration::from_secs_f64(1.0 / per_second)),
            next_slot: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Waits until the next request slot.
    pub async fn wait(&self) {
        let Some(spacing) = self.spacing else { return };
        let slot = {
            let mut next = self.next_slot.lock().await;
            let slot = (*next).max(Instant::now());
            *next = slot + spacing;
            slot
        };
        tokio::time::sleep_until(slot).await;
    }
}
