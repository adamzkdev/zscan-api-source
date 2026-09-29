//! ZScan accounts: secrets, sessions, and API keys on public requests.
//!
//! Sign-in links, sessions and API keys are random 32-byte tokens shown once; the database only
//! keeps their SHA-256. Sessions start with `zss_`, API keys with `zsk_`.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::HeaderMap;
use base64::Engine;
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use lru::LruCache;
use sha2::{Digest, Sha256};
use sqlx::PgPool;

use crate::error::ApiError;
use crate::AppState;

pub const SESSION_PREFIX: &str = "zss_";
pub const KEY_PREFIX: &str = "zsk_";

/// A random URL-safe token with a prefix.
pub fn new_token(prefix: &str) -> String {
    let mut b = [0u8; 32];
    getrandom::getrandom(&mut b).expect("OS randomness");
    format!(
        "{prefix}{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
    )
}

pub fn hash(secret: &str) -> String {
    hex::encode(Sha256::digest(secret.as_bytes()))
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::trim)
}

/// The API key of a request: `X-API-Key: zsk_…` or `Authorization: Bearer zsk_…`.
pub fn api_key(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .or_else(|| bearer(headers).filter(|t| t.starts_with(KEY_PREFIX)))
}

/// The signed-in user of a console request (`Authorization: Bearer zss_…`).
pub struct User {
    pub id: i64,
    pub email: String,
}

impl FromRequestParts<AppState> for User {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, s: &AppState) -> Result<Self, Self::Rejection> {
        let token = bearer(&parts.headers)
            .filter(|t| t.starts_with(SESSION_PREFIX))
            .ok_or_else(|| ApiError::Unauthorized("unauthorized", "Sign in required".into()))?;
        let row: Option<(i64, String)> = sqlx::query_as(
            "SELECT u.id, u.email FROM sessions s JOIN users u ON u.id = s.user_id
             WHERE s.token_hash = $1 AND s.expires_at > now()",
        )
        .bind(hash(token))
        .fetch_optional(&s.db.pool)
        .await?;
        let (id, email) = row.ok_or_else(|| {
            ApiError::Unauthorized("session_expired", "Session expired, sign in again".into())
        })?;
        Ok(User { id, email })
    }
}

/// API keys seen on public requests: a short cache in front of the database (a revoked key
/// stops working within a minute), their own rate limit, and usage counts flushed in batches.
#[derive(Clone)]
pub struct Keys {
    cache: Arc<Mutex<LruCache<String, (Option<i64>, Instant)>>>,
    usage: Arc<Mutex<HashMap<i64, u64>>>,
    pub limiter: Arc<DefaultKeyedRateLimiter<i64>>,
}

const KEY_CACHE_TTL: Duration = Duration::from_secs(60);

impl Keys {
    pub fn new(per_sec: u32) -> Self {
        let per_sec = NonZeroU32::new(per_sec.max(1)).unwrap();
        Self {
            cache: Arc::new(Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(10_000).unwrap(),
            ))),
            usage: Default::default(),
            limiter: Arc::new(RateLimiter::keyed(
                Quota::per_second(per_sec)
                    .allow_burst(per_sec.saturating_mul(NonZeroU32::new(3).unwrap())),
            )),
        }
    }

    /// The key's id, or None for an unknown or revoked key.
    pub async fn lookup(&self, db: &PgPool, key: &str) -> Result<Option<i64>, sqlx::Error> {
        let h = hash(key);
        if let Some((id, at)) = self.cache.lock().unwrap().get(&h).copied() {
            if at.elapsed() < KEY_CACHE_TTL {
                return Ok(id);
            }
        }
        let id: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL",
        )
        .bind(&h)
        .fetch_optional(db)
        .await?;
        self.cache.lock().unwrap().put(h, (id, Instant::now()));
        Ok(id)
    }

    pub fn record(&self, id: i64) {
        *self.usage.lock().unwrap().entry(id).or_default() += 1;
    }

    /// Writes usage counts every 30 s and trims the rate-limit table.
    pub async fn flush_loop(self, db: PgPool) {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let batch: Vec<(i64, u64)> = self.usage.lock().unwrap().drain().collect();
            for (id, n) in batch {
                if let Err(e) = sqlx::query(
                    "UPDATE api_keys SET requests = requests + $2, last_used_at = now() WHERE id = $1",
                )
                .bind(id)
                .bind(n as i64)
                .execute(&db)
                .await
                {
                    tracing::warn!("api key usage: {e}");
                }
            }
            self.limiter.retain_recent();
        }
    }
}
