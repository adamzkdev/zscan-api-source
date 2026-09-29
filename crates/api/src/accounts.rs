//! API keys on public requests. Keys (`zsk_…`) are created in the ZScan console and stored in
//! Supabase as SHA-256 hashes; the API looks them up there, with a short cache in front (a
//! revoked key stops working within a minute), gives each its own rate limit, and reports usage
//! in batches.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::http::HeaderMap;
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use lru::LruCache;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use zscan_chain::supabase::Supabase;

pub const KEY_PREFIX: &str = "zsk_";

pub fn hash(secret: &str) -> String {
    hex::encode(Sha256::digest(secret.as_bytes()))
}

/// The API key of a request: `X-API-Key: zsk_…` or `Authorization: Bearer zsk_…`.
pub fn api_key(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .or_else(|| {
            headers
                .get("authorization")?
                .to_str()
                .ok()?
                .strip_prefix("Bearer ")
                .map(str::trim)
                .filter(|t| t.starts_with(KEY_PREFIX))
        })
}

#[derive(Clone)]
pub struct Keys {
    supabase: Option<Supabase>,
    cache: Arc<Mutex<LruCache<String, (Option<i64>, Instant)>>>,
    usage: Arc<Mutex<HashMap<i64, u64>>>,
    pub limiter: Arc<DefaultKeyedRateLimiter<i64>>,
}

const KEY_CACHE_TTL: Duration = Duration::from_secs(60);

#[derive(Deserialize)]
struct KeyRow {
    id: i64,
}

impl Keys {
    pub fn new(supabase: Option<Supabase>, per_sec: u32) -> Self {
        let per_sec = NonZeroU32::new(per_sec.max(1)).unwrap();
        Self {
            supabase,
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

    /// Whether keys can be checked (Supabase configured). Without it, keys are ignored and the
    /// request is limited per IP like any other.
    pub fn enabled(&self) -> bool {
        self.supabase.is_some()
    }

    /// The key's id, or None for an unknown or revoked key.
    pub async fn lookup(&self, key: &str) -> anyhow::Result<Option<i64>> {
        let Some(sb) = &self.supabase else {
            return Ok(None);
        };
        let h = hash(key);
        if let Some((id, at)) = self.cache.lock().unwrap().get(&h).copied() {
            if at.elapsed() < KEY_CACHE_TTL {
                return Ok(id);
            }
        }
        let rows: Vec<KeyRow> = sb
            .select(
                "api_keys",
                &format!("select=id&key_hash=eq.{h}&revoked_at=is.null&limit=1"),
            )
            .await?;
        let id = rows.first().map(|r| r.id);
        self.cache.lock().unwrap().put(h, (id, Instant::now()));
        Ok(id)
    }

    pub fn record(&self, id: i64) {
        *self.usage.lock().unwrap().entry(id).or_default() += 1;
    }

    /// Reports usage every 30 s and trims the rate-limit table.
    pub async fn flush_loop(self) {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            self.limiter.retain_recent();
            let Some(sb) = &self.supabase else { continue };
            let batch: Vec<(i64, u64)> = self.usage.lock().unwrap().drain().collect();
            for (id, n) in batch {
                if let Err(e) = sb
                    .rpc::<_, serde_json::Value>(
                        "record_api_usage",
                        &serde_json::json!({"key_id": id, "n": n}),
                    )
                    .await
                {
                    tracing::warn!("api key usage: {e:#}");
                }
            }
        }
    }
}
