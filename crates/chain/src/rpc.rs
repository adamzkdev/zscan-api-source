//! zebrad JSON-RPC client with a request-spacing rate limiter.
//!
//! Public gateways (e.g. Tatum) allow ~5 req/min per IP, so every call waits for its slot.
//! A self-hosted Zebra/Zaino node can set the rate to 0 (unlimited).

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::limiter::Limiter;

#[derive(Clone)]
pub struct Rpc {
    http: reqwest::Client,
    url: String,
    limiter: Limiter,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ValuePool {
    pub id: String,
    #[serde(rename = "chainValueZat")]
    pub chain_value_zat: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChainSupply {
    #[serde(rename = "chainValueZat")]
    pub chain_value_zat: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BlockchainInfo {
    pub blocks: u32,
    pub bestblockhash: String,
    pub difficulty: f64,
    #[serde(rename = "valuePools", default)]
    pub value_pools: Vec<ValuePool>,
    #[serde(rename = "chainSupply")]
    pub chain_supply: Option<ChainSupply>,
}

impl BlockchainInfo {
    pub fn pool(&self, id: &str) -> Option<i64> {
        self.value_pools
            .iter()
            .find(|p| p.id == id)
            .map(|p| p.chain_value_zat)
    }
}

impl Rpc {
    /// `per_minute == 0` disables rate limiting.
    pub fn new(url: &str, per_minute: u32) -> Self {
        Self {
            http: crate::http_client_builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
            url: url.to_string(),
            limiter: Limiter::per_second(per_minute as f64 / 60.0),
        }
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        self.limiter.wait().await;
        tracing::debug!(method, "rpc call");
        let resp: Value = self
            .http
            .post(&self.url)
            .json(&json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params }))
            .send()
            .await
            .with_context(|| format!("rpc {method}"))?
            .json()
            .await
            .with_context(|| format!("rpc {method} body"))?;
        if let Some(err) = resp.get("error").filter(|e| !e.is_null()) {
            return Err(anyhow!("rpc {method} error: {err}"));
        }
        resp.get("result")
            .cloned()
            .ok_or_else(|| anyhow!("rpc {method}: no result"))
    }

    pub async fn blockchain_info(&self) -> Result<BlockchainInfo> {
        Ok(serde_json::from_value(
            self.call("getblockchaininfo", json!([])).await?,
        )?)
    }
}
