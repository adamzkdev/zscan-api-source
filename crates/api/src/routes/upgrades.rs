//! Network upgrades: history, the next activation and what changes with it.

use axum::{extract::State, Json};
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;
use zscan_chain::upgrades;

use crate::error::ApiResult;
use crate::AppState;

#[derive(Debug, Serialize, ToSchema)]
pub struct Upgrade {
    /// Zebra's identifier, e.g. `Nu6_3`.
    pub name: String,
    /// Display name, e.g. `NU6.3`.
    pub label: String,
    /// `None` while the activation height hasn't been set.
    pub activation_height: Option<u32>,
    pub activated: bool,
    /// Block time at activation, when that block is indexed.
    pub activated_at: Option<DateTime<Utc>>,
    pub blocks_remaining: Option<u32>,
    /// Projected activation time at target block spacing.
    pub eta: Option<DateTime<Utc>>,
    /// Date announced by the protocol developers, for upgrades without a height yet.
    pub announced_date: Option<&'static str>,
    /// Target seconds per block once active.
    pub target_block_time: u32,
    pub summary: Option<&'static str>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Upgrades {
    pub network: &'static str,
    pub height: Option<i32>,
    pub current: Option<String>,
    pub next: Option<String>,
    /// Past an upgrade whose subsidy rules ZScan doesn't implement yet (see `/v1/supply`).
    pub economics_pending: bool,
    pub upgrades: Vec<Upgrade>,
}

fn label(name: &str) -> String {
    match name.strip_prefix("Nu") {
        Some(v) => format!("NU{}", v.replace('_', ".")),
        None => name.to_string(),
    }
}

fn summary(name: &str) -> Option<&'static str> {
    Some(match name {
        "Overwinter" => "Transaction expiry and replay protection (v3 transactions).",
        "Sapling" => "Sapling shielded pool: fast, mobile-friendly private transactions.",
        "Blossom" => "Block time cut from 150s to 75s.",
        "Heartwood" => "Shielded coinbase and FlyClient light-client support.",
        "Canopy" => "Dev Fund (ZIP 1014) and the end of new Sprout deposits.",
        "Nu5" => "Orchard pool with Halo 2 (no trusted setup), Unified Addresses, v5 transactions.",
        "Nu6" => "Lockbox for part of the block reward (ZIP 1015).",
        "Nu6_1" => "Community grants and the coinholder-controlled fund (ZIP 1016).",
        "Nu6_2" => "Emergency fix for the Orchard soundness bug.",
        "Nu6_3" => "Ironwood shielded pool; Orchard funds leave through a turnstile.",
        "Nu7" => "25-second blocks (ZIP 218), v4 transactions disabled, Network Sustainability Mechanism (ZIPs 234/235).",
        _ => return None,
    })
}

/// Dates announced before activation heights are final (forum "NU7 timeline", 2026-09-17).
fn announced(name: &str) -> Option<&'static str> {
    match (name, zscan_chain::is_testnet()) {
        ("Nu7", false) => Some("2026-11-05"),
        ("Nu7", true) => Some("2026-10-06"),
        _ => None,
    }
}

/// Network upgrades with activation heights, projected dates and what each one changed.
#[utoipa::path(get, path = "/v1/upgrades", responses((status = 200, body = Upgrades)), tag = "network")]
pub async fn list(State(s): State<AppState>) -> ApiResult<Upgrades> {
    let height = super::tip(&s.db).await?;
    let tip = height.unwrap_or(0).max(0) as u32;
    let all = upgrades::upgrades();
    let heights: Vec<i32> = all
        .iter()
        .filter_map(|u| u.activation_height.map(|h| h as i32))
        .collect();
    let times: Vec<(i32, DateTime<Utc>)> =
        sqlx::query_as("SELECT height, time FROM blocks WHERE height = ANY($1)")
            .bind(&heights)
            .fetch_all(&s.db.pool)
            .await?;
    let now = Utc::now();
    let list: Vec<Upgrade> = all
        .into_iter()
        .map(|u| {
            let activated = u.activation_height.is_some_and(|h| h <= tip);
            let remaining = u.activation_height.filter(|_| !activated).map(|h| h - tip);
            Upgrade {
                activated_at: u.activation_height.and_then(|h| {
                    times
                        .iter()
                        .find(|(t, _)| *t == h as i32)
                        .map(|(_, at)| *at)
                }),
                eta: u.activation_height.filter(|_| !activated).map(|h| {
                    now + chrono::Duration::seconds(upgrades::expected_seconds(tip, h) as i64)
                }),
                announced_date: announced(&u.name).filter(|_| !activated),
                summary: summary(&u.name),
                activation_height: u.activation_height,
                activated,
                blocks_remaining: remaining,
                target_block_time: u.target_block_time,
                label: label(&u.name),
                name: u.name,
            }
        })
        .collect();
    Ok(Json(Upgrades {
        network: if zscan_chain::is_testnet() {
            "testnet"
        } else {
            "mainnet"
        },
        height,
        current: list
            .iter()
            .rev()
            .find(|u| u.activated)
            .map(|u| u.name.clone()),
        next: list.iter().find(|u| !u.activated).map(|u| u.name.clone()),
        economics_pending: !upgrades::economics_known(tip + 1),
        upgrades: list,
    }))
}

#[cfg(test)]
mod tests {
    #[test]
    fn labels() {
        assert_eq!(super::label("Nu6_3"), "NU6.3");
        assert_eq!(super::label("Nu5"), "NU5");
        assert_eq!(super::label("Canopy"), "Canopy");
    }
}
