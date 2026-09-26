//! Address labels and the protocol registry behind the Protocols page.
//!
//! Labels come from four places, each verifiable: the curated `labels/addresses.json` (every
//! entry cites a public source), consensus rules (funding-stream recipients), coinbase
//! signatures (mining pools) and protocols' own public APIs (bridge vaults).

use std::sync::OnceLock;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Protocol {
    pub slug: String,
    pub name: String,
    /// shielded_pool | treasury | bridge | exchange | fund | mining_pool
    pub category: String,
    pub website: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Label {
    pub address: String,
    pub name: String,
    pub category: String,
    pub protocol: Option<String>,
    pub source: String,
    /// curated | consensus | coinbase | api
    pub origin: &'static str,
    /// False for addresses the protocol no longer uses (kept so their history stays attributed).
    pub active: bool,
}

#[derive(Deserialize)]
struct ProtocolsFile {
    protocols: Vec<Protocol>,
}

#[derive(Deserialize)]
struct LabelsFile {
    labels: Vec<CuratedLabel>,
}

#[derive(Deserialize)]
struct CuratedLabel {
    address: String,
    name: String,
    category: String,
    protocol: Option<String>,
    source: String,
}

/// The curated protocol registry (`labels/protocols.json`).
pub fn protocols() -> &'static [Protocol] {
    static P: OnceLock<Vec<Protocol>> = OnceLock::new();
    P.get_or_init(|| {
        serde_json::from_str::<ProtocolsFile>(include_str!("../../../labels/protocols.json"))
            .expect("labels/protocols.json is valid")
            .protocols
    })
}

/// Curated labels (`labels/addresses.json`) for the current network.
pub fn curated() -> Vec<Label> {
    let file: LabelsFile = serde_json::from_str(include_str!("../../../labels/addresses.json"))
        .expect("labels/addresses.json is valid");
    let testnet = crate::is_testnet();
    file.labels
        .into_iter()
        .filter(|l| l.address.starts_with("tm") == testnet)
        .map(|l| Label {
            protocol: l
                .protocol
                .or_else(|| (l.category == "mining_pool").then(|| slug(&l.name))),
            address: l.address,
            name: l.name,
            category: l.category,
            source: l.source,
            origin: "curated",
            active: true,
        })
        .collect()
}

/// URL-safe identifier for a name: "2Miners" → "2miners", "F2Pool (x)" → "f2pool-x".
pub fn slug(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('-') && !out.is_empty() {
            out.push('-');
        }
    }
    out.trim_end_matches('-').to_string()
}

const MAYANODE: &str = "https://mayanode.mayachain.info";
const MAYA_MIDGARD: &str = "https://midgard.mayachain.info";

/// A Maya Protocol ZEC vault from its Asgard vault list.
#[derive(Debug, Clone, Serialize)]
pub struct MayaVault {
    pub address: String,
    pub active: bool,
    /// ZEC held according to Maya, zatoshis (Maya uses 8 decimals like ZEC).
    pub amount: i64,
}

/// Maya's ZEC vaults (active and retiring), from its public node API.
pub async fn maya_vaults(http: &reqwest::Client) -> Result<Vec<MayaVault>> {
    #[derive(Deserialize)]
    struct Vault {
        status: String,
        #[serde(default)]
        addresses: Vec<Addr>,
        #[serde(default)]
        coins: Vec<Coin>,
    }
    #[derive(Deserialize)]
    struct Addr {
        chain: String,
        address: String,
    }
    #[derive(Deserialize)]
    struct Coin {
        asset: String,
        amount: String,
    }
    let vaults: Vec<Vault> = http
        .get(format!("{MAYANODE}/mayachain/vaults/asgard"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .context("maya vaults")?;
    Ok(vaults
        .into_iter()
        .filter_map(|v| {
            let address = v.addresses.into_iter().find(|a| a.chain == "ZEC")?.address;
            let amount = v
                .coins
                .iter()
                .find(|c| c.asset == "ZEC.ZEC")
                .and_then(|c| c.amount.parse().ok())
                .unwrap_or(0);
            Some(MayaVault {
                address,
                active: v.status == "ActiveVault",
                amount,
            })
        })
        .collect())
}

/// Maya's ZEC pool (depth, volume, status) and whether ZEC trading is halted.
pub async fn maya_stats(http: &reqwest::Client) -> Result<serde_json::Value> {
    let pool: serde_json::Value = http
        .get(format!("{MAYA_MIDGARD}/v2/pool/ZEC.ZEC"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .context("maya pool")?;
    let inbound: Vec<serde_json::Value> = http
        .get(format!("{MAYANODE}/mayachain/inbound_addresses"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .context("maya inbound")?;
    let zec = inbound.iter().find(|i| i["chain"] == "ZEC");
    let num = |k: &str| pool[k].as_str().and_then(|v| v.parse::<f64>().ok());
    Ok(serde_json::json!({
        "status": pool["status"],
        "halted": zec.and_then(|z| z["halted"].as_bool()),
        "trading_paused": zec.and_then(|z| z["chain_trading_paused"].as_bool()),
        // ZEC in the pool, zatoshis.
        "asset_depth": num("assetDepth").map(|v| v as i64),
        // Maya reports 24h volume in CACAO (10 decimals); kept as reported.
        "volume_24h_cacao": num("volume24h"),
        "shielded_deposits": zec.and_then(|z| z["shielded_memo_config"]["enabled"].as_bool()),
        "source": format!("{MAYA_MIDGARD}/v2/pool/ZEC.ZEC"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_and_curated_parse() {
        assert!(protocols().iter().any(|p| p.slug == "ironwood"));
        let c = curated();
        assert!(c.iter().all(|l| !l.source.is_empty()));
        assert!(c.iter().any(|l| l.protocol.as_deref() == Some("foundry")));
    }

    #[test]
    fn slugs() {
        assert_eq!(slug("2Miners"), "2miners");
        assert_eq!(slug("F2Pool (x)"), "f2pool-x");
        assert_eq!(slug("Coinholder Fund"), "coinholder-fund");
    }
}
