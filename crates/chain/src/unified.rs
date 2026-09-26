//! Unified Address inspection: which pools a UA can receive in.

use serde::Serialize;
use zcash_address::unified::{self, Container, Encoding};
use zebra_chain::{parameters::NetworkKind, transparent};

#[derive(Debug, Clone, Serialize)]
pub struct Receiver {
    /// transparent_p2pkh | transparent_p2sh | sapling | orchard | unknown
    pub kind: String,
    /// Human description of the pools this receiver can be paid in.
    pub pools: String,
    /// Encoded address for transparent receivers (links to the explorer).
    pub address: Option<String>,
    /// Raw receiver bytes, hex.
    pub raw: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct UnifiedInfo {
    pub network: String,
    pub receivers: Vec<Receiver>,
}

pub fn inspect(ua: &str) -> Option<UnifiedInfo> {
    let (net, addr) = unified::Address::decode(ua).ok()?;
    let kind = match net {
        zcash_protocol::consensus::NetworkType::Main => NetworkKind::Mainnet,
        zcash_protocol::consensus::NetworkType::Test => NetworkKind::Testnet,
        zcash_protocol::consensus::NetworkType::Regtest => NetworkKind::Regtest,
    };
    let receivers = addr
        .items()
        .into_iter()
        .map(|r| match r {
            unified::Receiver::P2pkh(h) => Receiver {
                kind: "transparent_p2pkh".into(),
                pools: "Transparent (public)".into(),
                address: Some(transparent::Address::from_pub_key_hash(kind, h).to_string()),
                raw: hex::encode(h),
            },
            unified::Receiver::P2sh(h) => Receiver {
                kind: "transparent_p2sh".into(),
                pools: "Transparent (public)".into(),
                address: Some(transparent::Address::from_script_hash(kind, h).to_string()),
                raw: hex::encode(h),
            },
            unified::Receiver::Sapling(b) => Receiver {
                kind: "sapling".into(),
                pools: "Sapling (shielded)".into(),
                address: None,
                raw: hex::encode(b),
            },
            // Ironwood reuses Orchard's address format, so this receiver serves both pools.
            unified::Receiver::Orchard(b) => Receiver {
                kind: "orchard".into(),
                pools: "Orchard / Ironwood (shielded)".into(),
                address: None,
                raw: hex::encode(b),
            },
            unified::Receiver::Unknown { typecode, data } => Receiver {
                kind: "unknown".into(),
                pools: format!("Unknown receiver type {typecode:#x}"),
                address: None,
                raw: hex::encode(data),
            },
        })
        .collect();
    Some(UnifiedInfo {
        network: format!("{kind:?}").to_lowercase(),
        receivers,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn inspects_mainnet_ua() {
        let ua = "u157q45z9fwy7349t5r7w4d0tpmdzhw6xvuwm4ucqq80ysftk9wxryrru0l7puxpdlujnkmj5d9ewzaxz5x6slfg3xys4egayx7vry57wd";
        let info = super::inspect(ua).unwrap();
        assert_eq!(info.network, "mainnet");
        assert!(!info.receivers.is_empty());
        assert!(super::inspect("u1nope").is_none());
    }
}
