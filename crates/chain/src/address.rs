//! Any Zcash address: type, network, the pools it can receive in, and its receivers.

use serde::Serialize;
use zcash_address::{unified, ConversionError, TryFromAddress, ZcashAddress};
use zcash_protocol::{consensus::NetworkType, PoolType};

#[derive(Debug, Clone, Serialize)]
pub struct AddressInfo {
    pub address: String,
    /// mainnet | testnet | regtest
    pub network: String,
    /// transparent_p2pkh | transparent_p2sh | tex | sapling | sprout | unified
    pub kind: String,
    /// Pools it can be paid in: transparent, sapling, orchard (Orchard receivers also receive
    /// Ironwood notes).
    pub pools: Vec<String>,
    pub can_receive_memo: bool,
    pub transparent_only: bool,
    /// The public key or script hash, or the Sapling/Sprout payment address bytes, hex.
    /// Unified addresses list theirs per receiver.
    pub raw: Option<String>,
    /// For a TEX address (ZIP-320): the ordinary P2PKH address it pays on-chain.
    pub p2pkh_equivalent: Option<String>,
    pub receivers: Option<Vec<crate::unified::Receiver>>,
}

/// Kind and payload of a parsed address.
struct Parsed(&'static str, Option<String>);

impl TryFromAddress for Parsed {
    type Error = std::convert::Infallible;

    fn try_from_sprout(_: NetworkType, d: [u8; 64]) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Parsed("sprout", Some(hex::encode(d))))
    }
    fn try_from_sapling(_: NetworkType, d: [u8; 43]) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Parsed("sapling", Some(hex::encode(d))))
    }
    fn try_from_unified(
        _: NetworkType,
        _: unified::Address,
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Parsed("unified", None))
    }
    fn try_from_transparent_p2pkh(
        _: NetworkType,
        d: [u8; 20],
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Parsed("transparent_p2pkh", Some(hex::encode(d))))
    }
    fn try_from_transparent_p2sh(
        _: NetworkType,
        d: [u8; 20],
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Parsed("transparent_p2sh", Some(hex::encode(d))))
    }
    fn try_from_tex(_: NetworkType, d: [u8; 20]) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Parsed("tex", Some(hex::encode(d))))
    }
}

/// Decodes and checksums any Zcash address, on any network. None if it is not one.
pub fn inspect_any(s: &str) -> Option<AddressInfo> {
    let s = s.trim();
    let addr = ZcashAddress::try_from_encoded(s).ok()?;
    let pools = [
        (PoolType::TRANSPARENT, "transparent"),
        (PoolType::SAPLING, "sapling"),
        (PoolType::ORCHARD, "orchard"),
    ]
    .into_iter()
    .filter(|(p, _)| addr.can_receive_as(*p))
    .map(|(_, n)| n.to_string())
    .collect();
    let can_receive_memo = addr.can_receive_memo();
    let transparent_only = addr.is_transparent_only();
    let (net, Parsed(kind, raw)) = addr.convert::<(NetworkType, Parsed)>().ok()?;
    let network = match net {
        NetworkType::Main => "mainnet",
        NetworkType::Test => "testnet",
        NetworkType::Regtest => "regtest",
    };
    Some(AddressInfo {
        address: s.to_string(),
        network: network.into(),
        kind: kind.into(),
        pools,
        can_receive_memo,
        transparent_only,
        raw,
        p2pkh_equivalent: (kind == "tex")
            .then(|| crate::parse::tex_to_p2pkh(s))
            .flatten(),
        receivers: (kind == "unified")
            .then(|| crate::unified::inspect(s).map(|u| u.receivers))
            .flatten(),
    })
}

#[cfg(test)]
mod tests {
    use super::inspect_any;

    #[test]
    fn inspects_every_kind() {
        let t = inspect_any("t1Hsc1LR8yKnbbe3twRp88p6vFfC5t7DLbs").unwrap();
        assert_eq!(
            (t.kind.as_str(), t.network.as_str()),
            ("transparent_p2pkh", "mainnet")
        );
        assert_eq!(t.pools, ["transparent"]);
        assert!(t.transparent_only && !t.can_receive_memo);

        let ua = inspect_any("u157q45z9fwy7349t5r7w4d0tpmdzhw6xvuwm4ucqq80ysftk9wxryrru0l7puxpdlujnkmj5d9ewzaxz5x6slfg3xys4egayx7vry57wd").unwrap();
        assert_eq!(ua.kind, "unified");
        assert!(ua.receivers.is_some_and(|r| !r.is_empty()));

        assert!(inspect_any("t1Hsc1LR8yKnbbe3twRp88p6vFfC5t7DLbX").is_none());
        assert!(inspect_any("hello").is_none());
    }
}
