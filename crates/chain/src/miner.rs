//! Mining pool identification from coinbase data.

/// Printable ASCII runs (>= 4 chars) from coinbase script data, joined with spaces.
pub fn coinbase_tag(data_hex: &str) -> Option<String> {
    let bytes = hex::decode(data_hex).ok()?;
    let mut runs = Vec::new();
    let mut cur = String::new();
    for b in bytes {
        if (0x20..0x7f).contains(&b) {
            cur.push(b as char);
        } else {
            if cur.trim().len() >= 4 {
                runs.push(cur.trim().to_string());
            }
            cur.clear();
        }
    }
    if cur.trim().len() >= 4 {
        runs.push(cur.trim().to_string());
    }
    let tag = runs.join(" ");
    (!tag.is_empty()).then_some(tag)
}

/// Known pool signatures, matched case-insensitively against the coinbase tag.
const POOLS: &[(&str, &str)] = &[
    ("luxor", "Luxor"),
    ("foundry", "Foundry"),
    ("2miners", "2Miners"),
    ("kupool", "KuPool"),
    ("viabtc", "ViaBTC"),
    ("f2pool", "F2Pool"),
    ("antpool", "AntPool"),
    ("binance", "Binance Pool"),
    ("poolin.com", "Poolin"),
    ("flypool", "Flypool"),
    ("nanopool", "Nanopool"),
    ("emcd", "EMCD"),
    ("sunpool", "SunPool"),
    ("herominers", "HeroMiners"),
    ("mysolopool", "Mysolopool"),
    ("kryptex", "Kryptex"),
    ("mining-dutch", "Mining-Dutch"),
    ("solo", "Solo miner"),
];

/// Identifies a pool from coinbase text. "Mined by <name>" segments name a *worker* unless
/// `<name>` is exactly a known pool (e.g. "/Mined by Luxor/"), so they're matched strictly
/// and excluded from substring matching; otherwise a worker called "xf2pool1" would read as F2Pool.
pub fn pool_name(tag: &str) -> Option<&'static str> {
    let t = tag.to_ascii_lowercase();
    let mut rest = String::new();
    let mut s = t.as_str();
    while let Some(i) = s.find("mined by ") {
        rest.push_str(&s[..i]);
        let after = &s[i + "mined by ".len()..];
        let end = after
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '-' || c == '.'))
            .unwrap_or(after.len());
        let who = &after[..end];
        if let Some((_, name)) = POOLS.iter().find(|(k, _)| *k == who) {
            return Some(name);
        }
        s = &after[end..];
    }
    rest.push_str(s);
    POOLS
        .iter()
        .find(|(k, _)| rest.contains(k))
        .map(|(_, n)| *n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_tags_and_pools() {
        let hex = hex::encode(b"\x03\x0a\x0b\x0c/Mined by Luxor/");
        let tag = coinbase_tag(&hex).unwrap();
        assert_eq!(tag, "/Mined by Luxor/");
        assert_eq!(pool_name(&tag), Some("Luxor"));
        assert_eq!(
            pool_name("jFoundry Zcash Pool #PrivacyMatters"),
            Some("Foundry")
        );
        assert_eq!(coinbase_tag("0301020304"), None);
        assert_eq!(pool_name("Mined by ikkf2pool1"), None);
        assert_eq!(pool_name("Mined by poolinelnzhsji"), None);
        assert_eq!(pool_name(": mined by KuPool"), Some("KuPool"));
        assert_eq!(pool_name(": Mysolopool.com"), Some("Mysolopool"));
    }
}
