//! The NFT project registry (`labels/nft_projects.json`): curated facts about Zcash NFT
//! projects, each citing public sources, and the links to what ZScan detects on chain.

use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

/// Where a project's items live, which bounds what ZScan can verify.
pub const KINDS: [&str; 5] = [
    "inscription",
    "marketplace",
    "zsa-testnet",
    "other-chain",
    "upcoming",
];

/// Marketplaces with a supported public API.
pub const MARKETS: [&str; 4] = ["zemon", "zilkroad", "zecpad", "zeccat"];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Link {
    #[serde(alias = "name")]
    pub label: String,
    pub url: String,
}

/// What ZScan detects on chain for a project.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Onchain {
    /// ZRC-721 collection name (uppercase).
    pub zrc721: Option<String>,
    /// Launchpad drop: the address every mint's commit transaction paid.
    pub drop: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    /// One of [`MARKETS`].
    pub kind: String,
    pub url: String,
    /// The collection's id on the venue, when it hosts several.
    pub id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub slug: String,
    pub name: String,
    pub description: String,
    /// One of [`KINDS`].
    pub kind: String,
    pub website: Option<String>,
    pub x: Option<String>,
    pub discord: Option<String>,
    pub telegram: Option<String>,
    /// Logo or representative image: HTTPS URL or `ipfs://` path.
    pub logo: Option<String>,
    pub supply: Option<i64>,
    pub minted_at: Option<String>,
    pub mint_price_zec: Option<f64>,
    #[serde(default)]
    pub onchain: Onchain,
    pub market: Option<Market>,
    /// Other places it trades, without a supported API.
    #[serde(default)]
    pub marketplaces: Vec<Link>,
    /// verified | likely
    pub confidence: String,
    pub sources: Vec<Link>,
}

#[derive(Deserialize)]
struct File {
    projects: Vec<Project>,
}

/// The registry, parsed once.
pub fn projects() -> &'static [Project] {
    static P: OnceLock<Vec<Project>> = OnceLock::new();
    P.get_or_init(|| {
        serde_json::from_str::<File>(include_str!("../../../labels/nft_projects.json"))
            .expect("labels/nft_projects.json is valid")
            .projects
    })
}

pub fn by_slug(slug: &str) -> Option<&'static Project> {
    projects().iter().find(|p| p.slug == slug)
}

/// The project a detected on-chain collection belongs to.
pub fn for_zrc721(name: &str) -> Option<&'static Project> {
    projects()
        .iter()
        .find(|p| p.onchain.zrc721.as_deref() == Some(name))
}

pub fn for_drop(key: &str) -> Option<&'static Project> {
    projects()
        .iter()
        .find(|p| p.onchain.drop.as_deref() == Some(key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn registry_is_consistent() {
        let ps = projects();
        assert!(!ps.is_empty());
        let mut slugs = HashSet::new();
        for p in ps {
            assert!(slugs.insert(&p.slug), "duplicate slug {}", p.slug);
            assert!(
                p.slug
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
                "slug {}",
                p.slug
            );
            assert!(
                KINDS.contains(&p.kind.as_str()),
                "{}: kind {}",
                p.slug,
                p.kind
            );
            assert!(
                ["verified", "likely"].contains(&p.confidence.as_str()),
                "{}: confidence",
                p.slug
            );
            assert!(!p.sources.is_empty(), "{}: needs a source", p.slug);
            let urls = [&p.website, &p.x, &p.discord, &p.telegram]
                .into_iter()
                .flatten()
                .chain(p.sources.iter().map(|s| &s.url))
                .chain(p.marketplaces.iter().map(|m| &m.url))
                .chain(p.market.iter().map(|m| &m.url));
            for u in urls {
                assert!(u.starts_with("https://"), "{}: {u}", p.slug);
            }
            if let Some(l) = &p.logo {
                assert!(
                    l.starts_with("https://") || l.starts_with("ipfs://"),
                    "{}: logo",
                    p.slug
                );
            }
            if let Some(m) = &p.market {
                assert!(MARKETS.contains(&m.kind.as_str()), "{}: market", p.slug);
            }
            if let Some(d) = &p.onchain.drop {
                assert!(d.starts_with("t1") && d.len() == 35, "{}: drop", p.slug);
            }
            if p.onchain.zrc721.is_some() || p.onchain.drop.is_some() {
                assert_eq!(p.kind, "inscription", "{}: on-chain link", p.slug);
            }
        }
    }
}
