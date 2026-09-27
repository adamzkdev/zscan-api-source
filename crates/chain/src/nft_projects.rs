//! The NFT project registry: curated facts about Zcash NFT collections, each citing public
//! sources, and the links to what ZScan detects on chain. It's maintained in the public
//! github.com/adamzkdev/zscan-nft-registry; `labels/nft_projects.json` is the bundled copy used
//! until (or when) the live one can't be read.

use std::collections::HashSet;
use std::sync::{OnceLock, RwLock};

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
    #[serde(alias = "projects")]
    collections: Vec<Project>,
}

/// Parses and checks a registry document.
pub fn parse(json: &str) -> Result<Vec<Project>, String> {
    let list = serde_json::from_str::<File>(json)
        .map_err(|e| e.to_string())?
        .collections;
    validate(&list)?;
    Ok(list)
}

/// The rules every registry version must follow.
pub fn validate(ps: &[Project]) -> Result<(), String> {
    let mut slugs = HashSet::new();
    let https = |u: &str| u.starts_with("https://");
    for p in ps {
        let err = |m: &str| Err(format!("{}: {m}", p.slug));
        if !slugs.insert(&p.slug) {
            return err("duplicate slug");
        }
        if p.slug.is_empty()
            || !p
                .slug
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return err("bad slug");
        }
        if !KINDS.contains(&p.kind.as_str()) {
            return err("unknown kind");
        }
        if !["verified", "likely"].contains(&p.confidence.as_str()) {
            return err("unknown confidence");
        }
        if p.sources.is_empty() {
            return err("needs a source");
        }
        let urls = [&p.website, &p.x, &p.discord, &p.telegram]
            .into_iter()
            .flatten()
            .chain(p.sources.iter().map(|s| &s.url))
            .chain(p.marketplaces.iter().map(|m| &m.url))
            .chain(p.market.iter().map(|m| &m.url));
        for u in urls {
            if !https(u) {
                return err(&format!("not an https link: {u}"));
            }
        }
        if p.logo
            .as_deref()
            .is_some_and(|l| !https(l) && !l.starts_with("ipfs://"))
        {
            return err("logo must be https or ipfs");
        }
        if p.market
            .as_ref()
            .is_some_and(|m| !MARKETS.contains(&m.kind.as_str()))
        {
            return err("unsupported market");
        }
        if p.onchain
            .drop
            .as_deref()
            .is_some_and(|d| !(d.starts_with("t1") && d.len() == 35))
        {
            return err("bad drop address");
        }
        if (p.onchain.zrc721.is_some() || p.onchain.drop.is_some()) && p.kind != "inscription" {
            return err("only inscription collections link to the chain");
        }
    }
    Ok(())
}

fn current() -> &'static RwLock<(u64, &'static [Project])> {
    static C: OnceLock<RwLock<(u64, &'static [Project])>> = OnceLock::new();
    C.get_or_init(|| {
        let bundled = parse(include_str!("../../../labels/nft_projects.json"))
            .expect("labels/nft_projects.json is valid");
        RwLock::new((0, Vec::leak(bundled)))
    })
}

/// The registry in use: the live version once loaded, else the bundled copy.
pub fn projects() -> &'static [Project] {
    current().read().unwrap().1
}

/// Installs a new registry version (already parsed). Returns false when it's unchanged. Old
/// versions stay allocated (they may still be borrowed), which is fine as changes are rare.
pub fn replace(source: &str, list: Vec<Project>) -> bool {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    source.hash(&mut h);
    let hash = h.finish();
    let mut c = current().write().unwrap();
    if c.0 == hash {
        return false;
    }
    *c = (hash, Vec::leak(list));
    true
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

    #[test]
    fn bundled_registry_is_valid() {
        let ps = parse(include_str!("../../../labels/nft_projects.json")).unwrap();
        assert!(!ps.is_empty());
    }

    #[test]
    fn invalid_versions_are_rejected() {
        let ok = r#"{"collections":[{"slug":"a","name":"A","description":"d","kind":"marketplace","confidence":"verified","sources":[{"label":"s","url":"https://x.y"}]}]}"#;
        assert!(parse(ok).is_ok());
        assert!(parse(&ok.replace("https://x.y", "http://x.y")).is_err());
        assert!(parse(&ok.replace(r#""kind":"marketplace""#, r#""kind":"other""#)).is_err());
        assert!(parse(&ok.replace(
            r#""sources":[{"label":"s","url":"https://x.y"}]"#,
            r#""sources":[]"#
        ))
        .is_err());
    }
}
