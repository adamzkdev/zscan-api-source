//! The treasury registry: shielded accounts whose owners chose to make them public by
//! publishing a unified full viewing key. ZScan scans each one and shows its balance, inflows,
//! outflows and memos at proof.zscan.cash. The list is `labels/treasuries.json`; an entry is
//! added by pull request, with public sources showing the owner published the key.

use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::nft_projects::Link;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Treasury {
    /// URL key: lowercase letters, digits, dashes.
    pub slug: String,
    pub name: String,
    /// One or two factual sentences: who holds the account and what it is for.
    pub description: String,
    /// What the owner says the account holds or is used for, in their words.
    pub statement: Option<String>,
    /// Unified full viewing key (`uview1…`) of a dedicated account.
    pub ufvk: String,
    /// First block to scan: the account can't have received anything before it.
    pub birthday_height: u32,
    pub website: Option<String>,
    pub x: Option<String>,
    /// Where the owner published the key (announcement, forum post, repository).
    pub sources: Vec<Link>,
}

#[derive(Deserialize)]
struct File {
    treasuries: Vec<Treasury>,
}

/// The registry bundled at build time.
pub fn list() -> &'static [Treasury] {
    static LIST: OnceLock<Vec<Treasury>> = OnceLock::new();
    LIST.get_or_init(|| {
        serde_json::from_str::<File>(include_str!("../../../labels/treasuries.json"))
            .expect("labels/treasuries.json is valid")
            .treasuries
    })
}

pub fn get(slug: &str) -> Option<&'static Treasury> {
    list().iter().find(|t| t.slug == slug)
}

#[cfg(test)]
mod tests {
    #[test]
    fn registry_parses_with_unique_slugs() {
        let list = super::list();
        let mut slugs: Vec<_> = list.iter().map(|t| t.slug.as_str()).collect();
        slugs.sort();
        slugs.dedup();
        assert_eq!(slugs.len(), list.len(), "duplicate slug");
        for t in list {
            assert!(
                t.ufvk.starts_with("uview"),
                "{}: a unified full viewing key is required",
                t.slug
            );
            assert!(!t.sources.is_empty(), "{}: sources are required", t.slug);
        }
    }
}
