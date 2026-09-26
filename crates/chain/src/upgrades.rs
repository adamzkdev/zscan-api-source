//! Network upgrades and block timing, including upgrades announced before the `zebra-chain`
//! release that implements them.
//!
//! NU7 (ZIP 218) cuts the block target spacing from 75s to 25s and divides the per-block subsidy
//! accordingly. Until `zebra-chain` ships its activation height and rules, operators set
//! `NU7_ACTIVATION_HEIGHT` once it is announced: ZScan then projects times with 25s blocks and
//! leaves post-NU7 block economics unknown rather than computing them with pre-NU7 rules.

use std::sync::OnceLock;

use serde::Serialize;
use zebra_chain::{block::Height, parameters::NetworkUpgrade};

use crate::network;

/// NU7 block target spacing (ZIP 218), in seconds.
pub const NU7_TARGET_SPACING: u32 = 25;
/// Blocks per halving after NU7 (ZIP 218): three times the post-Blossom interval.
const NU7_HALVING_RATIO: u32 = 3;

/// Height NU7 activates at: `zebra-chain`'s value when it has one, else `NU7_ACTIVATION_HEIGHT`.
pub fn nu7_height() -> Option<u32> {
    static H: OnceLock<Option<u32>> = OnceLock::new();
    *H.get_or_init(|| {
        zebra_nu7_height().or_else(|| {
            std::env::var("NU7_ACTIVATION_HEIGHT")
                .ok()
                .and_then(|v| v.trim().parse().ok())
        })
    })
}

/// Whether `zebra-chain` itself implements NU7 for this network.
fn zebra_nu7_height() -> Option<u32> {
    NetworkUpgrade::Nu7
        .activation_height(network())
        .map(|h| h.0)
}

/// Whether our consensus library computes the subsidy correctly at `height`. False past an NU7
/// height that `zebra-chain` doesn't know yet: those blocks keep unknown economics (NULL) and are
/// filled in after upgrading `zebra-chain`.
pub fn economics_known(height: u32) -> bool {
    match nu7_height() {
        Some(a) if height >= a => zebra_nu7_height().is_some(),
        _ => true,
    }
}

/// Target seconds between blocks at `height`.
pub fn target_spacing(height: u32) -> u32 {
    let base = NetworkUpgrade::target_spacing_for_height(network(), Height(height)).num_seconds();
    spacing_with(nu7_height(), base as u32, height)
}

/// Expected seconds for the chain to grow from `from` to `to` at target spacing.
pub fn expected_seconds(from: u32, to: u32) -> u64 {
    let base = NetworkUpgrade::target_spacing_for_height(network(), Height(from + 1)).num_seconds();
    seconds_with(nu7_height(), base as u32, from, to)
}

/// Converts a height computed with pre-NU7 halving intervals to its post-NU7 equivalent: the
/// halving keeps its date, so blocks past activation count three times.
pub fn rescale_for_nu7(height: u32) -> u32 {
    if zebra_nu7_height().is_some() {
        return height;
    }
    rescale_with(nu7_height(), height)
}

fn spacing_with(nu7: Option<u32>, pre_nu7: u32, height: u32) -> u32 {
    match nu7 {
        Some(a) if height >= a => NU7_TARGET_SPACING,
        _ => pre_nu7,
    }
}

/// `pre_nu7` is the spacing in effect at `from + 1` if that is before NU7.
fn seconds_with(nu7: Option<u32>, pre_nu7: u32, from: u32, to: u32) -> u64 {
    if to <= from {
        return 0;
    }
    let (pre, nu7s) = (pre_nu7 as u64, NU7_TARGET_SPACING as u64);
    match nu7 {
        Some(a) if from + 1 >= a => (to - from) as u64 * nu7s,
        Some(a) if to >= a => (a - 1 - from) as u64 * pre + (to - a + 1) as u64 * nu7s,
        _ => (to - from) as u64 * pre,
    }
}

fn rescale_with(nu7: Option<u32>, height: u32) -> u32 {
    match nu7 {
        Some(a) if height > a => a + (height - a) * NU7_HALVING_RATIO,
        _ => height,
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct UpgradeInfo {
    pub name: String,
    pub activation_height: Option<u32>,
    /// Target block spacing once active, in seconds.
    pub target_block_time: u32,
}

/// Every upgrade from Overwinter on, with activation heights for this network.
pub fn upgrades() -> Vec<UpgradeInfo> {
    NetworkUpgrade::iter()
        .filter(|u| *u >= NetworkUpgrade::Overwinter)
        .map(|u| {
            let height = if u == NetworkUpgrade::Nu7 {
                nu7_height()
            } else {
                u.activation_height(network()).map(|h| h.0)
            };
            let target_block_time = if u == NetworkUpgrade::Nu7 {
                NU7_TARGET_SPACING
            } else {
                u.target_spacing().num_seconds() as u32
            };
            UpgradeInfo {
                name: u.to_string(),
                activation_height: height,
                target_block_time,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timing_across_nu7() {
        let a = Some(1_000);
        assert_eq!(spacing_with(a, 75, 999), 75);
        assert_eq!(spacing_with(a, 75, 1_000), 25);
        assert_eq!(spacing_with(None, 75, 5_000), 75);
        // 989 → 999 at 75s, then 1000..=1009 at 25s.
        assert_eq!(seconds_with(a, 75, 989, 1_009), 10 * 75 + 10 * 25);
        assert_eq!(seconds_with(a, 25, 1_000, 1_010), 250);
        assert_eq!(seconds_with(None, 75, 0, 10), 750);
        assert_eq!(seconds_with(a, 75, 10, 10), 0);
    }

    #[test]
    fn halving_keeps_its_date() {
        // A halving 1,000 pre-NU7 blocks after activation lands 3,000 NU7 blocks after it,
        // i.e. the same 75,000 seconds later.
        assert_eq!(rescale_with(Some(1_000), 2_000), 4_000);
        assert_eq!(rescale_with(Some(1_000), 900), 900);
        assert_eq!(rescale_with(None, 2_000), 2_000);
    }

    #[test]
    fn lists_upgrades() {
        let names: Vec<_> = upgrades().into_iter().map(|u| u.name).collect();
        assert!(names.len() >= 10, "{names:?}");
        assert_eq!(names.first().map(String::as_str), Some("Overwinter"));
    }
}
