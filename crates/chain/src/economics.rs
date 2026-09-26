//! Protocol block economics: subsidy, funding streams (dev fund) and the deferred lockbox,
//! computed with Zebra's consensus rules so ZScan's supply numbers match the network exactly.

use std::collections::HashSet;

use serde::Serialize;
use zebra_chain::{
    block::Height,
    parameters::{
        subsidy::{block_subsidy, funding_stream_values, FundingStreamReceiver},
        Network,
    },
};

/// Expected issuance for a block.
#[derive(Debug, Clone, Default, Serialize)]
pub struct BlockEconomics {
    /// New ZEC created by this block (zatoshis).
    pub subsidy: i64,
    /// Portion of the subsidy paid to funding-stream addresses (e.g. Zcash Community Grants).
    pub funding_streams: i64,
    /// Portion deferred to the lockbox / coinholder-controlled fund (ZIP 1015/1016).
    pub deferred: i64,
    /// Portion left for the miner.
    pub miner_subsidy: i64,
}

/// `None` where the subsidy rules aren't implemented yet (past an announced NU7 height that
/// `zebra-chain` doesn't know; see [`crate::upgrades::economics_known`]).
pub fn block_economics(height: u32, net: &Network) -> Option<BlockEconomics> {
    if !crate::upgrades::economics_known(height) {
        return None;
    }
    let h = Height(height);
    let Ok(subsidy) = block_subsidy(h, net) else {
        return Some(BlockEconomics::default());
    };
    let streams = funding_stream_values(h, net, subsidy).unwrap_or_default();
    let mut funding = 0i64;
    let mut deferred = 0i64;
    for (receiver, amount) in streams {
        if receiver == FundingStreamReceiver::Deferred {
            deferred += i64::from(amount);
        } else {
            funding += i64::from(amount);
        }
    }
    let subsidy = i64::from(subsidy);
    Some(BlockEconomics {
        subsidy,
        funding_streams: funding,
        deferred,
        miner_subsidy: subsidy - funding - deferred,
    })
}

/// Transparent addresses of every non-deferred funding-stream recipient active at `height`.
pub fn funding_stream_addresses(height: u32, net: &Network) -> HashSet<String> {
    let mut out = HashSet::new();
    if let Some(fs) = net.funding_streams(Height(height)) {
        for (receiver, recipient) in fs.recipients() {
            if *receiver != FundingStreamReceiver::Deferred {
                out.extend(recipient.addresses().iter().map(|a| a.to_string()));
            }
        }
    }
    out
}

/// Blocks per halving period after Blossom, and the next halving height after `height`.
pub fn next_halving(height: u32, net: &Network) -> Option<u32> {
    use zebra_chain::parameters::subsidy::halving;
    let current = halving(Height(height), net);
    // Walk forward in coarse steps, then refine: halvings are ~1.68M blocks apart.
    let mut lo = height;
    let mut hi = height + 4_000_000;
    if halving(Height(hi), net) == current {
        return None;
    }
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if halving(Height(mid), net) == current {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    Some(crate::upgrades::rescale_for_nu7(hi))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mainnet_post_nu6_1_economics() {
        let e = block_economics(3_497_141, &Network::Mainnet).unwrap();
        // 1.5625 ZEC subsidy: 8% to ZCG, 12% deferred, 80% to the miner.
        assert_eq!(e.subsidy, 156_250_000);
        assert_eq!(e.funding_streams, 12_500_000);
        assert_eq!(e.deferred, 18_750_000);
        assert_eq!(e.miner_subsidy, 125_000_000);
        assert!(!funding_stream_addresses(3_497_141, &Network::Mainnet).is_empty());
        assert!(next_halving(3_497_141, &Network::Mainnet).unwrap() > 3_497_141);
    }
}
