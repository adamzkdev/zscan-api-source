//! Chain access for ZScan: lightwalletd gRPC, zebrad JSON-RPC, and transaction decoding.

pub mod classify;
pub mod consts;
pub mod economics;
pub mod hash;
pub mod limiter;
pub mod lwd;
pub mod miner;
pub mod parse;
pub mod resolve;
pub mod rpc;
pub mod unified;
pub mod upgrades;

pub mod proto {
    tonic::include_proto!("cash.z.wallet.sdk.rpc");
}

pub use classify::{summarize, TxKind, TxSummary};
pub use lwd::Lwd;
pub use parse::{parse_tx, ParsedTx};
pub use resolve::{PrevOut, Resolver};
pub use rpc::Rpc;

use std::sync::OnceLock;

/// The network ZScan runs against, from `ZSCAN_NETWORK` (`mainnet` default, or `testnet`).
pub fn network() -> &'static zebra_chain::parameters::Network {
    static NET: OnceLock<zebra_chain::parameters::Network> = OnceLock::new();
    NET.get_or_init(|| match std::env::var("ZSCAN_NETWORK").as_deref() {
        Ok("testnet") => zebra_chain::parameters::Network::new_default_testnet(),
        _ => zebra_chain::parameters::Network::Mainnet,
    })
}

pub fn is_testnet() -> bool {
    matches!(std::env::var("ZSCAN_NETWORK").as_deref(), Ok("testnet"))
}

/// HTTP client builder shared by ZScan services. Ambient `HTTP(S)_PROXY` variables are ignored
/// (container runtimes inject them, sometimes broken); set `ZSCAN_HTTP_PROXY` to use a proxy.
pub fn http_client_builder() -> reqwest::ClientBuilder {
    let b = reqwest::Client::builder().no_proxy();
    match std::env::var("ZSCAN_HTTP_PROXY")
        .ok()
        .filter(|p| !p.is_empty())
    {
        Some(p) => match reqwest::Proxy::all(&p) {
            Ok(proxy) => b.proxy(proxy),
            Err(e) => {
                tracing::warn!("invalid ZSCAN_HTTP_PROXY: {e}");
                b
            }
        },
        None => b,
    }
}
