//! zscan-verify: reproduce ZScan's supply audit without trusting ZScan.
//!
//! It takes two node snapshots N blocks apart, decodes every block in between from
//! lightwalletd, and checks that each value pool changed by exactly the decoded amount.
//!
//!   zscan-verify [blocks=2]
//!   env: LWD_URL (default https://zec.rocks:443), RPC_URL, RPC_RATE_PER_MIN (default 4)

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use zscan_chain::{
    classify::PoolDeltas, economics::block_economics, hash, parse_tx, rpc::BlockchainInfo,
    summarize, Lwd, Resolver, Rpc,
};

fn env_or(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    let n: u32 = std::env::args()
        .nth(1)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(2);
    let lwd = Lwd::connect_with(&env_or("LWD_URL", "https://zec.rocks:443"), 20.0)?;
    let rpc = Rpc::new(
        &env_or("RPC_URL", "https://zcash-mainnet-zebrad.gateway.tatum.io"),
        env_or("RPC_RATE_PER_MIN", "4").parse()?,
    );
    let resolver = Resolver::new(lwd.clone(), 100_000);

    let a = rpc.blockchain_info().await?;
    println!("snapshot A at block {}", a.blocks);
    println!("waiting for {n} new block(s)...");
    let b = loop {
        tokio::time::sleep(Duration::from_secs(30)).await;
        let b = rpc.blockchain_info().await?;
        if b.blocks >= a.blocks + n {
            break b;
        }
        println!("  node at {}", b.blocks);
    };
    println!("snapshot B at block {}", b.blocks);

    let (mut deltas, mut deferred, mut issued) = (PoolDeltas::default(), 0i64, 0i64);
    for h in a.blocks + 1..=b.blocks {
        let cb = lwd.block(h).await?.context("block not found")?;
        let mut fees = 0i64;
        let mut coinbase = 0i64;
        for ctx in &cb.vtx {
            let txid = hash::to_display(&ctx.txid);
            let raw = lwd.raw_tx(&txid).await?.context("tx not found")?;
            let tx = parse_tx(&raw.data)?;
            let prev = resolver.resolve(&tx.inputs, &HashMap::new()).await?;
            let values: Vec<_> = prev.iter().map(|p| p.as_ref().map(|p| p.value)).collect();
            let s = summarize(&tx, &values);
            deltas += s.deltas;
            fees += s.fee.unwrap_or(0);
            if tx.is_coinbase {
                coinbase = tx.transparent_out() - (tx.sapling_vb + tx.orchard_vb + tx.ironwood_vb);
            }
        }
        let e = block_economics(h, zscan_chain::network());
        deferred += e.deferred;
        issued += e.subsidy - (fees + e.subsidy - e.deferred - coinbase);
        println!("  decoded block {h}: {} txs", cb.vtx.len());
    }

    let diff = |id: &str, decoded: i64, a: &BlockchainInfo, b: &BlockchainInfo| {
        let node = b.pool(id).unwrap_or(0) - a.pool(id).unwrap_or(0);
        (id.to_string(), node, decoded, node - decoded)
    };
    let supply_node = b
        .chain_supply
        .as_ref()
        .map(|s| s.chain_value_zat)
        .unwrap_or(0)
        - a.chain_supply
            .as_ref()
            .map(|s| s.chain_value_zat)
            .unwrap_or(0);
    let rows = vec![
        diff("transparent", deltas.transparent, &a, &b),
        diff("sprout", deltas.sprout, &a, &b),
        diff("sapling", deltas.sapling, &a, &b),
        diff("orchard", deltas.orchard, &a, &b),
        diff("ironwood", deltas.ironwood, &a, &b),
        diff("lockbox", deferred, &a, &b),
        (
            "supply".to_string(),
            supply_node,
            issued,
            supply_node - issued,
        ),
    ];
    println!(
        "\n{:<12} {:>18} {:>18} {:>10}",
        "pool", "node Δ (zat)", "decoded Δ (zat)", "diff"
    );
    let mut ok = true;
    for (id, node, dec, d) in rows {
        ok &= d == 0;
        println!("{id:<12} {node:>18} {dec:>18} {d:>10}");
    }
    println!(
        "\n{}",
        if ok {
            "✓ supply verified: every pool matches to the zatoshi"
        } else {
            "✗ MISMATCH"
        }
    );
    std::process::exit(if ok { 0 } else { 1 });
}
