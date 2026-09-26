//! Runs GetTransaction at a fixed rate for 60s and reports rate-limit errors.
//! Usage: cargo run -p zscan-chain --example probe_rate -- <url> <req_per_sec>

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use zscan_chain::Lwd;

const TXID: &str = "39d1e0130e5f8381c6960cdf6e0930ea9351f3fd1b405243ebf3561d733b294c";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::args().nth(1).unwrap();
    let rate: u64 = std::env::args().nth(2).unwrap().parse()?;
    let lwd = Lwd::connect(&url)?;
    let (ok, err) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let mut tick = tokio::time::interval(Duration::from_micros(1_000_000 / rate));
    for _ in 0..rate * 60 {
        tick.tick().await;
        let (lwd, ok, err) = (lwd.clone(), ok.clone(), err.clone());
        tokio::spawn(async move {
            match lwd.raw_tx(TXID).await {
                Ok(_) => ok.fetch_add(1, Ordering::Relaxed),
                Err(_) => err.fetch_add(1, Ordering::Relaxed),
            };
        });
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!(
        "{url} @ {rate}/s: ok={} err={}",
        ok.load(Ordering::Relaxed),
        err.load(Ordering::Relaxed)
    );
    Ok(())
}
