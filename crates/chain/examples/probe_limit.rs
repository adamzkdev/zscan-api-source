//! Measures a lightwalletd server's per-IP rate limit.
//! Usage: cargo run -p zscan-chain --example probe_limit -- https://zec.rocks:443 [concurrency]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use zscan_chain::Lwd;

const TXID: &str = "39d1e0130e5f8381c6960cdf6e0930ea9351f3fd1b405243ebf3561d733b294c";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or("https://zec.rocks:443".into());
    let conc: usize = std::env::args()
        .nth(2)
        .map(|c| c.parse().unwrap())
        .unwrap_or(8);
    let lwd = Lwd::connect(&url)?;
    let ok = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let mut handles = vec![];
    for _ in 0..conc {
        let (lwd, ok) = (lwd.clone(), ok.clone());
        handles.push(tokio::spawn(async move {
            loop {
                match lwd.raw_tx(TXID).await {
                    Ok(_) => {
                        ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => return e.to_string(),
                }
                if t0.elapsed().as_secs() > 60 {
                    return "no limit hit in 60s".into();
                }
            }
        }));
    }
    let first = futures::future::select_all(handles).await.0?;
    let n = ok.load(Ordering::Relaxed);
    let secs = t0.elapsed().as_secs_f64();
    println!(
        "limited after {n} ok requests in {secs:.1}s ({:.1} req/s): {first}",
        n as f64 / secs
    );
    // Measure recovery.
    let t1 = Instant::now();
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        if lwd.raw_tx(TXID).await.is_ok() {
            println!("recovered after {:.0}s", t1.elapsed().as_secs_f64());
            // How many succeed right after recovery?
            let mut k = 0;
            while lwd.raw_tx(TXID).await.is_ok() {
                k += 1;
                if k > 2000 {
                    break;
                }
            }
            println!("then {k} sequential requests before limiting again");
            return Ok(());
        }
    }
}
