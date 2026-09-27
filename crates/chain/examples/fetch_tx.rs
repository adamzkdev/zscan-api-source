//! Prints raw transactions as hex, e.g. to capture test vectors.
//! Usage: cargo run -p zscan-chain --example fetch_tx -- <txid> [txid...]

use zscan_chain::Lwd;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("LWD_URL").unwrap_or("https://zec.rocks:443".into());
    let lwd = Lwd::connect(&url)?;
    for txid in std::env::args().skip(1) {
        match lwd.raw_tx(&txid).await? {
            Some(r) => println!("{txid} {} {}", r.height, hex::encode(&r.data)),
            None => println!("{txid} not found"),
        }
    }
    Ok(())
}
