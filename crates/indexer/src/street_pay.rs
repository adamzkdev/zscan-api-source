//! ZStreet's shielded address (`STREET_PAY_UFVK`): scanned a block or two behind the tip with
//! the same viewing-key scanner as the treasuries. Every received note is stored with its memo;
//! a memo carrying `ZS1:<ref>` names the intent it pays, and the API plays it for everyone.
//!
//! `zscan-indexer street-keygen` creates the address once: a fresh 24-word seed (kept by the
//! operator, it can be restored in any Zcash wallet), its Orchard unified address and the full
//! viewing key the indexer needs. Nothing is written anywhere.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tracing::{info, warn};
use zcash_address::unified::{self, Encoding};
use zcash_protocol::consensus::NetworkType;
use zscan_db::sqlx;
use zscan_wasm::native::Key;

use crate::Ctx;

const CHUNK: u32 = 200;

fn when(secs: u32) -> DateTime<Utc> {
    DateTime::from_timestamp(secs as i64, 0).unwrap_or_default()
}

/// The intent reference in a memo: `ZS1:` then 8 characters of base32 (A-Z, 2-7).
pub fn memo_ref(memo: &str) -> Option<String> {
    let at = memo.find("ZS1:")?;
    let r: String = memo[at + 4..].chars().take(8).collect();
    (r.len() == 8 && r.bytes().all(|b| b.is_ascii_uppercase() || (b'2'..=b'7').contains(&b)))
        .then_some(r)
}

/// Prints a new seed, its unified address and its unified full viewing key (Orchard, account 0).
pub fn keygen(birthday: u32) -> Result<()> {
    let mnemonic = <bip0039::Mnemonic>::generate(bip0039::Count::Words24);
    let net = if zscan_chain::is_testnet() {
        NetworkType::Test
    } else {
        NetworkType::Main
    };
    let (ua, ufvk) = derive(&mnemonic.to_seed(""), net)?;
    println!("# ZStreet pay address — keep the seed offline, it spends what the address receives");
    println!("STREET_PAY_SEED=\"{}\"", mnemonic.phrase());
    println!("STREET_PAY_BIRTHDAY={birthday}");
    println!("STREET_PAY_ADDRESS={ua}");
    println!("STREET_PAY_UFVK={ufvk}");
    Ok(())
}

/// The Orchard unified address and unified full viewing key of a seed's account 0.
fn derive(seed: &[u8], net: NetworkType) -> Result<(String, String)> {
    let coin = if net == NetworkType::Main { 133 } else { 1 };
    let sk = orchard::keys::SpendingKey::from_zip32_seed(&seed, coin, zip32::AccountId::ZERO)
        .map_err(|_| anyhow::anyhow!("seed gives no Orchard key, run again"))?;
    let fvk = orchard::keys::FullViewingKey::from(&sk);
    let addr = fvk.address_at(0u32, orchard::keys::Scope::External);
    let ua = unified::Address::try_from_items(vec![unified::Receiver::Orchard(
        addr.to_raw_address_bytes(),
    )])
    .context("unified address")?;
    let ufvk = unified::Ufvk::try_from_items(vec![unified::Fvk::Orchard(fvk.to_bytes())])
        .context("unified viewing key")?;
    Ok((ua.encode(&net), ufvk.encode(&net)))
}

pub async fn run(ctx: &Ctx, lag: u32) -> Result<()> {
    let Some(ufvk) = std::env::var("STREET_PAY_UFVK")
        .ok()
        .filter(|k| !k.trim().is_empty())
    else {
        return Ok(());
    };
    let ufvk = ufvk.trim().to_string();
    let birthday: u32 = std::env::var("STREET_PAY_BIRTHDAY")
        .context("STREET_PAY_BIRTHDAY is required with STREET_PAY_UFVK")?
        .trim()
        .parse()?;
    let mut key = match Key::parse(&ufvk) {
        Ok(k) if k.is_full() => k,
        Ok(_) => {
            warn!("STREET_PAY_UFVK is not a full viewing key, street pay off");
            return Ok(());
        }
        Err(e) => {
            warn!("STREET_PAY_UFVK rejected: {e}");
            return Ok(());
        }
    };
    info!(birthday, "street pay scan starting");
    loop {
        let (tip, _) = ctx.lwd.tip().await?;
        scan(ctx, &ufvk, &mut key, birthday, tip.saturating_sub(lag)).await?;
        tokio::time::sleep(Duration::from_secs(20)).await;
    }
}

async fn cursor(ctx: &Ctx, ufvk: &str, birthday: u32) -> Result<u32> {
    let next: Option<i32> =
        sqlx::query_scalar("SELECT next_height FROM street_pay_sync WHERE ufvk = $1")
            .bind(ufvk)
            .fetch_optional(&ctx.db.pool)
            .await?;
    match next {
        Some(n) => Ok(n as u32),
        None => {
            sqlx::query("INSERT INTO street_pay_sync (ufvk, next_height) VALUES ($1, $2)")
                .bind(ufvk)
                .bind(birthday as i32)
                .execute(&ctx.db.pool)
                .await?;
            Ok(birthday)
        }
    }
}

async fn scan(ctx: &Ctx, ufvk: &str, key: &mut Key, birthday: u32, safe: u32) -> Result<()> {
    let mut next = cursor(ctx, ufvk, birthday).await?;
    while next <= safe {
        let to = (next + CHUNK - 1).min(safe);
        let data = zscan_chain::compact::compact_range(&ctx.lwd, next, to).await?;
        key.clear_spends();
        let found = key.scan(&data).map_err(anyhow::Error::msg)?;

        // Payments in, by transaction (change from our own spends is not a payment).
        let mut txs: HashMap<String, (u32, u32)> = HashMap::new();
        for n in found.notes.iter().filter(|n| n.scope == "external") {
            txs.insert(n.txid.clone(), (n.height, n.time));
        }
        let mut db = ctx.db.pool.begin().await?;
        let mut paid = 0;
        for (txid, (height, time)) in txs {
            let raw = ctx
                .lwd
                .raw_tx(&txid)
                .await?
                .with_context(|| format!("tx {txid} not found"))?;
            let outputs = zscan_chain::parse::shielded_outputs(&raw.data)?;
            let json = serde_json::to_string(&outputs)?;
            for d in key.decrypt_tx(&json, None).map_err(anyhow::Error::msg)? {
                if d.kind != "received" {
                    continue;
                }
                let r = d.memo.as_deref().and_then(memo_ref);
                sqlx::query(
                    "INSERT INTO street_payments (txid, pool, idx, height, time, value, memo, ref)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING",
                )
                .bind(&txid)
                .bind(d.pool)
                .bind(d.index as i32)
                .bind(height as i32)
                .bind(when(time))
                .bind(d.value as i64)
                .bind(&d.memo)
                .bind(&r)
                .execute(&mut *db)
                .await?;
                paid += 1;
            }
        }
        next = to + 1;
        sqlx::query(
            "UPDATE street_pay_sync SET next_height = $2, updated_at = now() WHERE ufvk = $1",
        )
        .bind(ufvk)
        .bind(next as i32)
        .execute(&mut *db)
        .await?;
        db.commit().await?;
        if paid > 0 {
            info!(to, paid, "street pay: payments received");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keygen_gives_a_key_the_scanner_takes() {
        let (ua, ufvk) = derive(&[7u8; 64], NetworkType::Main).unwrap();
        assert!(ua.starts_with("u1"));
        let key = Key::parse(&ufvk).unwrap();
        assert!(key.is_full());
        assert_eq!(key.network(), "mainnet");
    }

    #[test]
    fn refs_in_memos() {
        assert_eq!(memo_ref("ZS1:ABCD2345\nhello").as_deref(), Some("ABCD2345"));
        assert_eq!(memo_ref("tip ZS1:QRSTUV77 thanks").as_deref(), Some("QRSTUV77"));
        assert_eq!(memo_ref("ZS1:abcd2345"), None);
        assert_eq!(memo_ref("ZS1:ABC"), None);
        assert_eq!(memo_ref("ZS1:ABCD1890"), None);
        assert_eq!(memo_ref("no reference"), None);
    }
}
