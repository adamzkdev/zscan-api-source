//! Treasuries made public with a viewing key (`labels/treasuries.json`): each account is scanned
//! from its birthday height with ZScan's viewing-key scanner (the same code the browser runs at
//! proof.zscan.cash), `lag` blocks behind the tip. Received notes, their spends and the
//! payments sent to others are stored with their memos.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tracing::{info, warn};
use zscan_chain::treasuries::{self, Treasury};
use zscan_db::sqlx;
use zscan_wasm::native::Key;

use crate::Ctx;

const CHUNK: u32 = 500;

fn when(secs: u32) -> DateTime<Utc> {
    DateTime::from_timestamp(secs as i64, 0).unwrap_or_default()
}

pub async fn run(ctx: &Ctx, lag: u32) -> Result<()> {
    let mut keys: HashMap<&str, Key> = HashMap::new();
    for t in treasuries::list() {
        match Key::parse(&t.ufvk) {
            Ok(k) if k.is_full() => {
                keys.insert(&t.slug, k);
            }
            Ok(_) => warn!(
                slug = t.slug,
                "treasury key is not a full viewing key, skipped"
            ),
            Err(e) => warn!(slug = t.slug, "treasury key rejected: {e}"),
        }
    }
    if keys.is_empty() {
        return Ok(());
    }
    info!(treasuries = keys.len(), "treasury scan starting");
    loop {
        let (tip, _) = ctx.lwd.tip().await?;
        let safe = tip.saturating_sub(lag);
        for t in treasuries::list() {
            let Some(key) = keys.get_mut(t.slug.as_str()) else {
                continue;
            };
            scan(ctx, t, key, safe)
                .await
                .with_context(|| format!("treasury {}", t.slug))?;
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

/// Where the scan of `t` resumes; a changed key starts over from the birthday.
async fn cursor(ctx: &Ctx, t: &Treasury) -> Result<u32> {
    let row: Option<(String, i32)> =
        sqlx::query_as("SELECT ufvk, next_height FROM treasury_sync WHERE slug = $1")
            .bind(&t.slug)
            .fetch_optional(&ctx.db.pool)
            .await?;
    match row {
        Some((ufvk, next)) if ufvk == t.ufvk => Ok(next as u32),
        _ => {
            let mut tx = ctx.db.pool.begin().await?;
            for table in ["treasury_notes", "treasury_sends"] {
                sqlx::query(&format!("DELETE FROM {table} WHERE slug = $1"))
                    .bind(&t.slug)
                    .execute(&mut *tx)
                    .await?;
            }
            sqlx::query(
                "INSERT INTO treasury_sync (slug, ufvk, next_height) VALUES ($1, $2, $3)
                 ON CONFLICT (slug) DO UPDATE SET ufvk = $2, next_height = $3, updated_at = now()",
            )
            .bind(&t.slug)
            .bind(&t.ufvk)
            .bind(t.birthday_height as i32)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(t.birthday_height)
        }
    }
}

async fn scan(ctx: &Ctx, t: &Treasury, key: &mut Key, safe: u32) -> Result<()> {
    let mut next = cursor(ctx, t).await?;
    while next <= safe {
        let to = (next + CHUNK - 1).min(safe);
        let data = zscan_chain::compact::compact_range(&ctx.lwd, next, to).await?;
        key.clear_spends();
        let found = key.scan(&data).map_err(anyhow::Error::msg)?;

        let mut db = ctx.db.pool.begin().await?;
        let mut decrypt: HashMap<String, (u32, u32)> = HashMap::new();
        for n in &found.notes {
            let Some(nf) = &n.nf else { continue };
            sqlx::query(
                "INSERT INTO treasury_notes (slug, txid, pool, idx, height, time, value, scope, nf)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT DO NOTHING",
            )
            .bind(&t.slug)
            .bind(&n.txid)
            .bind(n.pool)
            .bind(n.output_index as i32)
            .bind(n.height as i32)
            .bind(when(n.time))
            .bind(n.value as i64)
            .bind(n.scope)
            .bind(nf)
            .execute(&mut *db)
            .await?;
            decrypt.insert(n.txid.clone(), (n.height, n.time));
        }

        // Spends of any unspent note, including the ones just found.
        let unspent: Vec<String> = sqlx::query_scalar(
            "SELECT nf FROM treasury_notes WHERE slug = $1 AND spent_txid IS NULL",
        )
        .bind(&t.slug)
        .fetch_all(&mut *db)
        .await?;
        for s in key.spent_by(unspent) {
            sqlx::query(
                "UPDATE treasury_notes SET spent_txid = $3, spent_height = $4, spent_time = $5
                 WHERE slug = $1 AND nf = $2",
            )
            .bind(&t.slug)
            .bind(&s.nf)
            .bind(&s.txid)
            .bind(s.height as i32)
            .bind(when(s.time))
            .execute(&mut *db)
            .await?;
            decrypt.insert(s.txid.clone(), (s.height, s.time));
        }

        // Memos of received notes, and payments to others, from the full transactions.
        let mut seen = HashSet::new();
        for (txid, (height, time)) in decrypt {
            if !seen.insert(txid.clone()) {
                continue;
            }
            let raw = ctx
                .lwd
                .raw_tx(&txid)
                .await?
                .with_context(|| format!("tx {txid} not found"))?;
            let outputs = zscan_chain::parse::shielded_outputs(&raw.data)?;
            let json = serde_json::to_string(&outputs)?;
            for d in key.decrypt_tx(&json, None).map_err(anyhow::Error::msg)? {
                if d.kind == "sent" {
                    sqlx::query(
                        "INSERT INTO treasury_sends (slug, txid, pool, idx, height, time, value, recipient, memo)
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT DO NOTHING",
                    )
                    .bind(&t.slug)
                    .bind(&txid)
                    .bind(d.pool)
                    .bind(d.index as i32)
                    .bind(height as i32)
                    .bind(when(time))
                    .bind(d.value as i64)
                    .bind(&d.to)
                    .bind(&d.memo)
                    .execute(&mut *db)
                    .await?;
                } else if d.memo.is_some() {
                    sqlx::query(
                        "UPDATE treasury_notes SET memo = $5
                         WHERE slug = $1 AND txid = $2 AND pool = $3 AND idx = $4",
                    )
                    .bind(&t.slug)
                    .bind(&txid)
                    .bind(d.pool)
                    .bind(d.index as i32)
                    .bind(&d.memo)
                    .execute(&mut *db)
                    .await?;
                }
            }
        }

        next = to + 1;
        sqlx::query(
            "UPDATE treasury_sync SET next_height = $2, updated_at = now() WHERE slug = $1",
        )
        .bind(&t.slug)
        .bind(next as i32)
        .execute(&mut *db)
        .await?;
        db.commit().await?;
        if !found.notes.is_empty() {
            info!(
                slug = t.slug,
                to,
                notes = found.notes.len(),
                "treasury scan"
            );
        }
    }
    Ok(())
}
