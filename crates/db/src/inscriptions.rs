//! Loads and saves the inscription / ZRC-20 engine state.

use std::collections::HashSet;

use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use zscan_chain::inscriptions::{
    media_kind, Balance, Committed, Engine, Held, Pending, Token, FIRST_HEIGHT,
};

use crate::Db;

fn ts(t: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(t, 0).single().unwrap_or_default()
}

fn num(s: &str) -> u128 {
    s.parse().unwrap_or(0)
}

impl Db {
    /// Next height the inscription scan will read.
    pub async fn inscription_cursor(&self) -> Result<u32> {
        let h: Option<i32> = sqlx::query_scalar("SELECT height FROM inscription_sync")
            .fetch_optional(&self.pool)
            .await?;
        Ok(h.map_or(FIRST_HEIGHT, |h| h as u32))
    }

    /// Rebuilds the engine from stored state.
    pub async fn load_inscription_engine(&self) -> Result<Engine> {
        let mut e = Engine::new();
        let max: Option<i64> = sqlx::query_scalar("SELECT max(number) FROM inscriptions")
            .fetch_one(&self.pool)
            .await?;
        e.next_number = max.unwrap_or(0) + 1;

        let held: Vec<(String, Option<String>, String, i32)> = sqlx::query_as(
            "SELECT id, owner, location_txid, location_vout FROM inscriptions
             WHERE state = 'transparent' AND location_txid IS NOT NULL",
        )
        .fetch_all(&self.pool)
        .await?;
        for (id, owner, txid, vout) in held {
            e.locations
                .entry((txid, vout as u32))
                .or_default()
                .push(Held { id, owner });
        }

        let pending: Vec<(String, i32, serde_json::Value)> =
            sqlx::query_as("SELECT txid, vout, data FROM inscription_pending")
                .fetch_all(&self.pool)
                .await?;
        for (txid, vout, data) in pending {
            let p: Pending = serde_json::from_value(data)?;
            e.pending.insert((txid, vout as u32), p);
        }

        type TokenRow = (
            String,
            String,
            String,
            i16,
            String,
            String,
            String,
            i64,
            String,
            String,
            String,
            i32,
            DateTime<Utc>,
        );
        let tokens: Vec<TokenRow> = sqlx::query_as(
            "SELECT tick, max::text, lim::text, dec, minted::text, burned::text, shielded::text,
                    mints, deployer, deploy_inscription, deploy_txid, height, time
             FROM zrc20_tokens",
        )
        .fetch_all(&self.pool)
        .await?;
        for t in tokens {
            e.tokens.insert(
                t.0.clone(),
                Token {
                    tick: t.0,
                    max: num(&t.1),
                    lim: num(&t.2),
                    dec: t.3 as u8,
                    minted: num(&t.4),
                    burned: num(&t.5),
                    shielded: num(&t.6),
                    mints: t.7,
                    deployer: t.8,
                    deploy_inscription: t.9,
                    deploy_txid: t.10,
                    height: t.11 as u32,
                    time: t.12.timestamp(),
                },
            );
        }

        let balances: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT tick, address, available::text, transferable::text FROM zrc20_balances",
        )
        .fetch_all(&self.pool)
        .await?;
        for (tick, address, a, t) in balances {
            e.balances.insert(
                (tick, address),
                Balance {
                    available: num(&a),
                    transferable: num(&t),
                },
            );
        }

        let committed: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT inscription_id, tick, amount::text, sender FROM zrc20_committed",
        )
        .fetch_all(&self.pool)
        .await?;
        for (id, tick, amount, sender) in committed {
            e.committed.insert(
                id,
                Committed {
                    tick,
                    amount: num(&amount),
                    sender,
                },
            );
        }
        Ok(e)
    }

    /// Writes the engine's changes and moves the cursor to `next_height`, atomically. Content
    /// larger than `content_max` bytes isn't stored (the API rebuilds it from the chain).
    pub async fn flush_inscriptions(
        &self,
        e: &mut Engine,
        next_height: u32,
        content_max: usize,
    ) -> Result<()> {
        let out = std::mem::take(&mut e.out);
        let mut tx = self.pool.begin().await?;

        for i in &out.inscriptions {
            sqlx::query(
                "INSERT INTO inscriptions (id, number, genesis_height, txid, height, block_index,
                    time, content_type, media, content_len, content, sha256, reveal_txids, zrc20,
                    owner, location_txid, location_vout, state, last_height)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$5)",
            )
            .bind(&i.id)
            .bind(i.number)
            .bind(i.genesis_height as i32)
            .bind(&i.txid)
            .bind(i.height as i32)
            .bind(i.block_index as i32)
            .bind(ts(i.time))
            .bind(&i.content_type)
            .bind(media_kind(&i.content_type))
            .bind(i.content.len() as i32)
            .bind((i.content.len() <= content_max).then_some(&i.content))
            .bind(&i.sha256)
            .bind(&i.reveal_txids)
            .bind(&i.zrc20)
            .bind(&i.owner)
            .bind(i.location.as_ref().map(|l| &l.0))
            .bind(i.location.as_ref().map(|l| l.1 as i32))
            .bind(i.state.as_str())
            .execute(&mut *tx)
            .await?;
        }

        for ev in &out.events {
            sqlx::query(
                "INSERT INTO inscription_events (inscription_id, kind, txid, height, time,
                    from_addr, to_addr)
                 VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING",
            )
            .bind(&ev.inscription_id)
            .bind(ev.kind)
            .bind(&ev.txid)
            .bind(ev.height as i32)
            .bind(ts(ev.time))
            .bind(&ev.from)
            .bind(&ev.to)
            .execute(&mut *tx)
            .await?;
            if ev.kind != "inscribed" {
                sqlx::query(
                    "UPDATE inscriptions SET owner = $2, location_txid = $3, location_vout = $4,
                        state = $5, transfers = transfers + 1, last_height = $6
                     WHERE id = $1",
                )
                .bind(&ev.inscription_id)
                .bind(&ev.to)
                .bind(ev.location.as_ref().map(|l| &l.0))
                .bind(ev.location.as_ref().map(|l| l.1 as i32))
                .bind(ev.state.as_str())
                .bind(ev.height as i32)
                .execute(&mut *tx)
                .await?;
            }
        }

        for key in &out.pending {
            match e.pending.get(key) {
                Some(p) => {
                    sqlx::query(
                        "INSERT INTO inscription_pending (txid, vout, data) VALUES ($1, $2, $3)
                         ON CONFLICT (txid, vout) DO UPDATE SET data = EXCLUDED.data",
                    )
                    .bind(&key.0)
                    .bind(key.1 as i32)
                    .bind(serde_json::to_value(p)?)
                    .execute(&mut *tx)
                    .await?;
                }
                None => {
                    sqlx::query("DELETE FROM inscription_pending WHERE txid = $1 AND vout = $2")
                        .bind(&key.0)
                        .bind(key.1 as i32)
                        .execute(&mut *tx)
                        .await?;
                }
            }
        }

        for tick in &out.tokens {
            let Some(t) = e.tokens.get(tick) else {
                continue;
            };
            sqlx::query(
                "INSERT INTO zrc20_tokens (tick, max, lim, dec, minted, burned, shielded, mints,
                    deployer, deploy_inscription, deploy_txid, height, time)
                 VALUES ($1, $2::numeric, $3::numeric, $4, $5::numeric, $6::numeric, $7::numeric,
                    $8, $9, $10, $11, $12, $13)
                 ON CONFLICT (tick) DO UPDATE SET minted = EXCLUDED.minted,
                    burned = EXCLUDED.burned, shielded = EXCLUDED.shielded, mints = EXCLUDED.mints",
            )
            .bind(&t.tick)
            .bind(t.max.to_string())
            .bind(t.lim.to_string())
            .bind(t.dec as i16)
            .bind(t.minted.to_string())
            .bind(t.burned.to_string())
            .bind(t.shielded.to_string())
            .bind(t.mints)
            .bind(&t.deployer)
            .bind(&t.deploy_inscription)
            .bind(&t.deploy_txid)
            .bind(t.height as i32)
            .bind(ts(t.time))
            .execute(&mut *tx)
            .await?;
        }

        let mut ticks: HashSet<&String> = out.tokens.iter().collect();
        for key in &out.balances {
            ticks.insert(&key.0);
            match e
                .balances
                .get(key)
                .filter(|b| b.available + b.transferable > 0)
            {
                Some(b) => {
                    sqlx::query(
                        "INSERT INTO zrc20_balances (tick, address, available, transferable)
                         VALUES ($1, $2, $3::numeric, $4::numeric)
                         ON CONFLICT (tick, address) DO UPDATE
                         SET available = EXCLUDED.available, transferable = EXCLUDED.transferable",
                    )
                    .bind(&key.0)
                    .bind(&key.1)
                    .bind(b.available.to_string())
                    .bind(b.transferable.to_string())
                    .execute(&mut *tx)
                    .await?;
                }
                None => {
                    sqlx::query("DELETE FROM zrc20_balances WHERE tick = $1 AND address = $2")
                        .bind(&key.0)
                        .bind(&key.1)
                        .execute(&mut *tx)
                        .await?;
                }
            }
        }
        // Empty balances are dropped from memory too, so the engine stays small.
        e.balances.retain(|_, b| b.available + b.transferable > 0);

        for id in &out.committed {
            match e.committed.get(id) {
                Some(c) => {
                    sqlx::query(
                        "INSERT INTO zrc20_committed (inscription_id, tick, amount, sender)
                         VALUES ($1, $2, $3::numeric, $4) ON CONFLICT DO NOTHING",
                    )
                    .bind(id)
                    .bind(&c.tick)
                    .bind(c.amount.to_string())
                    .bind(&c.sender)
                    .execute(&mut *tx)
                    .await?;
                }
                None => {
                    sqlx::query("DELETE FROM zrc20_committed WHERE inscription_id = $1")
                        .bind(id)
                        .execute(&mut *tx)
                        .await?;
                }
            }
        }

        for z in &out.zrc_events {
            sqlx::query(
                "INSERT INTO zrc20_events (tick, op, inscription_id, txid, height, time,
                    from_addr, to_addr, amount, valid, reason)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::numeric,$10,$11)",
            )
            .bind(&z.tick)
            .bind(z.op)
            .bind(&z.inscription_id)
            .bind(&z.txid)
            .bind(z.height as i32)
            .bind(ts(z.time))
            .bind(&z.from)
            .bind(&z.to)
            .bind(z.amount.map(|a| a.to_string()))
            .bind(z.valid)
            .bind(&z.reason)
            .execute(&mut *tx)
            .await?;
        }

        if !ticks.is_empty() {
            let ticks: Vec<&String> = ticks.into_iter().collect();
            sqlx::query(
                "UPDATE zrc20_tokens t SET holders =
                    (SELECT count(*) FROM zrc20_balances b WHERE b.tick = t.tick)
                 WHERE t.tick = ANY($1)",
            )
            .bind(&ticks)
            .execute(&mut *tx)
            .await?;
        }

        sqlx::query(
            "INSERT INTO inscription_sync (id, height) VALUES (true, $1)
             ON CONFLICT (id) DO UPDATE SET height = EXCLUDED.height, updated_at = now()",
        )
        .bind(next_height as i32)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
