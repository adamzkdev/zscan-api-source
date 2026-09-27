//! Loads and saves the inscription engine state.

use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use zscan_chain::inscriptions::{Engine, Held, Kind, Pending, FIRST_HEIGHT};

use crate::Db;

fn ts(t: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(t, 0).single().unwrap_or_default()
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
        let next: Option<i64> = sqlx::query_scalar("SELECT next_number FROM inscription_sync")
            .fetch_optional(&self.pool)
            .await?;
        e.next_number = next.unwrap_or(1);

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

        e.collections = sqlx::query_scalar::<_, String>("SELECT name FROM collections")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .collect();
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
            let (collection, item, ipfs, ipfs_mime) = match &i.kind {
                Kind::Deploy { collection, .. } => (Some(collection), None, None, None),
                Kind::Item { collection, item } => (Some(collection), Some(item), None, None),
                Kind::Ipfs { cid, mime } => (None, None, Some(cid), Some(mime)),
                Kind::Media(_) | Kind::Token => (None, None, None, None),
            };
            sqlx::query(
                "INSERT INTO inscriptions (id, number, genesis_height, txid, height, block_index,
                    time, content_type, media, content_len, content, sha256, reveal_txids,
                    owner, location_txid, location_vout, state, last_height,
                    collection, item, ipfs, ipfs_mime)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$5,
                    $18,$19,$20,$21)",
            )
            .bind(&i.id)
            .bind(i.number)
            .bind(i.genesis_height as i32)
            .bind(&i.txid)
            .bind(i.height as i32)
            .bind(i.block_index as i32)
            .bind(ts(i.time))
            .bind(&i.content_type)
            .bind(i.kind.media())
            .bind(i.content.len() as i32)
            .bind((i.content.len() <= content_max).then_some(&i.content))
            .bind(&i.sha256)
            .bind(&i.reveal_txids)
            .bind(&i.owner)
            .bind(i.location.as_ref().map(|l| &l.0))
            .bind(i.location.as_ref().map(|l| l.1 as i32))
            .bind(i.state.as_str())
            .bind(collection)
            .bind(item)
            .bind(ipfs)
            .bind(ipfs_mime)
            .execute(&mut *tx)
            .await?;
        }

        for c in &out.collections {
            sqlx::query(
                "INSERT INTO collections (name, supply, meta, deploy_inscription, deployer,
                    height, time)
                 VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (name) DO NOTHING",
            )
            .bind(&c.name)
            .bind(c.supply)
            .bind(&c.meta)
            .bind(&c.deploy_inscription)
            .bind(&c.deployer)
            .bind(c.height as i32)
            .bind(ts(c.time))
            .execute(&mut *tx)
            .await?;
        }

        for ev in &out.events {
            sqlx::query(
                "INSERT INTO inscription_events (inscription_id, kind, txid, height, time,
                    from_addr, to_addr)
                 VALUES ($1,$2,$3,$4,$5,$6,$7)",
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

        sqlx::query(
            "INSERT INTO inscription_sync (id, height, next_number) VALUES (true, $1, $2)
             ON CONFLICT (id) DO UPDATE SET height = EXCLUDED.height,
                next_number = EXCLUDED.next_number, updated_at = now()",
        )
        .bind(next_height as i32)
        .bind(e.next_number)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
