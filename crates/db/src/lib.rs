//! Postgres storage and block building shared by the indexer and the API.

pub mod build;

use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, Transaction};
use zscan_chain::{
    classify::PoolDeltas,
    resolve::{OutPoint, PrevOut},
    ParsedTx, TxSummary,
};

pub use sqlx;

#[derive(Clone)]
pub struct Db {
    pub pool: PgPool,
}

pub struct IndexedTx {
    pub block_index: u32,
    pub tx: ParsedTx,
    pub summary: TxSummary,
    pub prevouts: Vec<Option<PrevOut>>,
}

pub struct IndexedBlock {
    pub height: u32,
    pub hash: String,
    pub prev_hash: String,
    pub time: DateTime<Utc>,
    pub size: usize,
    pub fees: Option<i64>,
    pub miner: Option<String>,
    pub miner_tag: Option<String>,
    pub miner_pool: Option<&'static str>,
    pub coinbase_value: i64,
    /// Protocol economics; `None` where the subsidy rules aren't implemented yet (post-NU7 until
    /// `zebra-chain` supports it). The economics task fills them in later.
    pub subsidy: Option<i64>,
    pub funding_paid: Option<i64>,
    pub deferred: Option<i64>,
    /// `None` when some input values are unknown (fees can't be computed).
    pub burned: Option<i64>,
    pub sapling_tree_size: Option<u32>,
    pub orchard_tree_size: Option<u32>,
    pub ironwood_tree_size: Option<u32>,
    pub deltas: PoolDeltas,
    pub txs: Vec<IndexedTx>,
}

#[derive(Debug, Clone, Copy)]
pub struct SyncState {
    pub low: Option<u32>,
    pub high: Option<u32>,
}

pub struct PoolSnapshot {
    pub height: u32,
    pub transparent: i64,
    pub sprout: i64,
    pub sapling: i64,
    pub orchard: i64,
    pub ironwood: i64,
    pub lockbox: i64,
    pub supply: i64,
}

pub struct MempoolRow {
    pub txid: String,
    pub size: i32,
    pub kind: String,
    pub fee: Option<i64>,
    pub transparent_out: i64,
    pub n_actions: i32,
}

impl Db {
    pub async fn connect(url: &str, max_connections: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    pub async fn sync_state(&self) -> Result<SyncState> {
        let (low, high): (Option<i32>, Option<i32>) =
            sqlx::query_as("SELECT low, high FROM sync_state")
                .fetch_one(&self.pool)
                .await?;
        Ok(SyncState {
            low: low.map(|v| v as u32),
            high: high.map(|v| v as u32),
        })
    }

    pub async fn set_low(&self, low: u32) -> Result<()> {
        sqlx::query("UPDATE sync_state SET low = $1")
            .bind(low as i32)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn set_high(&self, high: u32) -> Result<()> {
        sqlx::query("UPDATE sync_state SET high = $1")
            .bind(high as i32)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn init_sync(&self, tip: u32) -> Result<()> {
        sqlx::query("UPDATE sync_state SET low = $1, high = $2 WHERE low IS NULL")
            .bind(tip as i32 + 1)
            .bind(tip as i32)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn block_hash(&self, height: u32) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT hash FROM blocks WHERE height = $1")
                .bind(height as i32)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    /// Deletes blocks in `[from, to]` (cascades to transactions).
    pub async fn delete_blocks(&self, from: u32, to: u32) -> Result<()> {
        sqlx::query("DELETE FROM blocks WHERE height BETWEEN $1 AND $2")
            .bind(from as i32)
            .bind(to as i32)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Looks up transparent outputs already in the index.
    pub async fn known_outputs(
        &self,
        outpoints: &[OutPoint],
    ) -> Result<HashMap<OutPoint, PrevOut>> {
        if outpoints.is_empty() {
            return Ok(HashMap::new());
        }
        let txids: Vec<&str> = outpoints.iter().map(|o| o.0.as_str()).collect();
        let ns: Vec<i32> = outpoints.iter().map(|o| o.1 as i32).collect();
        let rows: Vec<(String, i32, i64, Option<String>)> = sqlx::query_as(
            "SELECT o.txid, o.n, o.value, o.address
             FROM tx_outputs o
             JOIN UNNEST($1::text[], $2::int[]) AS q(txid, n) ON o.txid = q.txid AND o.n = q.n",
        )
        .bind(&txids)
        .bind(&ns)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(t, n, value, address)| ((t, n as u32), PrevOut { value, address }))
            .collect())
    }

    pub async fn insert_block(&self, b: &IndexedBlock) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        insert_block_tx(&mut tx, b).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn insert_pool_snapshot(&self, s: &PoolSnapshot) -> Result<()> {
        sqlx::query(
            "INSERT INTO pool_snapshots (height, transparent, sprout, sapling, orchard, ironwood, lockbox, supply)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (height) DO UPDATE SET transparent = EXCLUDED.transparent, sprout = EXCLUDED.sprout,
               sapling = EXCLUDED.sapling, orchard = EXCLUDED.orchard, ironwood = EXCLUDED.ironwood,
               lockbox = EXCLUDED.lockbox, supply = EXCLUDED.supply",
        )
        .bind(s.height as i32)
        .bind(s.transparent)
        .bind(s.sprout)
        .bind(s.sapling)
        .bind(s.orchard)
        .bind(s.ironwood)
        .bind(s.lockbox)
        .bind(s.supply)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_network_snapshot(
        &self,
        height: u32,
        difficulty: f64,
        solps: Option<f64>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO network_snapshots (height, difficulty, solps) VALUES ($1, $2, $3)",
        )
        .bind(height as i32)
        .bind(difficulty)
        .bind(solps)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Returns `true` if the transaction was new.
    pub async fn upsert_mempool(&self, r: &MempoolRow) -> Result<bool> {
        let res = sqlx::query(
            "INSERT INTO mempool (txid, size, kind, fee, transparent_out, n_actions)
             VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (txid) DO NOTHING",
        )
        .bind(&r.txid)
        .bind(r.size)
        .bind(&r.kind)
        .bind(r.fee)
        .bind(r.transparent_out)
        .bind(r.n_actions)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    /// Removes mempool entries that were mined, plus stale ones.
    pub async fn prune_mempool(&self) -> Result<()> {
        sqlx::query(
            "DELETE FROM mempool m WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.txid = m.txid)
               OR m.seen_at < now() - interval '6 hours'",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Blocks indexed before miner tagging existed: (height, coinbase txid).
    pub async fn untagged_blocks(&self, limit: i64) -> Result<Vec<(i32, String)>> {
        Ok(sqlx::query_as(
            "SELECT b.height, t.txid FROM blocks b JOIN transactions t ON t.height = b.height AND t.block_index = 0
             WHERE b.miner_tag IS NULL AND b.miner_pool IS NULL ORDER BY b.height DESC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?)
    }

    /// `tag` may be empty when the coinbase carries no text, which marks the block as done.
    pub async fn set_miner_tag(&self, height: i32, tag: &str, pool: Option<&str>) -> Result<()> {
        sqlx::query("UPDATE blocks SET miner_tag = $2, miner_pool = $3 WHERE height = $1")
            .bind(height)
            .bind(tag)
            .bind(pool)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn record_reorg(&self, height: u32, old_hash: &str, new_hash: &str) -> Result<()> {
        sqlx::query("INSERT INTO reorgs (height, old_hash, new_hash) VALUES ($1, $2, $3)")
            .bind(height as i32)
            .bind(old_hash)
            .bind(new_hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Blocks indexed before economics columns existed, with their coinbase outputs and fees.
    /// Blocks below `below` whose economics are still unknown.
    pub async fn blocks_missing_economics(
        &self,
        limit: i64,
        below: i32,
    ) -> Result<Vec<(i32, Option<i64>, i64, Vec<String>, Vec<i64>)>> {
        Ok(sqlx::query_as(
            "SELECT b.height, b.fees, b.coinbase_value,
                    COALESCE(array_agg(o.address) FILTER (WHERE o.address IS NOT NULL), '{}'),
                    COALESCE(array_agg(o.value) FILTER (WHERE o.address IS NOT NULL), '{}')
             FROM blocks b
             JOIN transactions t ON t.height = b.height AND t.block_index = 0
             LEFT JOIN tx_outputs o ON o.txid = t.txid
             WHERE b.subsidy IS NULL AND b.height < $2
             GROUP BY b.height ORDER BY b.height DESC LIMIT $1",
        )
        .bind(limit)
        .bind(below)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn set_economics(
        &self,
        height: i32,
        subsidy: i64,
        funding_paid: i64,
        deferred: i64,
        burned: Option<i64>,
    ) -> Result<()> {
        sqlx::query("UPDATE blocks SET subsidy = $2, funding_paid = $3, deferred = $4, burned = $5 WHERE height = $1")
            .bind(height)
            .bind(subsidy)
            .bind(funding_paid)
            .bind(deferred)
            .bind(burned)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Runs pending supply audits between consecutive node snapshots whose whole block range
    /// is indexed. Returns the number of new checks.
    pub async fn run_audits(&self) -> Result<u64> {
        let r = sqlx::query(
            "WITH s AS (
                SELECT height, transparent, sprout, sapling, orchard, ironwood, lockbox, supply,
                       lag(height) OVER w AS ph, lag(transparent) OVER w AS pt, lag(sprout) OVER w AS psp,
                       lag(sapling) OVER w AS psa, lag(orchard) OVER w AS por, lag(ironwood) OVER w AS piw,
                       lag(lockbox) OVER w AS plb, lag(supply) OVER w AS psu
                FROM pool_snapshots WINDOW w AS (ORDER BY height)
             ), todo AS (
                SELECT s.* FROM s, sync_state st
                WHERE s.ph IS NOT NULL AND s.ph >= st.low AND s.height <= st.high
                  AND NOT EXISTS (SELECT 1 FROM audit_checks a WHERE a.height = s.height)
             ), d AS (
                SELECT t.height, t.ph,
                  (t.transparent - t.pt) - COALESCE(sum(b.d_transparent), 0) AS dt,
                  (t.sprout - t.psp) - COALESCE(sum(b.d_sprout), 0) AS dsp,
                  (t.sapling - t.psa) - COALESCE(sum(b.d_sapling), 0) AS dsa,
                  (t.orchard - t.por) - COALESCE(sum(b.d_orchard), 0) AS dor,
                  (t.ironwood - t.piw) - COALESCE(sum(b.d_ironwood), 0) AS diw,
                  (t.lockbox - t.plb) - COALESCE(sum(b.deferred), 0) AS dlb,
                  (t.supply - t.psu) - COALESCE(sum(b.subsidy - COALESCE(b.burned, 0)), 0) AS dsu,
                  count(b.height) AS n, bool_and(b.subsidy IS NOT NULL) AS econ
                FROM todo t LEFT JOIN blocks b ON b.height > t.ph AND b.height <= t.height
                GROUP BY t.height, t.ph, t.transparent, t.pt, t.sprout, t.psp, t.sapling, t.psa,
                         t.orchard, t.por, t.ironwood, t.piw, t.lockbox, t.plb, t.supply, t.psu
             )
             INSERT INTO audit_checks (height, prev_height, diff_transparent, diff_sprout, diff_sapling,
                diff_orchard, diff_ironwood, diff_lockbox, diff_supply, ok)
             SELECT height, ph, dt, dsp, dsa, dor, diw, dlb, dsu,
                    dt = 0 AND dsp = 0 AND dsa = 0 AND dor = 0 AND diw = 0 AND dlb = 0 AND dsu = 0
             FROM d WHERE n = height - ph AND econ",
        )
        .execute(&self.pool)
        .await?;
        Ok(r.rows_affected())
    }

    /// Recomputes the `daily_blocks` rollup for every day from `since` on (all days when `None`),
    /// dropping days that no longer have blocks (after a reorg).
    pub async fn refresh_daily(&self, since: Option<DateTime<Utc>>) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM daily_blocks WHERE $1::timestamptz IS NULL OR day >= date_trunc('day', $1)")
            .bind(since)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO daily_blocks
             SELECT date_trunc('day', time), count(*), count(subsidy),
                    COALESCE(sum(subsidy), 0), COALESCE(sum(funding_paid) FILTER (WHERE subsidy IS NOT NULL), 0),
                    COALESCE(sum(deferred) FILTER (WHERE subsidy IS NOT NULL), 0),
                    COALESCE(sum(fees) FILTER (WHERE subsidy IS NOT NULL), 0),
                    COALESCE(sum(burned) FILTER (WHERE subsidy IS NOT NULL), 0),
                    max(sapling_tree_size), max(orchard_tree_size), max(ironwood_tree_size),
                    sum(d_transparent), sum(d_sprout), sum(d_sapling), sum(d_orchard), sum(d_ironwood)
             FROM blocks
             WHERE $1::timestamptz IS NULL OR time >= date_trunc('day', $1)
             GROUP BY 1",
        )
        .bind(since)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn notify(&self, channel: &str, payload: &str) -> Result<()> {
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel)
            .bind(payload)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

async fn insert_block_tx(tx: &mut Transaction<'_, Postgres>, b: &IndexedBlock) -> Result<()> {
    sqlx::query(
        "INSERT INTO blocks (height, hash, prev_hash, time, tx_count, size, fees, miner, coinbase_value,
            sapling_tree_size, orchard_tree_size, ironwood_tree_size,
            d_transparent, d_sprout, d_sapling, d_orchard, d_ironwood, miner_tag, miner_pool,
            subsidy, funding_paid, deferred, burned)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)",
    )
    .bind(b.height as i32)
    .bind(&b.hash)
    .bind(&b.prev_hash)
    .bind(b.time)
    .bind(b.txs.len() as i32)
    .bind(b.size as i32)
    .bind(b.fees)
    .bind(&b.miner)
    .bind(b.coinbase_value)
    .bind(b.sapling_tree_size.map(|v| v as i32))
    .bind(b.orchard_tree_size.map(|v| v as i32))
    .bind(b.ironwood_tree_size.map(|v| v as i32))
    .bind(b.deltas.transparent)
    .bind(b.deltas.sprout)
    .bind(b.deltas.sapling)
    .bind(b.deltas.orchard)
    .bind(b.deltas.ironwood)
    .bind(&b.miner_tag)
    .bind(b.miner_pool)
    .bind(b.subsidy)
    .bind(b.funding_paid)
    .bind(b.deferred)
    .bind(b.burned)
    .execute(&mut **tx)
    .await?;

    if b.txs.is_empty() {
        return Ok(());
    }

    // Transactions, bulk-inserted with UNNEST.
    let mut c = TxColumns::default();
    for t in &b.txs {
        c.push(t);
    }
    sqlx::query(
        "INSERT INTO transactions (txid, height, block_index, time, version, size, kind, fee,
            transparent_in, transparent_out, sprout_vb, sapling_vb, orchard_vb, ironwood_vb,
            n_tin, n_tout, n_joinsplits, n_sapling_spends, n_sapling_outputs, n_orchard_actions, n_ironwood_actions)
         SELECT t.txid, $2, t.block_index, $3, t.version, t.size, t.kind, t.fee,
            t.tin, t.tout, t.sprout, t.sapling, t.orchard, t.ironwood,
            t.n_tin, t.n_tout, t.n_js, t.n_ss, t.n_so, t.n_oa, t.n_ia
         FROM UNNEST($1::text[], $4::int[], $5::int[], $6::int[], $7::text[], $8::bigint[],
            $9::bigint[], $10::bigint[], $11::bigint[], $12::bigint[], $13::bigint[], $14::bigint[],
            $15::int[], $16::int[], $17::int[], $18::int[], $19::int[], $20::int[], $21::int[])
         AS t(txid, block_index, version, size, kind, fee, tin, tout, sprout, sapling, orchard, ironwood,
            n_tin, n_tout, n_js, n_ss, n_so, n_oa, n_ia)",
    )
    .bind(&c.txid)
    .bind(b.height as i32)
    .bind(b.time)
    .bind(&c.block_index)
    .bind(&c.version)
    .bind(&c.size)
    .bind(&c.kind)
    .bind(&c.fee)
    .bind(&c.tin)
    .bind(&c.tout)
    .bind(&c.sprout)
    .bind(&c.sapling)
    .bind(&c.orchard)
    .bind(&c.ironwood)
    .bind(&c.n_tin)
    .bind(&c.n_tout)
    .bind(&c.n_js)
    .bind(&c.n_ss)
    .bind(&c.n_so)
    .bind(&c.n_oa)
    .bind(&c.n_ia)
    .execute(&mut **tx)
    .await?;

    // Transparent outputs.
    let (mut o_txid, mut o_n, mut o_value, mut o_addr) = (vec![], vec![], vec![], vec![]);
    // Transparent inputs.
    let (mut i_txid, mut i_n, mut i_prev, mut i_prev_n, mut i_value, mut i_addr) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    for t in &b.txs {
        for o in &t.tx.outputs {
            o_txid.push(t.tx.txid.as_str());
            o_n.push(o.n as i32);
            o_value.push(o.value);
            o_addr.push(o.address.as_deref());
        }
        for (n, (inp, prev)) in t.tx.inputs.iter().zip(&t.prevouts).enumerate() {
            let (Some(pt), Some(pn)) = (inp.prev_txid.as_deref(), inp.prev_n) else {
                continue;
            };
            i_txid.push(t.tx.txid.as_str());
            i_n.push(n as i32);
            i_prev.push(pt);
            i_prev_n.push(pn as i32);
            i_value.push(prev.as_ref().map(|p| p.value));
            i_addr.push(prev.as_ref().and_then(|p| p.address.as_deref()));
        }
    }
    if !o_txid.is_empty() {
        sqlx::query(
            "INSERT INTO tx_outputs (txid, n, value, address)
             SELECT * FROM UNNEST($1::text[], $2::int[], $3::bigint[], $4::text[])",
        )
        .bind(&o_txid)
        .bind(&o_n)
        .bind(&o_value)
        .bind(&o_addr)
        .execute(&mut **tx)
        .await?;
    }
    if !i_txid.is_empty() {
        sqlx::query(
            "INSERT INTO tx_inputs (txid, n, prev_txid, prev_n, value, address)
             SELECT * FROM UNNEST($1::text[], $2::int[], $3::text[], $4::int[], $5::bigint[], $6::text[])",
        )
        .bind(&i_txid)
        .bind(&i_n)
        .bind(&i_prev)
        .bind(&i_prev_n)
        .bind(&i_value)
        .bind(&i_addr)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

#[derive(Default)]
struct TxColumns {
    txid: Vec<String>,
    block_index: Vec<i32>,
    version: Vec<i32>,
    size: Vec<i32>,
    kind: Vec<&'static str>,
    fee: Vec<Option<i64>>,
    tin: Vec<i64>,
    tout: Vec<i64>,
    sprout: Vec<i64>,
    sapling: Vec<i64>,
    orchard: Vec<i64>,
    ironwood: Vec<i64>,
    n_tin: Vec<i32>,
    n_tout: Vec<i32>,
    n_js: Vec<i32>,
    n_ss: Vec<i32>,
    n_so: Vec<i32>,
    n_oa: Vec<i32>,
    n_ia: Vec<i32>,
}

impl TxColumns {
    fn push(&mut self, t: &IndexedTx) {
        let p = &t.tx;
        self.txid.push(p.txid.clone());
        self.block_index.push(t.block_index as i32);
        self.version.push(p.version as i32);
        self.size.push(p.size as i32);
        self.kind.push(t.summary.kind.as_str());
        self.fee.push(t.summary.fee);
        self.tin.push(t.summary.transparent_in);
        self.tout.push(t.summary.transparent_out);
        self.sprout.push(p.sprout_vb);
        self.sapling.push(p.sapling_vb);
        self.orchard.push(p.orchard_vb);
        self.ironwood.push(p.ironwood_vb);
        self.n_tin.push(p.inputs.len() as i32);
        self.n_tout.push(p.outputs.len() as i32);
        self.n_js.push(p.n_joinsplits as i32);
        self.n_ss.push(p.sapling_spends.len() as i32);
        self.n_so.push(p.sapling_outputs.len() as i32);
        self.n_oa.push(p.orchard_actions.len() as i32);
        self.n_ia.push(p.ironwood_actions.len() as i32);
    }
}
