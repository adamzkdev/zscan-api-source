//! ZScan indexer: follows the chain tip, backfills history down to a start height,
//! records value-pool snapshots and tracks the mempool.

use std::time::Duration;

use anyhow::{Context, Result};
use futures::{future::BoxFuture, FutureExt, TryStreamExt};
use serde_json::json;
use tracing::{error, info, warn};
use zscan_chain::{consts::nu6_3_height, summarize, Lwd, Resolver, Rpc};
use zscan_db::{build::build_block, Db, MempoolRow, PoolSnapshot};

/// Public lightwalletd servers (zec.rocks, Zebra-backed).
const DEFAULT_LWD: &str = "https://zec.rocks:443,https://na.zec.rocks:443,https://eu.zec.rocks:443";

pub struct Ctx {
    pub db: Db,
    pub lwd: Lwd,
    pub rpc: Rpc,
    pub resolver: Resolver,
}

struct Config {
    database_url: String,
    lwd_url: String,
    lwd_per_second: f64,
    rpc_url: String,
    rpc_per_minute: u32,
    /// Absolute height, or `tip-N` to start N blocks below the tip.
    start_height: String,
    backfill_chunk: u32,
    backfill_concurrency: usize,
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

impl Config {
    fn from_env() -> Result<Self> {
        Ok(Self {
            database_url: std::env::var("DATABASE_URL").context("DATABASE_URL is required")?,
            lwd_url: env_or("LWD_URL", DEFAULT_LWD),
            lwd_per_second: env_or("LWD_RATE_PER_SEC", "50").parse()?,
            rpc_url: env_or("RPC_URL", "https://zcash-mainnet-zebrad.gateway.tatum.io"),
            rpc_per_minute: env_or("RPC_RATE_PER_MIN", "4").parse()?,
            start_height: env_or("START_HEIGHT", &nu6_3_height().to_string()),
            backfill_chunk: env_or("BACKFILL_CHUNK", "200").parse()?,
            backfill_concurrency: env_or("BACKFILL_CONCURRENCY", "4").parse()?,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,zscan_indexer=info".into()),
        )
        .init();

    let cfg = Config::from_env()?;
    let db = Db::connect(&cfg.database_url, 16).await?;
    db.migrate().await?;
    let lwd = Lwd::connect_with(&cfg.lwd_url, cfg.lwd_per_second)?;
    let info = lwd.info().await.context("lightwalletd unreachable")?;
    info!(server = %info.zcashd_subversion, height = info.block_height, "connected to lightwalletd");

    let ctx: &'static Ctx = Box::leak(Box::new(Ctx {
        rpc: Rpc::new(&cfg.rpc_url, cfg.rpc_per_minute),
        resolver: Resolver::new(lwd.clone(), 200_000),
        db,
        lwd,
    }));

    let (tip, _) = ctx.lwd.tip().await?;
    ctx.db.init_sync(tip).await?;
    let start_height: u32 = match cfg.start_height.strip_prefix("tip-") {
        Some(back) => tip.saturating_sub(back.parse()?),
        None => cfg.start_height.parse()?,
    };
    zscan_db::sqlx::query("UPDATE sync_state SET target = $1")
        .bind(start_height as i32)
        .execute(&ctx.db.pool)
        .await?;
    info!(
        network = if zscan_chain::is_testnet() {
            "testnet"
        } else {
            "mainnet"
        },
        start_height, tip, "indexer starting"
    );

    let follow = tokio::spawn(supervise("follow", move || follow(ctx).boxed()));
    let backfill = tokio::spawn(supervise("backfill", {
        let (start, chunk, conc) = (start_height, cfg.backfill_chunk, cfg.backfill_concurrency);
        move || backfill(ctx, start, chunk, conc).boxed()
    }));
    let snapshots = tokio::spawn(supervise("snapshots", move || snapshots(ctx).boxed()));
    let mempool = tokio::spawn(supervise("mempool", move || mempool(ctx).boxed()));
    let retag = tokio::spawn(supervise("retag", move || retag(ctx).boxed()));
    let economics = tokio::spawn(supervise("economics", move || economics(ctx).boxed()));
    let audit = tokio::spawn(supervise("audit", move || audit(ctx).boxed()));
    let daily = tokio::spawn(supervise("daily", move || daily(ctx).boxed()));

    let _ = tokio::join!(follow, backfill, snapshots, mempool, retag, economics, audit, daily);
    Ok(())
}

/// Restarts a task after errors, with a short backoff.
async fn supervise<F>(name: &'static str, f: F)
where
    F: Fn() -> BoxFuture<'static, Result<()>>,
{
    loop {
        match f().await {
            Ok(()) => return,
            Err(e) => {
                error!(task = name, "error: {e:#}");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

/// Indexes new blocks at the tip, rolling back on reorgs.
async fn follow(ctx: &Ctx) -> Result<()> {
    loop {
        let (tip, _) = ctx.lwd.tip().await?;
        let mut high = ctx.db.sync_state().await?.high.unwrap_or(tip);
        while high < tip {
            let h = high + 1;
            let Some(cb) = ctx.lwd.block(h).await? else {
                break;
            };
            let prev = zscan_chain::hash::to_display(&cb.prev_hash);
            if let Some(ours) = ctx.db.block_hash(high).await? {
                if ours != prev {
                    warn!(height = high, "reorg detected, rolling back one block");
                    ctx.db.record_reorg(high, &ours, &prev).await?;
                    ctx.db.delete_blocks(high, high).await?;
                    high -= 1;
                    ctx.db.set_high(high).await?;
                    continue;
                }
            }
            let b = build_block(&ctx.db, &ctx.lwd, &ctx.resolver, cb).await?;
            let n = b.txs.len();
            ctx.db.insert_block(&b).await?;
            ctx.db.set_high(h).await?;
            ctx.db
                .notify(
                    "zscan_block",
                    &json!({ "height": h, "hash": b.hash, "tx_count": n }).to_string(),
                )
                .await?;
            info!(height = h, txs = n, "indexed tip block");
            high = h;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Indexes history downwards from the first indexed block to `start`.
async fn backfill(ctx: &Ctx, start: u32, chunk: u32, concurrency: usize) -> Result<()> {
    loop {
        let low = ctx
            .db
            .sync_state()
            .await?
            .low
            .context("sync not initialized")?;
        if low <= start {
            info!(low, "backfill complete");
            return Ok(());
        }
        let from = low.saturating_sub(chunk).max(start);
        let to = low - 1;
        // Clear any partial chunk left by a previous run.
        ctx.db.delete_blocks(from, to).await?;

        let t0 = std::time::Instant::now();
        let blocks = ctx.lwd.block_range(from, to).await?;
        let mut built = blocks
            .map_err(anyhow::Error::from)
            .map_ok(|cb| build_block(&ctx.db, &ctx.lwd, &ctx.resolver, cb))
            .try_buffered(concurrency);
        let mut txs = 0;
        while let Some(b) = built.try_next().await? {
            txs += b.txs.len();
            ctx.db.insert_block(&b).await?;
        }
        ctx.db.set_low(from).await?;
        info!(
            from,
            to,
            txs,
            secs = t0.elapsed().as_secs_f32(),
            remaining = from - start,
            "backfilled chunk"
        );
    }
}

/// Periodically stores authoritative pool balances and network stats from the node.
async fn snapshots(ctx: &Ctx) -> Result<()> {
    let mut last_height = 0;
    loop {
        let bi = ctx.rpc.blockchain_info().await?;
        if bi.blocks != last_height {
            let p = |id| bi.pool(id).unwrap_or(0);
            ctx.db
                .insert_pool_snapshot(&PoolSnapshot {
                    height: bi.blocks,
                    transparent: p("transparent"),
                    sprout: p("sprout"),
                    sapling: p("sapling"),
                    orchard: p("orchard"),
                    ironwood: p("ironwood"),
                    lockbox: p("lockbox"),
                    supply: bi
                        .chain_supply
                        .as_ref()
                        .map(|s| s.chain_value_zat)
                        .unwrap_or(0),
                })
                .await?;
            ctx.db
                .insert_network_snapshot(bi.blocks, bi.difficulty, None)
                .await?;
            info!(
                height = bi.blocks,
                ironwood = p("ironwood"),
                orchard = p("orchard"),
                "pool snapshot"
            );
            last_height = bi.blocks;
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

/// Fills protocol economics for blocks indexed before those columns existed.
async fn economics(ctx: &Ctx) -> Result<()> {
    loop {
        let batch = ctx.db.blocks_missing_economics(500).await?;
        if batch.is_empty() {
            info!("block economics complete");
            return Ok(());
        }
        for (height, fees, coinbase_value, addrs, values) in batch {
            let h = height as u32;
            let econ = zscan_chain::economics::block_economics(h, zscan_chain::network());
            let fs = zscan_chain::economics::funding_stream_addresses(h, zscan_chain::network());
            let paid = addrs
                .iter()
                .zip(&values)
                .filter(|(a, _)| fs.contains(*a))
                .map(|(_, v)| v)
                .sum();
            let burned = fees.map(|f| f + econ.subsidy - econ.deferred - coinbase_value);
            ctx.db
                .set_economics(height, econ.subsidy, paid, econ.deferred, burned)
                .await?;
        }
    }
}

/// Reconciles node snapshots against decoded block deltas; alerts on any mismatch.
async fn audit(ctx: &Ctx) -> Result<()> {
    loop {
        let n = ctx.db.run_audits().await?;
        if n > 0 {
            let bad: i64 =
                zscan_db::sqlx::query_scalar("SELECT count(*) FROM audit_checks WHERE NOT ok")
                    .fetch_one(&ctx.db.pool)
                    .await?;
            if bad > 0 {
                warn!(failed = bad, "supply audit mismatch");
            } else {
                info!(new = n, "supply audit passed");
            }
            ctx.db
                .notify(
                    "zscan_audit",
                    &json!({ "checks": n, "failed": bad }).to_string(),
                )
                .await?;
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

/// Keeps the `daily_blocks` rollup current: the last two days often, everything periodically
/// (backfill and the economics fill touch older days).
async fn daily(ctx: &Ctx) -> Result<()> {
    let mut tick = 0u32;
    loop {
        let since =
            (!tick.is_multiple_of(20)).then(|| chrono::Utc::now() - chrono::Duration::days(2));
        ctx.db.refresh_daily(since).await?;
        tick = tick.wrapping_add(1);
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

/// Fills mining-pool tags for blocks indexed before tagging existed.
async fn retag(ctx: &Ctx) -> Result<()> {
    // Re-derive pool names from stored tags so detection fixes apply to past blocks.
    let tags: Vec<String> =
        zscan_db::sqlx::query_scalar("SELECT DISTINCT miner_tag FROM blocks WHERE miner_tag <> ''")
            .fetch_all(&ctx.db.pool)
            .await?;
    for tag in tags {
        zscan_db::sqlx::query("UPDATE blocks SET miner_pool = $2 WHERE miner_tag = $1 AND miner_pool IS DISTINCT FROM $2")
            .bind(&tag)
            .bind(zscan_chain::miner::pool_name(&tag))
            .execute(&ctx.db.pool)
            .await?;
    }
    loop {
        let batch = ctx.db.untagged_blocks(100).await?;
        if batch.is_empty() {
            info!("miner tags complete");
            return Ok(());
        }
        for (height, txid) in batch {
            let tag = match ctx.lwd.raw_tx(&txid).await? {
                Some(raw) => zscan_chain::parse_tx(&raw.data)?
                    .inputs
                    .first()
                    .and_then(|i| i.coinbase_data.as_deref())
                    .and_then(zscan_chain::miner::coinbase_tag)
                    .unwrap_or_default(),
                None => String::new(),
            };
            let pool = zscan_chain::miner::pool_name(&tag);
            ctx.db.set_miner_tag(height, &tag, pool).await?;
        }
    }
}

/// Streams mempool transactions; each stream ends when a block is mined.
async fn mempool(ctx: &Ctx) -> Result<()> {
    loop {
        let mut s = ctx.lwd.mempool_stream().await?;
        while let Some(raw) = s.message().await? {
            let tx = match zscan_chain::parse_tx(&raw.data) {
                Ok(t) => t,
                Err(e) => {
                    warn!("mempool decode: {e:#}");
                    continue;
                }
            };
            let outpoints: Vec<_> = tx
                .inputs
                .iter()
                .filter_map(|i| Some((i.prev_txid.clone()?, i.prev_n?)))
                .collect();
            let known = ctx.db.known_outputs(&outpoints).await?;
            let prevouts = ctx.resolver.resolve(&tx.inputs, &known).await?;
            let values: Vec<_> = prevouts
                .iter()
                .map(|p| p.as_ref().map(|p| p.value))
                .collect();
            let summary = summarize(&tx, &values);
            let row = MempoolRow {
                txid: tx.txid.clone(),
                size: tx.size as i32,
                kind: summary.kind.as_str().to_string(),
                fee: summary.fee,
                transparent_out: summary.transparent_out,
                n_actions: (tx.orchard_actions.len()
                    + tx.ironwood_actions.len()
                    + tx.sapling_spends.len()
                    + tx.sapling_outputs.len()) as i32,
            };
            if !ctx.db.upsert_mempool(&row).await? {
                continue;
            }
            ctx.db
                .notify(
                    "zscan_mempool",
                    &json!({ "txid": row.txid, "kind": row.kind, "fee": row.fee, "size": row.size }).to_string(),
                )
                .await?;
        }
        ctx.db.prune_mempool().await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
