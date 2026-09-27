//! ZScan indexer: follows the chain tip, backfills history down to a start height,
//! records value-pool snapshots and tracks the mempool.

use std::time::Duration;

use anyhow::{Context, Result};
use futures::{future::BoxFuture, FutureExt, StreamExt, TryStreamExt};
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
    /// Keep transactions for this many days only (blocks are always kept). Unset keeps all.
    tx_retention_days: Option<u32>,
    /// Scan for inscriptions (`INSCRIPTIONS=off` disables it).
    inscriptions: bool,
    /// Blocks kept between the inscription scan and the tip, so reorgs don't reach it.
    inscriptions_lag: u32,
    /// Inscription content above this size (bytes) isn't stored; the API rebuilds it from the
    /// chain on request.
    inscription_content_max: usize,
    /// Pause the inscription scan while the database is larger than this (MB), so it can't
    /// fill a small volume. Unset = no limit.
    inscriptions_max_db_mb: Option<i64>,
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
            tx_retention_days: match std::env::var("TX_RETENTION_DAYS") {
                Ok(v) if !v.trim().is_empty() => Some(v.trim().parse()?),
                _ => None,
            },
            inscriptions: env_or("INSCRIPTIONS", "on") != "off",
            inscriptions_lag: env_or("INSCRIPTIONS_LAG", "10").parse()?,
            inscription_content_max: env_or("INSCRIPTION_CONTENT_MAX", "61200").parse()?,
            inscriptions_max_db_mb: match std::env::var("INSCRIPTIONS_MAX_DB_MB") {
                Ok(v) if !v.trim().is_empty() => Some(v.trim().parse()?),
                _ => None,
            },
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
    let labels = tokio::spawn(supervise("labels", move || labels(ctx).boxed()));
    let bridges = tokio::spawn(supervise("bridges", move || bridges(ctx).boxed()));
    let inscriptions = tokio::spawn(supervise("inscriptions", {
        let (on, lag, max, db_max) = (
            cfg.inscriptions,
            cfg.inscriptions_lag,
            cfg.inscription_content_max,
            cfg.inscriptions_max_db_mb,
        );
        move || inscriptions(ctx, on, lag, max, db_max).boxed()
    }));
    let nft_enrich = tokio::spawn(supervise("nft_enrich", {
        let on = cfg.inscriptions;
        move || nft_enrich(ctx, on).boxed()
    }));
    let retention = tokio::spawn(supervise("retention", {
        let days = cfg.tx_retention_days;
        move || retention(ctx, days).boxed()
    }));

    let _ = tokio::join!(
        follow,
        backfill,
        snapshots,
        mempool,
        retag,
        economics,
        audit,
        daily,
        labels,
        bridges,
        inscriptions,
        nft_enrich,
        retention
    );
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

/// Deletes transactions older than the retention window, every 10 minutes. Small disks (the
/// hosted demo has 500 MB) can then follow the tip indefinitely.
async fn retention(ctx: &Ctx, days: Option<u32>) -> Result<()> {
    let Some(days) = days else {
        return Ok(());
    };
    loop {
        let (low, deleted) = ctx.db.prune_transactions(days).await?;
        if deleted > 0 {
            info!(days, tx_low = low, deleted, "pruned old transactions");
        }
        tokio::time::sleep(Duration::from_secs(600)).await;
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
        // Past an NU7 height our consensus library doesn't implement, economics stay unknown.
        let below = zscan_chain::upgrades::nu7_height()
            .filter(|&a| !zscan_chain::upgrades::economics_known(a))
            .map_or(i32::MAX, |a| a as i32);
        let batch = ctx.db.blocks_missing_economics(500, below).await?;
        if batch.is_empty() {
            info!("block economics complete");
            return Ok(());
        }
        for (height, fees, coinbase_value, addrs, values) in batch {
            let h = height as u32;
            let Some(econ) = zscan_chain::economics::block_economics(h, zscan_chain::network())
            else {
                continue;
            };
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

/// Refreshes address labels from every source, their balances, and protocol API stats.
async fn labels(ctx: &Ctx) -> Result<()> {
    use zscan_chain::labels::{self, Label};
    let http = zscan_chain::http_client_builder()
        .timeout(Duration::from_secs(20))
        .user_agent("zscan/0.1")
        .build()?;
    loop {
        let mut all = labels::curated();

        // Consensus: funding-stream recipients at the tip (Zcash Community Grants).
        let (tip, _) = ctx.lwd.tip().await?;
        for address in zscan_chain::economics::funding_stream_addresses(tip, zscan_chain::network())
        {
            all.push(Label {
                address,
                name: "Zcash Community Grants".into(),
                category: "treasury".into(),
                protocol: Some("zcg".into()),
                source: "Funding-stream recipient in the consensus rules (ZIP 1016)".into(),
                origin: "consensus",
                active: true,
            });
        }

        // Coinbase: payout addresses of blocks whose coinbase carries a known pool signature.
        let miners: Vec<(String, String, i64)> = zscan_db::sqlx::query_as(
            "SELECT miner, mode() WITHIN GROUP (ORDER BY miner_pool), count(*)
             FROM blocks WHERE miner IS NOT NULL AND miner_pool IS NOT NULL GROUP BY miner",
        )
        .fetch_all(&ctx.db.pool)
        .await?;
        for (address, pool, blocks) in miners {
            if all.iter().any(|l| l.address == address) {
                continue;
            }
            all.push(Label {
                address,
                protocol: Some(labels::slug(&pool)),
                name: pool,
                category: "mining_pool".into(),
                source: format!("Coinbase signature on {blocks} indexed blocks"),
                origin: "coinbase",
                active: true,
            });
        }

        // Protocol APIs: Maya's ZEC vaults (mainnet only).
        if !zscan_chain::is_testnet() {
            match labels::maya_vaults(&http).await {
                Ok(vaults) => {
                    // Retiring vaults stay labeled: funds move from them to the new ones.
                    for v in vaults {
                        all.push(Label {
                            active: v.active,
                            address: v.address,
                            name: "Maya Protocol: Vault".into(),
                            category: "bridge".into(),
                            protocol: Some("maya".into()),
                            source: "Maya Asgard vault list (mayanode /mayachain/vaults/asgard)"
                                .into(),
                            origin: "api",
                        });
                    }
                }
                Err(e) => warn!("maya vaults: {e:#}"),
            }
            match labels::maya_stats(&http).await {
                Ok(stats) => ctx.db.set_protocol_stats("maya", &stats).await?,
                Err(e) => warn!("maya stats: {e:#}"),
            }
        }

        ctx.db.sync_labels(&all).await?;

        // Balances from the node, for every labeled address (retired ones too).
        let addresses: Vec<String> = zscan_db::sqlx::query_scalar("SELECT address FROM labels")
            .fetch_all(&ctx.db.pool)
            .await?;
        for address in addresses {
            match ctx.lwd.taddr_balance(&address).await {
                Ok(b) => ctx.db.set_label_balance(&address, b).await?,
                Err(e) => warn!(%address, "label balance: {e:#}"),
            }
        }
        info!(labels = all.len(), "labels refreshed");
        tokio::time::sleep(Duration::from_secs(600)).await;
    }
}

/// Cross-chain flows (mainnet only): Maya swaps touching ZEC, NEAR Intents mints and burns,
/// and daily bridge holdings. Kept for 90 days.
async fn bridges(ctx: &Ctx) -> Result<()> {
    use zscan_chain::{bridges, labels};
    const KEEP_DAYS: i64 = 90;
    if zscan_chain::is_testnet() {
        return Ok(());
    }
    let http = zscan_chain::http_client_builder()
        .timeout(Duration::from_secs(20))
        .user_agent("zscan/0.1")
        .build()?;
    loop {
        let cutoff = chrono::Utc::now().timestamp() - KEEP_DAYS * 86_400;

        // Maya: newest page first (refreshes pending swaps), then older pages until known
        // events or the retention window. Bounded so a first run backfills over a few cycles.
        let mut token: Option<String> = None;
        for page in 0..20 {
            let (events, next) = match bridges::maya_swaps(&http, token.as_deref()).await {
                Ok(v) => v,
                Err(e) => {
                    warn!("maya swaps: {e:#}");
                    break;
                }
            };
            let Some(oldest) = events.last().cloned() else {
                break;
            };
            let known = page > 0 && ctx.db.has_bridge_event("maya", &oldest.external_id).await?;
            ctx.db.upsert_bridge_events(&events).await?;
            if known || oldest.time < cutoff || next.is_none() {
                break;
            }
            token = next;
        }
        match labels::maya_stats(&http).await {
            Ok(stats) => {
                if let Some(depth) = stats["asset_depth"].as_i64() {
                    ctx.db.set_bridge_snapshot("maya", depth).await?;
                }
            }
            Err(e) => warn!("maya stats: {e:#}"),
        }

        // NEAR Intents: ZEC minted on NEAR is ZEC the bridge holds on Zcash.
        match bridges::near_zec_supply(&http).await {
            Ok(supply) => {
                ctx.db.set_bridge_snapshot("near-intents", supply).await?;
                ctx.db
                    .set_protocol_stats(
                        "near-intents",
                        &serde_json::json!({
                            "zec_on_near": supply,
                            "token": bridges::NEAR_ZEC_TOKEN,
                            "source": format!("ft_total_supply of {} (NEAR RPC)", bridges::NEAR_ZEC_TOKEN),
                        }),
                    )
                    .await?;
            }
            Err(e) => warn!("near supply: {e:#}"),
        }
        // NearBlocks' free tier is tight: one page per cycle, best effort.
        match bridges::near_recent(&http).await {
            Ok(events) => ctx.db.upsert_bridge_events(&events).await?,
            Err(e) => warn!("near events: {e:#}"),
        }

        let pruned = ctx.db.prune_bridge_events(KEEP_DAYS as i32).await?;
        info!(pruned, "bridge flows refreshed");
        tokio::time::sleep(Duration::from_secs(600)).await;
    }
}

/// Scans for inscriptions from the first envelope block, `lag` blocks behind the
/// tip. Only transactions with transparent inputs are fetched: nothing else can carry an
/// envelope or spend an inscription.
async fn inscriptions(
    ctx: &Ctx,
    on: bool,
    lag: u32,
    content_max: usize,
    max_db_mb: Option<i64>,
) -> Result<()> {
    const CHUNK: u32 = 500;
    if !on || zscan_chain::is_testnet() {
        return Ok(());
    }
    let mut engine = ctx.db.load_inscription_engine().await?;
    let mut next = ctx.db.inscription_cursor().await?;
    info!(
        next,
        inscriptions = engine.next_number - 1,
        "inscription scan starting"
    );
    loop {
        let (tip, _) = ctx.lwd.tip().await?;
        let safe = tip.saturating_sub(lag);
        if next > safe {
            tokio::time::sleep(Duration::from_secs(30)).await;
            continue;
        }
        if let Some(max) = max_db_mb {
            let mb: i64 = zscan_db::sqlx::query_scalar(
                "SELECT pg_database_size(current_database()) / 1048576",
            )
            .fetch_one(&ctx.db.pool)
            .await?;
            if mb > max {
                warn!(
                    mb,
                    max, "database above INSCRIPTIONS_MAX_DB_MB, inscription scan paused"
                );
                tokio::time::sleep(Duration::from_secs(600)).await;
                continue;
            }
        }
        let to = (next + CHUNK - 1).min(safe);
        let t0 = std::time::Instant::now();
        let before = engine.next_number;
        let mut blocks = ctx
            .lwd
            .block_range(next, to)
            .await?
            .map_err(anyhow::Error::from)
            .map_ok(|cb| transparent_txs(&ctx.lwd, cb))
            .try_buffered(4);
        let mut flushed = std::time::Instant::now();
        while let Some((height, time, txs)) = blocks.try_next().await? {
            for (index, tx) in &txs {
                engine.apply_tx(height, time, *index, tx);
            }
            next = height + 1;
            if !engine.out.is_empty() || flushed.elapsed() > Duration::from_secs(10) {
                ctx.db
                    .flush_inscriptions(&mut engine, next, content_max)
                    .await?;
                flushed = std::time::Instant::now();
            }
        }
        ctx.db
            .flush_inscriptions(&mut engine, next, content_max)
            .await?;
        info!(
            to = next - 1,
            new = engine.next_number - before,
            total = engine.next_number - 1,
            pending = engine.pending.len(),
            behind = safe.saturating_sub(next - 1),
            secs = t0.elapsed().as_secs_f32(),
            "inscription scan"
        );
    }
}

/// Enriches indexed NFTs from the chain: marks moves recorded before sale detection existed
/// that were marketplace sales, and assigns launchpad drops from each mint's commit
/// transaction.
async fn nft_enrich(ctx: &Ctx, on: bool) -> Result<()> {
    if !on || zscan_chain::is_testnet() {
        return Ok(());
    }
    let fetch = |txid: String| async move {
        let raw = ctx
            .lwd
            .raw_tx(&txid)
            .await?
            .with_context(|| format!("tx {txid} not found"))?;
        zscan_chain::parse_tx(&raw.data).with_context(|| format!("decode {txid}"))
    };
    loop {
        let mut busy = false;

        if let Some((from, to)) = ctx.db.sales_backfill_window(2_000).await? {
            busy = true;
            let txids = ctx.db.transfer_txids(from, to).await?;
            let txs: Vec<_> = futures::stream::iter(txids)
                .map(fetch)
                .buffered(16)
                .try_collect()
                .await?;
            let mut sales = 0;
            for tx in &txs {
                if let Some((_, price)) = zscan_chain::inscriptions::single_sale(tx) {
                    ctx.db.record_sale(&tx.txid, price).await?;
                    sales += 1;
                }
            }
            ctx.db.set_sales_cursor(to + 1).await?;
            info!(from, to, moves = txs.len(), sales, "nft sales backfill");
        }

        let todo = ctx.db.drops_to_compute(200).await?;
        if !todo.is_empty() {
            busy = true;
            let keys: Vec<(String, String)> = futures::stream::iter(todo)
                .map(|(id, reveal, minter)| async move {
                    let reveal = fetch(reveal).await?;
                    let key = match reveal.inputs.iter().find_map(|i| i.prev_txid.clone()) {
                        Some(commit) => zscan_chain::inscriptions::drop_key(
                            &fetch(commit).await?,
                            minter.as_deref(),
                        ),
                        None => String::new(),
                    };
                    Ok::<_, anyhow::Error>((id, key))
                })
                .buffered(16)
                .try_collect()
                .await?;
            for (id, key) in &keys {
                ctx.db.set_drop(id, key).await?;
            }
            info!(nfts = keys.len(), "nft drops assigned");
        }

        if !busy {
            tokio::time::sleep(Duration::from_secs(600)).await;
        }
    }
}

/// Height, time and the decoded transactions with transparent inputs of a compact block.
async fn transparent_txs(
    lwd: &Lwd,
    cb: zscan_chain::proto::CompactBlock,
) -> Result<(u32, i64, Vec<(u32, zscan_chain::ParsedTx)>)> {
    let height = cb.height as u32;
    let txs = futures::stream::iter(cb.vtx.into_iter().filter(|t| !t.vin.is_empty()))
        .map(|t| async move {
            let txid = zscan_chain::hash::to_display(&t.txid);
            let raw = lwd
                .raw_tx(&txid)
                .await?
                .with_context(|| format!("tx {txid} not found"))?;
            let tx = zscan_chain::parse_tx(&raw.data).with_context(|| format!("decode {txid}"))?;
            Ok::<_, anyhow::Error>((t.index as u32, tx))
        })
        .buffered(16)
        .try_collect()
        .await
        .with_context(|| format!("block {height}"))?;
    Ok((height, cb.time as i64, txs))
}

/// Fills mining-pool tags for blocks indexed before tagging existed.
async fn retag(ctx: &Ctx) -> Result<()> {
    // Blocks indexed while the miner was taken from the first coinbase output could name a
    // funding-stream address (the dev fund) as their miner; point them at the real payout.
    let sync = ctx.db.sync_state().await?;
    let mut funding: Vec<String> = Vec::new();
    for h in [sync.low, sync.high].into_iter().flatten() {
        funding.extend(zscan_chain::economics::funding_stream_addresses(
            h,
            zscan_chain::network(),
        ));
    }
    funding.sort();
    funding.dedup();
    let fixed = zscan_db::sqlx::query(
        "UPDATE blocks b SET miner = (
            SELECT o.address FROM transactions t JOIN tx_outputs o ON o.txid = t.txid
            WHERE t.height = b.height AND t.block_index = 0 AND o.address IS NOT NULL
              AND NOT (o.address = ANY($1))
            ORDER BY o.n LIMIT 1)
         WHERE b.miner = ANY($1)",
    )
    .bind(&funding)
    .execute(&ctx.db.pool)
    .await?
    .rows_affected();
    if fixed > 0 {
        info!(
            blocks = fixed,
            "miner re-attributed away from funding-stream outputs"
        );
    }

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
                n_actions: tx.logical_actions() as i32,
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
