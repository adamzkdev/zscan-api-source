//! Turns a compact block into a fully decoded, fee-annotated `IndexedBlock`.

use anyhow::{anyhow, Context, Result};
use chrono::{TimeZone, Utc};
use futures::{stream, StreamExt, TryStreamExt};
use zscan_chain::{
    classify::PoolDeltas,
    consts::{compact_size_len, BLOCK_HEADER_SIZE},
    hash,
    parse::ParsedTx,
    proto::CompactBlock,
    resolve::{OutPoint, PrevOut},
    summarize, TxKind,
};
use zscan_chain::{Lwd, Resolver};

use crate::{Db, IndexedBlock, IndexedTx};

/// Concurrent raw-transaction fetches per block.
const TX_FETCH_CONCURRENCY: usize = 16;

/// Decodes every transaction of a compact block and resolves spent outputs (index first,
/// then lightwalletd) to compute fees, pool deltas and classification.
pub async fn build_block(
    db: &Db,
    lwd: &Lwd,
    resolver: &Resolver,
    cb: CompactBlock,
) -> Result<IndexedBlock> {
    let height = cb.height as u32;
    let txids: Vec<String> = cb.vtx.iter().map(|t| hash::to_display(&t.txid)).collect();

    // Owned values keep this future `Send` for tokio::spawn.
    let parsed: Vec<ParsedTx> = stream::iter(txids)
        .map(|txid| {
            let lwd = lwd.clone();
            async move {
                let raw = lwd
                    .raw_tx(&txid)
                    .await?
                    .ok_or_else(|| anyhow!("tx {txid} not found"))?;
                zscan_chain::parse_tx(&raw.data).with_context(|| format!("decode {txid}"))
            }
        })
        .buffered(TX_FETCH_CONCURRENCY)
        .try_collect()
        .await
        .with_context(|| format!("block {height}"))?;

    // Outputs created in this block can be spent later in the same block.
    for t in &parsed {
        resolver.remember(
            &t.txid,
            t.outputs
                .iter()
                .map(|o| PrevOut {
                    value: o.value,
                    address: o.address.clone(),
                })
                .collect(),
        );
    }
    let outpoints: Vec<OutPoint> = parsed
        .iter()
        .flat_map(|t| t.inputs.iter())
        .filter_map(|i| Some((i.prev_txid.clone()?, i.prev_n?)))
        .collect();
    let known = db.known_outputs(&outpoints).await?;

    let mut txs = Vec::with_capacity(parsed.len());
    let mut deltas = PoolDeltas::default();
    let mut fees = Some(0i64);
    let mut miner = None;
    let mut miner_tag = None;
    let mut coinbase_value = 0;
    for (i, tx) in parsed.into_iter().enumerate() {
        let prevouts = resolver.resolve(&tx.inputs, &known).await?;
        let values: Vec<Option<i64>> = prevouts
            .iter()
            .map(|p| p.as_ref().map(|p| p.value))
            .collect();
        let summary = summarize(&tx, &values);
        deltas += summary.deltas;
        if summary.kind == TxKind::Coinbase {
            miner = tx.outputs.iter().find_map(|o| o.address.clone());
            // Empty string = "no tag" so the block isn't re-scanned later.
            miner_tag = Some(
                tx.inputs
                    .first()
                    .and_then(|i| i.coinbase_data.as_deref())
                    .and_then(zscan_chain::miner::coinbase_tag)
                    .unwrap_or_default(),
            );
            // Shielded coinbase outputs show up as negative value balances.
            coinbase_value =
                tx.transparent_out() - (tx.sapling_vb + tx.orchard_vb + tx.ironwood_vb);
        } else {
            fees = fees.zip(summary.fee).map(|(a, b)| a + b);
        }
        txs.push(IndexedTx {
            block_index: i as u32,
            tx,
            summary,
            prevouts,
        });
    }

    let econ = zscan_chain::economics::block_economics(height, zscan_chain::network());
    let fs_addrs = zscan_chain::economics::funding_stream_addresses(height, zscan_chain::network());
    let funding_paid: i64 = txs
        .first()
        .filter(|t| t.tx.is_coinbase)
        .map(|t| {
            t.tx.outputs
                .iter()
                .filter(|o| o.address.as_ref().is_some_and(|a| fs_addrs.contains(a)))
                .map(|o| o.value)
                .sum()
        })
        .unwrap_or(0);
    // Whatever the coinbase didn't claim of subsidy + fees is removed from supply (NU7 NSM).
    let burned = econ
        .as_ref()
        .zip(fees)
        .map(|(e, f)| f + e.subsidy - e.deferred - coinbase_value);

    let size = BLOCK_HEADER_SIZE
        + compact_size_len(txs.len())
        + txs.iter().map(|t| t.tx.size).sum::<usize>();
    let meta = cb.chain_metadata.as_ref();

    Ok(IndexedBlock {
        height,
        hash: hash::to_display(&cb.hash),
        prev_hash: hash::to_display(&cb.prev_hash),
        time: Utc
            .timestamp_opt(cb.time as i64, 0)
            .single()
            .unwrap_or_default(),
        size,
        fees,
        miner_pool: miner_tag.as_deref().and_then(zscan_chain::miner::pool_name),
        miner_tag,
        miner,
        coinbase_value,
        subsidy: econ.as_ref().map(|e| e.subsidy),
        funding_paid: econ.as_ref().map(|_| funding_paid),
        deferred: econ.as_ref().map(|e| e.deferred),
        burned,
        sapling_tree_size: meta.map(|m| m.sapling_commitment_tree_size),
        orchard_tree_size: meta.map(|m| m.orchard_commitment_tree_size),
        ironwood_tree_size: meta.map(|m| m.ironwood_commitment_tree_size),
        deltas,
        txs,
    })
}
