//! Compact shielded blocks in ZScan's binary layout ("ZSC1"), as served by `GET /v1/compact`
//! and trial-decrypted by the viewing-key scanner (in the browser, and by the treasury indexer).

use anyhow::Result;

use crate::Lwd;

/// `[start, end]` in the "ZSC1" layout documented on the `/v1/compact` route. Every block of the
/// range is present, including the ones with no shielded data.
pub async fn compact_range(lwd: &Lwd, start: u32, end: u32) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(64 * 1024);
    out.extend_from_slice(b"ZSC1");
    let mut stream = lwd.block_range(start, end).await?;
    while let Some(b) = stream.message().await? {
        let txs: Vec<_> = b
            .vtx
            .iter()
            .filter(|t| {
                !t.spends.is_empty()
                    || !t.outputs.is_empty()
                    || !t.actions.is_empty()
                    || !t.ironwood_actions.is_empty()
            })
            .collect();
        out.extend_from_slice(&(b.height as u32).to_le_bytes());
        out.extend_from_slice(&b.time.to_le_bytes());
        let sapling_size = b
            .chain_metadata
            .as_ref()
            .map_or(0, |m| m.sapling_commitment_tree_size);
        out.extend_from_slice(&sapling_size.to_le_bytes());
        out.extend_from_slice(&(txs.len() as u32).to_le_bytes());
        for t in txs {
            put(&mut out, &t.txid, 32);
            out.extend_from_slice(&(t.index as u32).to_le_bytes());
            for n in [
                t.spends.len(),
                t.outputs.len(),
                t.actions.len(),
                t.ironwood_actions.len(),
            ] {
                out.extend_from_slice(&(n as u16).to_le_bytes());
            }
            for sp in &t.spends {
                put(&mut out, &sp.nf, 32);
            }
            for o in &t.outputs {
                put(&mut out, &o.cmu, 32);
                put(&mut out, &o.ephemeral_key, 32);
                put(&mut out, &o.ciphertext, 52);
            }
            for a in t.actions.iter().chain(&t.ironwood_actions) {
                put(&mut out, &a.nullifier, 32);
                put(&mut out, &a.cmx, 32);
                put(&mut out, &a.ephemeral_key, 32);
                put(&mut out, &a.ciphertext, 52);
            }
        }
    }
    Ok(out)
}

/// Writes exactly `len` bytes: the field, truncated or zero-padded (fields are fixed-size in
/// valid blocks; this only keeps the layout aligned if a server sends something odd).
fn put(out: &mut Vec<u8>, bytes: &[u8], len: usize) {
    let n = bytes.len().min(len);
    out.extend_from_slice(&bytes[..n]);
    out.resize(out.len() + (len - n), 0);
}
