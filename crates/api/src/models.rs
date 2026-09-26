//! Response types shared by the routes.

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::FromRow;
use utoipa::ToSchema;
use zscan_db::IndexedTx;

/// Amounts are in zatoshis (1 ZEC = 100,000,000 zat).
#[derive(Debug, Serialize, FromRow, ToSchema)]
pub struct BlockSummary {
    pub height: i32,
    pub hash: String,
    pub time: DateTime<Utc>,
    pub tx_count: i32,
    pub size: i32,
    pub fees: Option<i64>,
    pub miner: Option<String>,
    /// Mining pool, identified from coinbase signatures.
    pub miner_pool: Option<String>,
    /// Miner reward including fees, after the lockbox deferral.
    pub coinbase_value: i64,
}

#[derive(Debug, Serialize, FromRow, ToSchema)]
pub struct TxRow {
    pub txid: String,
    pub height: i32,
    pub block_index: i32,
    pub time: DateTime<Utc>,
    /// coinbase | transparent | shielding | deshielding | shielded | migration | mixed
    pub kind: String,
    pub fee: Option<i64>,
    pub size: i32,
    pub transparent_in: i64,
    pub transparent_out: i64,
    pub sapling_vb: i64,
    pub orchard_vb: i64,
    pub ironwood_vb: i64,
    pub n_tin: i32,
    pub n_tout: i32,
    pub n_sapling_spends: i32,
    pub n_sapling_outputs: i32,
    pub n_orchard_actions: i32,
    pub n_ironwood_actions: i32,
    /// First transparent input address, if any.
    pub from_address: Option<String>,
    /// First transparent output address, if any.
    pub to_address: Option<String>,
}

pub const TX_ROW_COLUMNS: &str =
    "txid, height, block_index, time, kind, fee, size, transparent_in, \
    transparent_out, sapling_vb, orchard_vb, ironwood_vb, n_tin, n_tout, n_sapling_spends, \
    n_sapling_outputs, n_orchard_actions, n_ironwood_actions, \
    (SELECT address FROM tx_inputs i WHERE i.txid = transactions.txid ORDER BY n LIMIT 1) AS from_address, \
    (SELECT address FROM tx_outputs o WHERE o.txid = transactions.txid ORDER BY n LIMIT 1) AS to_address";

impl TxRow {
    pub fn from_indexed(t: &IndexedTx, height: u32, time: DateTime<Utc>) -> Self {
        let p = &t.tx;
        TxRow {
            txid: p.txid.clone(),
            height: height as i32,
            block_index: t.block_index as i32,
            time,
            kind: t.summary.kind.as_str().to_string(),
            fee: t.summary.fee,
            size: p.size as i32,
            transparent_in: t.summary.transparent_in,
            transparent_out: t.summary.transparent_out,
            sapling_vb: p.sapling_vb,
            orchard_vb: p.orchard_vb,
            ironwood_vb: p.ironwood_vb,
            n_tin: p.inputs.len() as i32,
            n_tout: p.outputs.len() as i32,
            n_sapling_spends: p.sapling_spends.len() as i32,
            n_sapling_outputs: p.sapling_outputs.len() as i32,
            n_orchard_actions: p.orchard_actions.len() as i32,
            n_ironwood_actions: p.ironwood_actions.len() as i32,
            from_address: t.prevouts.iter().flatten().find_map(|p| p.address.clone()),
            to_address: p.outputs.iter().find_map(|o| o.address.clone()),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PoolValues {
    pub transparent: i64,
    pub sprout: i64,
    pub sapling: i64,
    pub orchard: i64,
    pub ironwood: i64,
}
