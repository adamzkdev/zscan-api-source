//! Transaction classification and value-flow accounting.

use serde::Serialize;

use crate::parse::ParsedTx;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TxKind {
    Coinbase,
    /// Transparent only, no shielded components.
    Transparent,
    /// Transparent value moves into shielded pools.
    Shielding,
    /// Shielded value moves out to transparent outputs.
    Deshielding,
    /// Fully shielded: no transparent inputs or outputs.
    Shielded,
    /// Value leaves Orchard and enters Ironwood.
    Migration,
    Mixed,
}

impl TxKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TxKind::Coinbase => "coinbase",
            TxKind::Transparent => "transparent",
            TxKind::Shielding => "shielding",
            TxKind::Deshielding => "deshielding",
            TxKind::Shielded => "shielded",
            TxKind::Migration => "migration",
            TxKind::Mixed => "mixed",
        }
    }
}

/// Net change of each chain value pool caused by a transaction, in zatoshis.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct PoolDeltas {
    pub transparent: i64,
    pub sprout: i64,
    pub sapling: i64,
    pub orchard: i64,
    pub ironwood: i64,
}

impl std::ops::AddAssign for PoolDeltas {
    fn add_assign(&mut self, o: Self) {
        self.transparent += o.transparent;
        self.sprout += o.sprout;
        self.sapling += o.sapling;
        self.orchard += o.orchard;
        self.ironwood += o.ironwood;
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TxSummary {
    pub kind: TxKind,
    /// `None` when a spent output's value could not be resolved, or for coinbase.
    pub fee: Option<i64>,
    pub transparent_in: i64,
    pub transparent_out: i64,
    pub deltas: PoolDeltas,
}

/// `input_values[i]` is the value of the output spent by input `i`, if known.
pub fn summarize(tx: &ParsedTx, input_values: &[Option<i64>]) -> TxSummary {
    let complete = tx.is_coinbase || input_values.iter().all(Option::is_some);
    let t_in: i64 = if tx.is_coinbase {
        0
    } else {
        input_values.iter().flatten().sum()
    };
    let t_out = tx.transparent_out();
    let shielded_net = tx.sprout_vb + tx.sapling_vb + tx.orchard_vb + tx.ironwood_vb;

    let fee = (!tx.is_coinbase && complete).then(|| t_in - t_out + shielded_net);

    let kind = if tx.is_coinbase {
        TxKind::Coinbase
    } else if tx.orchard_vb > 0 && tx.ironwood_vb < 0 {
        TxKind::Migration
    } else if !tx.has_shielded() {
        TxKind::Transparent
    } else if tx.inputs.is_empty() && tx.outputs.is_empty() {
        TxKind::Shielded
    } else if !tx.inputs.is_empty() && shielded_net < 0 {
        TxKind::Shielding
    } else if !tx.outputs.is_empty() && shielded_net > 0 {
        TxKind::Deshielding
    } else {
        TxKind::Mixed
    };

    TxSummary {
        kind,
        fee,
        transparent_in: t_in,
        transparent_out: t_out,
        deltas: PoolDeltas {
            transparent: t_out - t_in,
            sprout: -tx.sprout_vb,
            sapling: -tx.sapling_vb,
            orchard: -tx.orchard_vb,
            ironwood: -tx.ironwood_vb,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::{Action, TxInput, TxOutput};

    fn tx() -> ParsedTx {
        ParsedTx {
            txid: String::new(),
            version: 6,
            size: 0,
            is_coinbase: false,
            lock_time: 0,
            expiry_height: None,
            inputs: vec![],
            outputs: vec![],
            sprout_vb: 0,
            sapling_vb: 0,
            orchard_vb: 0,
            ironwood_vb: 0,
            n_joinsplits: 0,
            sapling_spends: vec![],
            sapling_outputs: vec![],
            orchard_actions: vec![],
            ironwood_actions: vec![],
        }
    }

    fn action() -> Action {
        Action {
            nullifier: String::new(),
            cmx: String::new(),
        }
    }

    fn input() -> TxInput {
        TxInput {
            prev_txid: Some("00".into()),
            prev_n: Some(0),
            coinbase_data: None,
        }
    }

    fn output(value: i64) -> TxOutput {
        TxOutput {
            n: 0,
            value,
            address: None,
            script: String::new(),
            op_return: None,
        }
    }

    #[test]
    fn migration() {
        let mut t = tx();
        t.orchard_actions = vec![action(), action()];
        t.ironwood_actions = vec![action(), action()];
        t.orchard_vb = 500_010_000;
        t.ironwood_vb = -500_000_000;
        let s = summarize(&t, &[]);
        assert_eq!(s.kind, TxKind::Migration);
        assert_eq!(s.fee, Some(10_000));
        assert_eq!(s.deltas.orchard, -500_010_000);
        assert_eq!(s.deltas.ironwood, 500_000_000);
    }

    #[test]
    fn shielding_with_change() {
        let mut t = tx();
        t.inputs = vec![input()];
        t.outputs = vec![output(40_000)];
        t.ironwood_actions = vec![action(), action()];
        t.ironwood_vb = -100_000;
        let s = summarize(&t, &[Some(150_000)]);
        assert_eq!(s.kind, TxKind::Shielding);
        assert_eq!(s.fee, Some(10_000));
        assert_eq!(s.deltas.transparent, -110_000);
    }

    #[test]
    fn deshielding() {
        let mut t = tx();
        t.outputs = vec![output(90_000)];
        t.ironwood_actions = vec![action(), action()];
        t.ironwood_vb = 100_000;
        let s = summarize(&t, &[]);
        assert_eq!(s.kind, TxKind::Deshielding);
        assert_eq!(s.fee, Some(10_000));
    }

    #[test]
    fn unknown_input_value_means_unknown_fee() {
        let mut t = tx();
        t.inputs = vec![input()];
        t.outputs = vec![output(1)];
        let s = summarize(&t, &[None]);
        assert_eq!(s.kind, TxKind::Transparent);
        assert_eq!(s.fee, None);
    }
}
