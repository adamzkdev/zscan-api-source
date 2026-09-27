//! Raw transaction decoding (v1–v6, including Ironwood) via zebra-chain.

use anyhow::{Context, Result};
use serde::Serialize;
use zebra_chain::{
    parameters::Network,
    serialization::ZcashDeserializeInto,
    transaction::Transaction,
    transparent::{self, Input},
};

#[derive(Debug, Clone, Serialize)]
pub struct TxInput {
    /// Display-order txid of the spent output; `None` for coinbase.
    pub prev_txid: Option<String>,
    pub prev_n: Option<u32>,
    /// Hex of the coinbase data, for coinbase inputs.
    pub coinbase_data: Option<String>,
    /// Unlocking script, which carries inscription envelopes. Not part of API responses.
    #[serde(skip)]
    pub script_sig: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TxOutput {
    pub n: u32,
    pub value: i64,
    pub address: Option<String>,
    pub script: String,
    /// Payload of an OP_RETURN output (hex), if this is one.
    pub op_return: Option<String>,
}

/// An Orchard or Ironwood action.
#[derive(Debug, Clone, Serialize)]
pub struct Action {
    pub nullifier: String,
    pub cmx: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParsedTx {
    pub txid: String,
    pub version: u32,
    pub size: usize,
    pub is_coinbase: bool,
    pub lock_time: u32,
    pub expiry_height: Option<u32>,
    pub inputs: Vec<TxInput>,
    pub outputs: Vec<TxOutput>,
    /// Value balances in zatoshis. Positive = value leaves the pool.
    pub sprout_vb: i64,
    pub sapling_vb: i64,
    pub orchard_vb: i64,
    pub ironwood_vb: i64,
    pub n_joinsplits: usize,
    pub sapling_spends: Vec<String>,
    pub sapling_outputs: Vec<String>,
    pub orchard_actions: Vec<Action>,
    pub ironwood_actions: Vec<Action>,
}

impl ParsedTx {
    pub fn transparent_out(&self) -> i64 {
        self.outputs.iter().map(|o| o.value).sum()
    }

    pub fn has_shielded(&self) -> bool {
        self.n_joinsplits > 0
            || !self.sapling_spends.is_empty()
            || !self.sapling_outputs.is_empty()
            || !self.orchard_actions.is_empty()
            || !self.ironwood_actions.is_empty()
    }

    /// ZIP-317 logical actions, which set the conventional fee. Transparent inputs and outputs
    /// are counted one each (standard P2PKH sizes), as in the fee endpoint.
    pub fn logical_actions(&self) -> usize {
        self.inputs.len().max(self.outputs.len())
            + 2 * self.n_joinsplits
            + self.sapling_spends.len().max(self.sapling_outputs.len())
            + self.orchard_actions.len()
            + self.ironwood_actions.len()
    }
}

pub fn parse_tx(raw: &[u8]) -> Result<ParsedTx> {
    let tx: Transaction = raw.zcash_deserialize_into().context("decode transaction")?;
    let net: &Network = crate::network();

    let inputs = tx
        .inputs()
        .into_iter()
        .map(|i| match i {
            Input::PrevOut {
                outpoint,
                unlock_script,
                ..
            } => TxInput {
                prev_txid: Some(outpoint.hash.to_string()),
                prev_n: Some(outpoint.index),
                coinbase_data: None,
                script_sig: unlock_script.as_raw_bytes().to_vec(),
            },
            Input::Coinbase { data, .. } => TxInput {
                prev_txid: None,
                prev_n: None,
                coinbase_data: Some(hex::encode(data)),
                script_sig: Vec::new(),
            },
        })
        .collect();

    let outputs = tx
        .outputs()
        .iter()
        .enumerate()
        .map(|(n, o): (usize, &transparent::Output)| TxOutput {
            n: n as u32,
            value: i64::from(o.value()),
            address: o.address(net).map(|a| a.to_string()),
            script: hex::encode(o.lock_script.as_raw_bytes()),
            op_return: op_return_payload(o.lock_script.as_raw_bytes()).map(hex::encode),
        })
        .collect();

    // Orchard and Ironwood share the same action type.
    macro_rules! actions {
        ($iter:expr) => {
            $iter
                .map(|a| Action {
                    nullifier: hex::encode(a.nullifier().to_bytes()),
                    cmx: hex::encode(a.cmx().to_bytes()),
                })
                .collect()
        };
    }

    Ok(ParsedTx {
        txid: tx.hash().to_string(),
        version: tx.version(),
        size: raw.len(),
        is_coinbase: tx.is_coinbase(),
        lock_time: tx.raw_lock_time(),
        expiry_height: tx.expiry_height().map(|h| h.0),
        inputs,
        outputs,
        sprout_vb: tx
            .sprout_value_balance()
            .map(|v| i64::from(v.sprout_amount()))
            .unwrap_or(0),
        sapling_vb: i64::from(tx.sapling_value_balance().sapling_amount()),
        orchard_vb: i64::from(tx.orchard_value_balance().orchard_amount()),
        ironwood_vb: i64::from(tx.ironwood_value_balance().ironwood_amount()),
        n_joinsplits: tx.joinsplit_count(),
        sapling_spends: tx
            .sapling_spends()
            .map(|s| hex::encode(s.nullifier().0))
            .collect(),
        sapling_outputs: tx
            .sapling_outputs()
            .map(|o| hex::encode(o.cmu().to_bytes()))
            .collect(),
        orchard_actions: actions!(tx.orchard_actions()),
        ironwood_actions: actions!(tx.ironwood_actions()),
    })
}

/// Full note ciphertexts of a transaction's shielded outputs, for wallets and viewing-key tools
/// that decrypt memos or recover outgoing notes (compact blocks only carry 52 bytes).
#[derive(Debug, Clone, Serialize)]
pub struct ShieldedOutput {
    /// Nullifier of the spent note (Orchard/Ironwood actions only), hex.
    pub nullifier: Option<String>,
    /// Note commitment (`cmu` for Sapling, `cmx` for Orchard/Ironwood), hex.
    pub cm: String,
    /// Value commitment (`cv` / `cv_net`), hex.
    pub cv: String,
    pub epk: String,
    /// 580 bytes, hex.
    pub enc_ciphertext: String,
    /// 80 bytes, hex.
    pub out_ciphertext: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShieldedOutputs {
    pub sapling: Vec<ShieldedOutput>,
    pub orchard: Vec<ShieldedOutput>,
    pub ironwood: Vec<ShieldedOutput>,
}

pub fn shielded_outputs(raw: &[u8]) -> Result<ShieldedOutputs> {
    let tx: Transaction = raw.zcash_deserialize_into().context("decode transaction")?;
    macro_rules! actions {
        ($iter:expr) => {
            $iter
                .map(|a| {
                    let n = a.encrypted_note();
                    ShieldedOutput {
                        nullifier: Some(hex::encode(a.nullifier().to_bytes())),
                        cm: hex::encode(a.cmx().to_bytes()),
                        cv: hex::encode(a.cv_net().to_bytes()),
                        epk: hex::encode(n.epk_bytes),
                        enc_ciphertext: hex::encode(n.enc_ciphertext),
                        out_ciphertext: hex::encode(n.out_ciphertext),
                    }
                })
                .collect()
        };
    }
    Ok(ShieldedOutputs {
        sapling: tx
            .sapling_outputs()
            .map(|o| ShieldedOutput {
                nullifier: None,
                cm: hex::encode(o.cmu().to_bytes()),
                cv: hex::encode(o.cv().to_bytes()),
                epk: hex::encode(o.ephemeral_key().0),
                enc_ciphertext: hex::encode(o.enc_ciphertext()),
                out_ciphertext: hex::encode(o.out_ciphertext()),
            })
            .collect(),
        orchard: actions!(tx.orchard_actions()),
        ironwood: actions!(tx.ironwood_actions()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fully shielded Ironwood transaction mined at height 3,497,141.
    const IRONWOOD_TX: &str = include_str!("../testdata/ironwood_3497141.hex");

    #[test]
    fn decodes_ironwood_v6() {
        let raw = hex::decode(IRONWOOD_TX.trim()).unwrap();
        let tx = parse_tx(&raw).unwrap();
        assert_eq!(tx.version, 6);
        assert_eq!(
            tx.txid,
            "39d1e0130e5f8381c6960cdf6e0930ea9351f3fd1b405243ebf3561d733b294c"
        );
        assert_eq!(tx.ironwood_actions.len(), 2);
        assert_eq!(tx.ironwood_vb, 10_000);
        assert!(tx.inputs.is_empty() && tx.outputs.is_empty());
    }
}

/// Data carried by an `OP_RETURN` script: `0x6a` followed by pushes.
pub fn op_return_payload(script: &[u8]) -> Option<Vec<u8>> {
    let (&op, mut rest) = script.split_first()?;
    if op != 0x6a {
        return None;
    }
    let mut data = Vec::new();
    while let Some((&len_op, tail)) = rest.split_first() {
        let (len, tail) = match len_op {
            0x01..=0x4b => (len_op as usize, tail),
            0x4c => (*tail.first()? as usize, &tail[1..]),
            0x4d => (
                u16::from_le_bytes([*tail.first()?, *tail.get(1)?]) as usize,
                &tail[2..],
            ),
            _ => return Some(data),
        };
        data.extend_from_slice(tail.get(..len)?);
        rest = &tail[len..];
    }
    Some(data)
}

/// Kind of address a search string looks like.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressKind {
    Transparent,
    Shielded,
}

/// Classifies a mainnet address string. Transparent addresses are fully validated.
pub fn address_kind(s: &str) -> Option<AddressKind> {
    let transparent_prefix = ["t1", "t3", "tm", "t2"].iter().any(|p| s.starts_with(p));
    if (transparent_prefix && s.parse::<transparent::Address>().is_ok())
        || tex_to_p2pkh(s).is_some()
    {
        return Some(AddressKind::Transparent);
    }
    let shielded_prefix = ["zs1", "u1", "zc", "ztestsapling", "utest"]
        .iter()
        .any(|p| s.starts_with(p));
    (shielded_prefix && s.len() > 40 && s.chars().all(|c| c.is_ascii_alphanumeric()))
        .then_some(AddressKind::Shielded)
}

/// A TEX address (ZIP-320) is a P2PKH address that only accepts transparent funds; on-chain it
/// pays the ordinary P2PKH script. Returns that `t1…`/`tm…` address, which is how it is indexed.
pub fn tex_to_p2pkh(s: &str) -> Option<String> {
    if !(s.starts_with("tex1") || s.starts_with("textest1")) {
        return None;
    }
    match s.parse::<transparent::Address>().ok()? {
        transparent::Address::Tex {
            network_kind,
            validating_key_hash,
        } => Some(
            transparent::Address::from_pub_key_hash(network_kind, validating_key_hash).to_string(),
        ),
        _ => None,
    }
}

#[cfg(test)]
mod address_tests {
    use super::*;

    #[test]
    fn classifies_addresses() {
        assert_eq!(
            address_kind("t1XQZdZMnzXBcL8yx2PR27dSNrqctgwLgux"),
            Some(AddressKind::Transparent)
        );
        assert_eq!(address_kind("t1XQZdZMnzXBcL8yx2PR27dSNrqctgwLguy"), None);
        assert_eq!(
            address_kind("u157q45z9fwy7349t5r7w4d0tpmdzhw6xvuwm4ucqq80ysftk9wxryrru0l7puxpdlujnkmj5d9ewzaxz5x6slfg3xys4egayx7vry57wd"),
            Some(AddressKind::Shielded)
        );
        assert_eq!(address_kind("hello"), None);
    }

    #[test]
    fn tex_is_transparent() {
        // ZIP-320 test vector.
        let tex = "tex1s2rt77ggv6q989lr49rkgzmh5slsksa9khdgte";
        assert_eq!(address_kind(tex), Some(AddressKind::Transparent));
        assert_eq!(
            tex_to_p2pkh(tex).as_deref(),
            Some("t1VmmGiyjVNeCjxDZzg7vZmd99WyzVby9yC")
        );
        assert_eq!(tex_to_p2pkh("t1VmmGiyjVNeCjxDZzg7vZmd99WyzVby9yC"), None);
    }

    #[test]
    fn decodes_op_return() {
        let script = [0x6a, 0x05, b'h', b'e', b'l', b'l', b'o'];
        assert_eq!(op_return_payload(&script).unwrap(), b"hello");
        assert_eq!(op_return_payload(&[0x76, 0xa9]), None);
    }
}
