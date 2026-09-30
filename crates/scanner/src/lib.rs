//! Viewing-key scanner for the ZScan viewing-key explorer, compiled to WebAssembly.
//!
//! Runs in the browser (in Web Workers): it parses a viewing key, trial-decrypts the compact
//! blocks served by `GET /v1/compact` and reports the notes that belong to the key. The key
//! never leaves the browser; the server only sees which block ranges are downloaded.
//!
//! A full viewing key (UFVK `uview1…`, or a legacy Sapling `zxviews1…`) also finds spends,
//! change and, with the full transaction, outgoing payments. An incoming viewing key
//! (`uivk1…`) only finds received notes.

use std::collections::HashMap;

use orchard::keys::{
    FullViewingKey as OrchardFvk, IncomingViewingKey as OrchardIvk,
    PreparedIncomingViewingKey as OrchardPivk, Scope,
};
use orchard::note::{
    ExtractedNoteCommitment as OrchardCmx, NoteVersion, Nullifier as OrchardNf, RandomSeed,
};
use orchard::note_encryption::{CompactAction, IronwoodDomain, OrchardDomain};
use sapling_crypto::keys::PreparedIncomingViewingKey as SaplingPivk;
use sapling_crypto::note_encryption::{CompactOutputDescription, SaplingDomain, Zip212Enforcement};
use sapling_crypto::zip32::{
    DiversifiableFullViewingKey, ExtendedFullViewingKey, IncomingViewingKey as SaplingIvk,
};
use serde::Serialize;
use wasm_bindgen::prelude::*;
use zcash_address::unified::{self, Container, Encoding};
use zcash_address::{ToAddress, ZcashAddress};
use zcash_note_encryption::{
    batch, try_note_decryption, try_output_recovery_with_ovk, try_output_recovery_with_pkd_esk,
    Domain, EphemeralKeyBytes, ShieldedOutput, ENC_CIPHERTEXT_SIZE,
};
use zcash_protocol::consensus::NetworkType as Network;

/* ------------------------------------------------------------------ keys */

struct OrchardKeys {
    fvk: Option<OrchardFvk>,
    /// External (payments) then internal (change) when the full key is known.
    ivks: Vec<(Scope, OrchardPivk)>,
}

struct SaplingKeys {
    fvk: Option<DiversifiableFullViewingKey>,
    ivks: Vec<(Scope, SaplingPivk)>,
}

fn scope_name(s: Scope) -> &'static str {
    match s {
        Scope::External => "external",
        Scope::Internal => "internal",
    }
}

fn sapling_scope(s: Scope) -> zip32::Scope {
    match s {
        Scope::External => zip32::Scope::External,
        Scope::Internal => zip32::Scope::Internal,
    }
}

fn orchard_from_fvk(fvk: OrchardFvk) -> OrchardKeys {
    let ivks = [Scope::External, Scope::Internal]
        .into_iter()
        .map(|s| (s, OrchardPivk::new(&fvk.to_ivk(s))))
        .collect();
    OrchardKeys {
        fvk: Some(fvk),
        ivks,
    }
}

fn sapling_from_fvk(fvk: DiversifiableFullViewingKey) -> SaplingKeys {
    let ivks = [Scope::External, Scope::Internal]
        .into_iter()
        .map(|s| (s, SaplingPivk::new(&fvk.to_ivk(sapling_scope(s)))))
        .collect();
    SaplingKeys {
        fvk: Some(fvk),
        ivks,
    }
}

#[derive(Serialize)]
struct KeyInfo {
    network: &'static str,
    /// "full" (UFVK / Sapling extended FVK) or "incoming" (UIVK).
    kind: &'static str,
    pools: Vec<&'static str>,
    /// The key also carries a transparent component, which this scanner does not follow.
    has_transparent: bool,
}

fn net_name(n: Network) -> &'static str {
    match n {
        Network::Main => "mainnet",
        Network::Test => "testnet",
        Network::Regtest => "regtest",
    }
}

/// Refuses anything that could spend funds, with a message saying why.
fn refuse_secrets(key: &str) -> Result<(), String> {
    let words = key.split_whitespace().count();
    if words >= 12 {
        return Err(String::from(
            "This looks like a seed phrase. Never paste your seed phrase into a website: it gives full control of your funds. Export a viewing key from your wallet instead.",
        ));
    }
    let lower = key.to_ascii_lowercase();
    if [
        "secret-extended-key",
        "zxsk",
        "secret-",
        "xprv",
        "tprv",
        "usk",
    ]
    .iter()
    .any(|p| lower.starts_with(p))
    {
        return Err(String::from(
            "This is a spending key: it can move your funds. Do not paste it anywhere. Export a viewing key (starts with uview1 or uivk1) instead.",
        ));
    }
    Ok(())
}

/* ------------------------------------------------------------------ scanner */

#[derive(Serialize)]
pub struct FoundNote {
    pub txid: String,
    pub height: u32,
    pub time: u32,
    /// Position of the transaction in its block.
    pub tx_index: u32,
    pub pool: &'static str,
    /// Index of the output (Sapling) or action (Orchard/Ironwood) within the transaction.
    pub output_index: u32,
    pub value: u64,
    /// "external" for received payments, "internal" for change.
    pub scope: &'static str,
    /// Nullifier (hex) that will mark this note spent; null with an incoming viewing key.
    pub nf: Option<String>,
}

#[derive(Serialize)]
pub struct ScanResult {
    pub notes: Vec<FoundNote>,
    pub first_height: u32,
    pub last_height: u32,
    pub blocks: u32,
    pub outputs: u32,
}

#[derive(Serialize)]
pub struct Spend {
    pub nf: String,
    pub txid: String,
    pub height: u32,
    pub time: u32,
}

/// Where a spend nullifier was seen: index into `spend_txs`.
type SpendRef = u32;

#[wasm_bindgen]
pub struct Scanner {
    network: Network,
    orchard: Option<OrchardKeys>,
    sapling: Option<SaplingKeys>,
    /// First 8 bytes of every spend nullifier seen in the scanned blocks → the spending tx.
    /// Notes can be found by another worker, so spends are matched once the scan ends.
    spends: HashMap<[u8; 8], SpendRef>,
    spend_txs: Vec<([u8; 32], u32, u32)>,
    has_transparent: bool,
}

fn txid_display(protocol_order: &[u8; 32]) -> String {
    let mut b = *protocol_order;
    b.reverse();
    hex::encode(b)
}

/// Reads the fixed-size fields of the `/v1/compact` layout.
struct Reader<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let s = self
            .b
            .get(self.at..self.at + n)
            .ok_or("Truncated block data")?;
        self.at += n;
        Ok(s)
    }
    fn arr<const N: usize>(&mut self) -> Result<[u8; N], String> {
        Ok(self.take(N)?.try_into().expect("length checked"))
    }
    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(self.arr()?))
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.arr()?))
    }
    fn done(&self) -> bool {
        self.at >= self.b.len()
    }
}

/// Where an output sits, to report it and derive its nullifier.
#[derive(Clone, Copy)]
struct Meta {
    tx: usize,
    output_index: u32,
    /// Sapling note commitment tree position (Sapling only).
    position: u64,
}

struct Tx {
    txid: [u8; 32],
    height: u32,
    time: u32,
    index: u32,
}

#[wasm_bindgen]
impl Scanner {
    /// Parses a viewing key. Errors (with a readable message) on spending keys, seed phrases
    /// and anything that is not a viewing key.
    #[wasm_bindgen(constructor)]
    pub fn new(key: &str) -> Result<Scanner, JsError> {
        Scanner::parse(key).map_err(|e| JsError::new(&e))
    }

    /// Network, key kind and scanned pools of the key.
    pub fn info(&self) -> Result<JsValue, JsError> {
        let mut pools = Vec::new();
        if self.sapling.is_some() {
            pools.push("sapling");
        }
        if self.orchard.is_some() {
            pools.extend(["orchard", "ironwood"]);
        }
        let full = self.orchard.as_ref().map_or(true, |o| o.fvk.is_some())
            && self.sapling.as_ref().map_or(true, |s| s.fvk.is_some());
        let info = KeyInfo {
            network: net_name(self.network),
            kind: if full { "full" } else { "incoming" },
            pools,
            has_transparent: self.has_transparent,
        };
        Ok(serde_wasm_bindgen::to_value(&info)?)
    }

    /// Scans one `/v1/compact` response and returns the notes that belong to the key.
    pub fn scan(&mut self, data: &[u8]) -> Result<JsValue, JsError> {
        let res = self.scan_blocks(data).map_err(|e| JsError::new(&e))?;
        Ok(serde_wasm_bindgen::to_value(&res)?)
    }
    /// Transactions (among the blocks this scanner saw) that spent any of the given nullifiers.
    pub fn spent_by(&self, nullifiers: Vec<String>) -> Result<JsValue, JsError> {
        Ok(serde_wasm_bindgen::to_value(&self.find_spends(nullifiers))?)
    }

    /// Frees the spend index (after `spent_by`).
    pub fn clear_spends(&mut self) {
        self.spends = HashMap::new();
        self.spend_txs = Vec::new();
    }

    /// Decrypts the full outputs of one transaction (`GET /v1/tx/{txid}/shielded`): memos of
    /// the notes received, and with a full viewing key the payments this key sent.
    pub fn decrypt_tx(&self, outputs: JsValue) -> Result<JsValue, JsError> {
        let o: TxOutputs = serde_wasm_bindgen::from_value(outputs)?;
        Ok(serde_wasm_bindgen::to_value(&self.decrypt_outputs(&o))?)
    }
}

impl Scanner {
    fn find_spends(&self, nullifiers: Vec<String>) -> Vec<Spend> {
        let mut out = Vec::new();
        for nf in nullifiers {
            let Ok(bytes) = hex::decode(&nf) else {
                continue;
            };
            let Some(prefix) = bytes.get(..8).and_then(|p| <[u8; 8]>::try_from(p).ok()) else {
                continue;
            };
            if let Some(&r) = self.spends.get(&prefix) {
                let (txid, height, time) = self.spend_txs[r as usize];
                out.push(Spend {
                    nf,
                    txid: txid_display(&txid),
                    height,
                    time,
                });
            }
        }
        out
    }

    fn parse(key: &str) -> Result<Scanner, String> {
        let key = key.trim();
        refuse_secrets(key)?;
        let mut orchard = None;
        let mut sapling = None;
        let mut has_transparent = false;
        let network;
        let lower = key.to_ascii_lowercase();

        if lower.starts_with("uview") {
            let (net, ufvk) = unified::Ufvk::decode(key)
                .map_err(|e| String::from(&format!("Invalid unified full viewing key: {e}")))?;
            network = net;
            for item in ufvk.items() {
                match item {
                    unified::Fvk::Orchard(b) => {
                        let fvk = OrchardFvk::from_bytes(&b)
                            .ok_or_else(|| String::from("Invalid Orchard viewing key"))?;
                        orchard = Some(orchard_from_fvk(fvk));
                    }
                    unified::Fvk::Sapling(b) => {
                        let fvk = DiversifiableFullViewingKey::from_bytes(&b)
                            .ok_or_else(|| String::from("Invalid Sapling viewing key"))?;
                        sapling = Some(sapling_from_fvk(fvk));
                    }
                    unified::Fvk::P2pkh(_) => has_transparent = true,
                    _ => {}
                }
            }
        } else if lower.starts_with("uivk") {
            let (net, uivk) = unified::Uivk::decode(key)
                .map_err(|e| String::from(&format!("Invalid unified incoming viewing key: {e}")))?;
            network = net;
            for item in uivk.items() {
                match item {
                    unified::Ivk::Orchard(b) => {
                        let ivk: Option<OrchardIvk> = OrchardIvk::from_bytes(&b).into();
                        let ivk = ivk
                            .ok_or_else(|| String::from("Invalid Orchard incoming viewing key"))?;
                        orchard = Some(OrchardKeys {
                            fvk: None,
                            ivks: vec![(Scope::External, OrchardPivk::new(&ivk))],
                        });
                    }
                    unified::Ivk::Sapling(b) => {
                        let ivk: Option<SaplingIvk> = SaplingIvk::from_bytes(&b).into();
                        let ivk = ivk
                            .ok_or_else(|| String::from("Invalid Sapling incoming viewing key"))?;
                        sapling = Some(SaplingKeys {
                            fvk: None,
                            ivks: vec![(Scope::External, ivk.prepare())],
                        });
                    }
                    unified::Ivk::P2pkh(_) => has_transparent = true,
                    _ => {}
                }
            }
        } else if lower.starts_with("zxview") {
            let (hrp, data) = bech32::decode(key)
                .map_err(|e| String::from(&format!("Invalid Sapling viewing key: {e}")))?;
            network = match hrp.as_str() {
                "zxviews" => Network::Main,
                "zxviewtestsapling" => Network::Test,
                _ => return Err(String::from("Unknown Sapling viewing key prefix")),
            };
            let efvk = ExtendedFullViewingKey::read(&data[..])
                .map_err(|e| String::from(&format!("Invalid Sapling viewing key: {e}")))?;
            sapling = Some(sapling_from_fvk(efvk.to_diversifiable_full_viewing_key()));
        } else {
            return Err(String::from(
                "Not a viewing key. Paste a unified full viewing key (uview1…), a unified incoming viewing key (uivk1…) or a Sapling viewing key (zxviews1…).",
            ));
        }
        if orchard.is_none() && sapling.is_none() {
            return Err(String::from(
                "This key has no Sapling or Orchard component to scan.",
            ));
        }
        Ok(Scanner {
            network,
            orchard,
            sapling,
            spends: HashMap::new(),
            spend_txs: Vec::new(),
            has_transparent,
        })
    }

    fn scan_blocks(&mut self, data: &[u8]) -> Result<ScanResult, String> {
        let mut r = Reader { b: data, at: 0 };
        if r.take(4)? != b"ZSC1" {
            return Err("Unexpected block data format".into());
        }
        let mut txs: Vec<Tx> = Vec::new();
        let mut sapling: Vec<(SaplingDomain, CompactOutputDescription)> = Vec::new();
        let mut sapling_meta: Vec<Meta> = Vec::new();
        let mut orchard: Vec<(OrchardDomain, CompactAction)> = Vec::new();
        let mut orchard_meta: Vec<Meta> = Vec::new();
        let mut ironwood: Vec<(IronwoodDomain, CompactAction)> = Vec::new();
        let mut ironwood_meta: Vec<Meta> = Vec::new();
        let (mut first, mut last, mut blocks, mut outputs) = (u32::MAX, 0u32, 0u32, 0u32);

        while !r.done() {
            let height = r.u32()?;
            let time = r.u32()?;
            let sapling_size_end = r.u32()?;
            let n_tx = r.u32()?;
            first = first.min(height);
            last = last.max(height);
            blocks += 1;
            // Sapling positions: the block's outputs end at `sapling_size_end`.
            let block_sapling_start = sapling.len();
            for _ in 0..n_tx {
                let txid: [u8; 32] = r.arr()?;
                let index = r.u32()?;
                let (ns, no, na, ni) = (r.u16()?, r.u16()?, r.u16()?, r.u16()?);
                let tx = txs.len();
                txs.push(Tx {
                    txid,
                    height,
                    time,
                    index,
                });
                let mut tx_has_spend = false;
                for _ in 0..ns {
                    let nf: [u8; 32] = r.arr()?;
                    self.record_spend(&nf, &txs[tx], &mut tx_has_spend);
                }
                for i in 0..no {
                    let cmu: [u8; 32] = r.arr()?;
                    let epk: [u8; 32] = r.arr()?;
                    let ct: [u8; 52] = r.arr()?;
                    outputs += 1;
                    if self.sapling.is_none() {
                        continue;
                    }
                    let Some(cmu) = Option::from(
                        sapling_crypto::note::ExtractedNoteCommitment::from_bytes(&cmu),
                    ) else {
                        continue;
                    };
                    sapling.push((
                        SaplingDomain::new(Zip212Enforcement::On),
                        CompactOutputDescription {
                            ephemeral_key: EphemeralKeyBytes(epk),
                            cmu,
                            enc_ciphertext: ct,
                        },
                    ));
                    sapling_meta.push(Meta {
                        tx,
                        output_index: i as u32,
                        position: 0,
                    });
                }
                for pool in 0..2 {
                    let n = if pool == 0 { na } else { ni };
                    for i in 0..n {
                        let nf: [u8; 32] = r.arr()?;
                        let cmx: [u8; 32] = r.arr()?;
                        let epk: [u8; 32] = r.arr()?;
                        let ct: [u8; 52] = r.arr()?;
                        outputs += 1;
                        self.record_spend(&nf, &txs[tx], &mut tx_has_spend);
                        if self.orchard.is_none() {
                            continue;
                        }
                        let (Some(nf), Some(cmx)) = (
                            Option::<OrchardNf>::from(OrchardNf::from_bytes(&nf)),
                            Option::<OrchardCmx>::from(OrchardCmx::from_bytes(&cmx)),
                        ) else {
                            continue;
                        };
                        let action = CompactAction::from_parts(nf, cmx, EphemeralKeyBytes(epk), ct);
                        let meta = Meta {
                            tx,
                            output_index: i as u32,
                            position: 0,
                        };
                        if pool == 0 {
                            orchard.push((OrchardDomain::for_compact_action(&action), action));
                            orchard_meta.push(meta);
                        } else {
                            ironwood.push((IronwoodDomain::for_compact_action(&action), action));
                            ironwood_meta.push(meta);
                        }
                    }
                }
            }
            // Every Sapling output of the block was pushed (when a Sapling key is scanned).
            let n_block = (sapling.len() - block_sapling_start) as u64;
            let start = (sapling_size_end as u64).saturating_sub(n_block);
            for (k, m) in sapling_meta[block_sapling_start..].iter_mut().enumerate() {
                m.position = start + k as u64;
            }
        }

        let mut notes = Vec::new();
        let tx_of = |m: &Meta| &txs[m.tx];

        if let Some(keys) = &self.sapling {
            let ivks: Vec<SaplingPivk> = keys.ivks.iter().map(|(_, k)| k.clone()).collect();
            for (i, res) in batch::try_compact_note_decryption(&ivks, &sapling)
                .into_iter()
                .enumerate()
            {
                let Some(((note, _addr), k)) = res else {
                    continue;
                };
                let scope = keys.ivks[k].0;
                let m = sapling_meta[i];
                let nf = keys.fvk.as_ref().map(|fvk| {
                    hex::encode(note.nf(&fvk.to_nk(sapling_scope(scope)), m.position).0)
                });
                let t = tx_of(&m);
                notes.push(FoundNote {
                    txid: txid_display(&t.txid),
                    height: t.height,
                    time: t.time,
                    tx_index: t.index,
                    pool: "sapling",
                    output_index: m.output_index,
                    value: note.value().inner(),
                    scope: scope_name(scope),
                    nf,
                });
            }
        }

        if let Some(keys) = &self.orchard {
            let ivks: Vec<OrchardPivk> = keys.ivks.iter().map(|(_, k)| k.clone()).collect();
            macro_rules! decrypt {
                ($outputs:expr, $meta:expr, $pool:expr) => {
                    for (i, res) in batch::try_compact_note_decryption(&ivks, &$outputs)
                        .into_iter()
                        .enumerate()
                    {
                        let Some(((note, _addr), k)) = res else {
                            continue;
                        };
                        let scope = keys.ivks[k].0;
                        let m = $meta[i];
                        let t = tx_of(&m);
                        notes.push(FoundNote {
                            txid: txid_display(&t.txid),
                            height: t.height,
                            time: t.time,
                            tx_index: t.index,
                            pool: $pool,
                            output_index: m.output_index,
                            value: note.value().inner(),
                            scope: scope_name(scope),
                            nf: keys
                                .fvk
                                .as_ref()
                                .map(|fvk| hex::encode(note.nullifier(fvk).to_bytes())),
                        });
                    }
                };
            }
            decrypt!(orchard, orchard_meta, "orchard");
            decrypt!(ironwood, ironwood_meta, "ironwood");
        }

        Ok(ScanResult {
            notes,
            first_height: if blocks == 0 { 0 } else { first },
            last_height: last,
            blocks,
            outputs,
        })
    }

    fn decrypt_outputs(&self, o: &TxOutputs) -> Vec<Decrypted> {
        let mut found = Vec::new();
        if let Some(keys) = &self.sapling {
            for (i, out) in o.sapling.iter().enumerate() {
                let Some(full) = FullOutput::parse(out) else {
                    continue;
                };
                let domain = SaplingDomain::new(Zip212Enforcement::On);
                let mut hit = None;
                for (scope, ivk) in &keys.ivks {
                    if let Some((note, _addr, memo)) = try_note_decryption(&domain, ivk, &full) {
                        hit = Some((
                            if *scope == Scope::External {
                                "received"
                            } else {
                                "change"
                            },
                            note,
                            memo,
                            None,
                        ));
                        break;
                    }
                }
                if hit.is_none() {
                    if let Some(fvk) = &keys.fvk {
                        let cv: Option<sapling_crypto::value::ValueCommitment> =
                            sapling_crypto::value::ValueCommitment::from_bytes_not_small_order(
                                &full.cv,
                            )
                            .into();
                        if let Some(cv) = cv {
                            let ovk = fvk.fvk().ovk;
                            if let Some((note, addr, memo)) =
                                try_output_recovery_with_ovk(&domain, &ovk, &full, &cv, &full.out)
                            {
                                let to =
                                    ZcashAddress::from_sapling(self.net_type(), addr.to_bytes())
                                        .encode();
                                hit = Some(("sent", note, memo, Some(to)));
                            }
                        }
                    }
                }
                if let Some((kind, note, memo, to)) = hit {
                    let receipt = match (o.txid.as_deref(), note.rseed()) {
                        (Some(txid), sapling_crypto::Rseed::AfterZip212(r)) => Receipt {
                            network: self.network,
                            pool: Pool::Sapling,
                            txid: txid.to_string(),
                            index: i as u16,
                            addr: note.recipient().to_bytes(),
                            rseed: *r,
                        }
                        .encode(),
                        // Pre-ZIP-212 notes carry rcm, not a seed: no receipt.
                        _ => None,
                    };
                    found.push(Decrypted {
                        pool: "sapling",
                        index: i as u32,
                        kind,
                        value: note.value().inner(),
                        memo: memo_text(&memo),
                        to,
                        receipt,
                    });
                }
            }
        }
        if let Some(keys) = &self.orchard {
            for (pool, list) in [("orchard", &o.orchard), ("ironwood", &o.ironwood)] {
                for (i, out) in list.iter().enumerate() {
                    let Some((full, compact)) = FullOutput::parse_action(out) else {
                        continue;
                    };
                    let hit = if pool == "orchard" {
                        let d = OrchardDomain::for_compact_action(&compact);
                        self.orchard_full(keys, &d, &full)
                    } else {
                        let d = IronwoodDomain::for_compact_action(&compact);
                        self.orchard_full(keys, &d, &full)
                    };
                    if let Some((kind, note, memo, to)) = hit {
                        let receipt = o.txid.as_deref().and_then(|txid| {
                            Receipt {
                                network: self.network,
                                pool: if pool == "orchard" {
                                    Pool::Orchard
                                } else {
                                    Pool::Ironwood
                                },
                                txid: txid.to_string(),
                                index: i as u16,
                                addr: note.recipient().to_raw_address_bytes(),
                                rseed: *note.rseed().as_bytes(),
                            }
                            .encode()
                        });
                        found.push(Decrypted {
                            pool,
                            index: i as u32,
                            kind,
                            value: note.value().inner(),
                            memo: memo_text(&memo),
                            to,
                            receipt,
                        });
                    }
                }
            }
        }
        found
    }

    fn record_spend(&mut self, nf: &[u8; 32], tx: &Tx, tx_has_spend: &mut bool) {
        if !*tx_has_spend {
            self.spend_txs.push((tx.txid, tx.height, tx.time));
            *tx_has_spend = true;
        }
        let r = (self.spend_txs.len() - 1) as u32;
        self.spends.insert(nf[..8].try_into().expect("8 bytes"), r);
    }

    fn net_type(&self) -> Network {
        self.network
    }

    fn orchard_full<V>(
        &self,
        keys: &OrchardKeys,
        domain: &orchard::note_encryption::NoteEncryptionDomain<V>,
        full: &FullOutput,
    ) -> Option<(&'static str, orchard::Note, [u8; 512], Option<String>)>
    where
        orchard::note_encryption::NoteEncryptionDomain<V>: zcash_note_encryption::Domain<
            IncomingViewingKey = OrchardPivk,
            OutgoingViewingKey = orchard::keys::OutgoingViewingKey,
            ValueCommitment = orchard::value::ValueCommitment,
            Note = orchard::Note,
            Recipient = orchard::Address,
            Memo = [u8; 512],
            ExtractedCommitmentBytes = [u8; 32],
        >,
    {
        for (scope, ivk) in &keys.ivks {
            if let Some((note, _addr, memo)) = try_note_decryption(domain, ivk, full) {
                let kind = if *scope == Scope::External {
                    "received"
                } else {
                    "change"
                };
                return Some((kind, note, memo, None));
            }
        }
        let fvk = keys.fvk.as_ref()?;
        let cv: Option<orchard::value::ValueCommitment> =
            orchard::value::ValueCommitment::from_bytes(&full.cv).into();
        let cv = cv?;
        for scope in [Scope::External, Scope::Internal] {
            let ovk = fvk.to_ovk(scope);
            if let Some((note, addr, memo)) =
                try_output_recovery_with_ovk(domain, &ovk, full, &cv, &full.out)
            {
                let ua = unified::Address::try_from_items(vec![unified::Receiver::Orchard(
                    addr.to_raw_address_bytes(),
                )])
                .ok()?;
                let to = ZcashAddress::from_unified(self.net_type(), ua).encode();
                // Change to our own internal address is already reported by the IVK pass.
                return Some(("sent", note, memo, Some(to)));
            }
        }
        None
    }
}

/* ------------------------------------------------------------------ native */

/// The same scanner for native Rust (ZScan's treasury indexer), so the server and the browser
/// find exactly the same notes.
#[cfg(not(target_arch = "wasm32"))]
pub mod native {
    pub use super::{Decrypted, FoundNote, ScanResult, Spend};

    pub struct Key(super::Scanner);

    impl Key {
        /// Parses a viewing key (refuses spending keys and seed phrases).
        pub fn parse(key: &str) -> Result<Key, String> {
            super::Scanner::parse(key).map(Key)
        }
        /// "mainnet", "testnet" or "regtest".
        pub fn network(&self) -> &'static str {
            super::net_name(self.0.network)
        }
        /// A full viewing key: finds spends and outgoing payments too.
        pub fn is_full(&self) -> bool {
            self.0.orchard.as_ref().map_or(true, |o| o.fvk.is_some())
                && self.0.sapling.as_ref().map_or(true, |s| s.fvk.is_some())
        }
        /// Trial-decrypts one `/v1/compact` payload ("ZSC1" layout).
        pub fn scan(&mut self, data: &[u8]) -> Result<ScanResult, String> {
            self.0.scan_blocks(data)
        }
        /// Transactions among the scanned blocks that spent these nullifiers (hex).
        pub fn spent_by(&self, nullifiers: Vec<String>) -> Vec<Spend> {
            self.0.find_spends(nullifiers)
        }
        pub fn clear_spends(&mut self) {
            self.0.spends.clear();
            self.0.spend_txs.clear();
        }
        /// Decrypts a transaction's full outputs, given as the JSON of
        /// `GET /v1/tx/{txid}/shielded`; receipts are included when `txid` is set.
        pub fn decrypt_tx(&self, outputs_json: &str, txid: Option<&str>) -> Result<Vec<Decrypted>, String> {
            let mut o: super::TxOutputs = serde_json::from_str(outputs_json).map_err(|e| e.to_string())?;
            if txid.is_some() {
                o.txid = txid.map(String::from);
            }
            Ok(self.0.decrypt_outputs(&o))
        }
    }
}


/* ------------------------------------------------------------------ receipts */

// A receipt proves one shielded output (recipient, value, memo) to anyone, without a viewing key.
// It carries the note's recipient address and `rseed`: the verifier rebuilds `esk` and `pk_d`
// from them, decrypts the on-chain ciphertext and checks it against the note commitment
// (`try_output_recovery_with_pkd_esk`). Nothing else about the wallet is revealed: the
// nullifier needs the key's `nk`, so the receipt doesn't tell whether the note was spent.
//
// Binary layout (then base64url, no padding):
//   version u8 = 1 | network u8 (0 main, 1 test) | pool u8 (0 sapling, 1 orchard, 2 ironwood)
//   | output index u16 LE | txid [32] (display order) | address [43] | rseed [32]

const RECEIPT_VERSION: u8 = 1;
const RECEIPT_LEN: usize = 1 + 1 + 1 + 2 + 32 + 43 + 32;

#[derive(Clone, Copy, PartialEq, Debug)]
enum Pool {
    Sapling,
    Orchard,
    Ironwood,
}

impl Pool {
    fn name(self) -> &'static str {
        match self {
            Pool::Sapling => "sapling",
            Pool::Orchard => "orchard",
            Pool::Ironwood => "ironwood",
        }
    }
}

#[derive(Debug, PartialEq)]
struct Receipt {
    network: Network,
    pool: Pool,
    txid: String,
    index: u16,
    addr: [u8; 43],
    rseed: [u8; 32],
}

impl Receipt {
    fn encode(&self) -> Option<String> {
        let txid: [u8; 32] = hex::decode(&self.txid).ok()?.try_into().ok()?;
        let mut b = Vec::with_capacity(RECEIPT_LEN);
        b.push(RECEIPT_VERSION);
        b.push(match self.network {
            Network::Main => 0,
            _ => 1,
        });
        b.push(self.pool as u8);
        b.extend_from_slice(&self.index.to_le_bytes());
        b.extend_from_slice(&txid);
        b.extend_from_slice(&self.addr);
        b.extend_from_slice(&self.rseed);
        Some(b64url_encode(&b))
    }

    fn decode(token: &str) -> Result<Receipt, String> {
        let bad = || String::from("This receipt is malformed or incomplete. Check that the whole link was copied.");
        let b = b64url_decode(token.trim()).ok_or_else(bad)?;
        if b.len() != RECEIPT_LEN {
            return Err(bad());
        }
        if b[0] != RECEIPT_VERSION {
            return Err(String::from("This receipt was made by a newer version of ZScan Proof."));
        }
        let network = match b[1] {
            0 => Network::Main,
            1 => Network::Test,
            _ => return Err(bad()),
        };
        let pool = match b[2] {
            0 => Pool::Sapling,
            1 => Pool::Orchard,
            2 => Pool::Ironwood,
            _ => return Err(bad()),
        };
        Ok(Receipt {
            network,
            pool,
            index: u16::from_le_bytes([b[3], b[4]]),
            txid: hex::encode(&b[5..37]),
            addr: b[37..80].try_into().expect("length checked"),
            rseed: b[80..112].try_into().expect("length checked"),
        })
    }
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn b64url_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let n = chunk.iter().enumerate().fold(0u32, |n, (i, &c)| n | (c as u32) << (16 - 8 * i));
        for i in 0..=chunk.len() {
            out.push(B64[(n >> (18 - 6 * i) & 63) as usize] as char);
        }
    }
    out
}

fn b64url_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let (mut acc, mut bits) = (0u32, 0u32);
    for c in s.bytes() {
        let v = B64.iter().position(|&x| x == c)? as u32;
        acc = acc << 6 | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits & 0xff) as u8);
        }
    }
    Some(out)
}

#[derive(Serialize)]
struct ReceiptHeader {
    network: &'static str,
    pool: &'static str,
    txid: String,
    index: u16,
}

#[derive(Serialize)]
struct VerifiedReceipt {
    network: &'static str,
    pool: &'static str,
    txid: String,
    index: u16,
    value: u64,
    /// The recipient, as a Sapling address or a Unified Address with one shielded receiver.
    recipient: String,
    memo: Option<String>,
}

/// What a receipt points at (network, pool, txid, output), to fetch that transaction's outputs
/// before `verify_receipt`. Errors on a malformed receipt.
#[wasm_bindgen]
pub fn parse_receipt(token: &str) -> Result<JsValue, JsError> {
    let r = Receipt::decode(token).map_err(|e| JsError::new(&e))?;
    Ok(serde_wasm_bindgen::to_value(&ReceiptHeader {
        network: net_name(r.network),
        pool: r.pool.name(),
        txid: r.txid,
        index: r.index,
    })?)
}

/// Checks a receipt against the transaction's outputs (`GET /v1/tx/{txid}/shielded`) and returns
/// what it proves: value, recipient and memo. Errors when the receipt doesn't match the chain.
#[wasm_bindgen]
pub fn verify_receipt(token: &str, outputs: JsValue) -> Result<JsValue, JsError> {
    let o: TxOutputs = serde_wasm_bindgen::from_value(outputs)?;
    let r = Receipt::decode(token).map_err(|e| JsError::new(&e))?;
    let v = check_receipt(&r, &o).map_err(|e| JsError::new(&e))?;
    Ok(serde_wasm_bindgen::to_value(&v)?)
}

fn check_receipt(r: &Receipt, o: &TxOutputs) -> Result<VerifiedReceipt, String> {
    let no_output = || format!("Transaction {} has no {} output #{}.", r.txid, r.pool.name(), r.index);
    let mismatch = || String::from("This receipt does not match the transaction on chain: it was altered or made up.");
    let i = r.index as usize;
    let (value, recipient, memo) = match r.pool {
        Pool::Sapling => {
            let out = o.sapling.get(i).ok_or_else(no_output)?;
            let full = FullOutput::parse(out).ok_or_else(no_output)?;
            let addr = sapling_crypto::PaymentAddress::from_bytes(&r.addr).ok_or_else(mismatch)?;
            let note = sapling_crypto::Note::from_parts(
                addr,
                sapling_crypto::value::NoteValue::from_raw(0),
                sapling_crypto::Rseed::AfterZip212(r.rseed),
            );
            let esk = SaplingDomain::derive_esk(&note).ok_or_else(mismatch)?;
            let domain = SaplingDomain::new(Zip212Enforcement::On);
            let (note, to, memo) =
                try_output_recovery_with_pkd_esk(&domain, SaplingDomain::get_pk_d(&note), esk, &full)
                    .ok_or_else(mismatch)?;
            let to = ZcashAddress::from_sapling(r.network, to.to_bytes()).encode();
            (note.value().inner(), to, memo)
        }
        Pool::Orchard | Pool::Ironwood => {
            let list = if r.pool == Pool::Orchard { &o.orchard } else { &o.ironwood };
            let out = list.get(i).ok_or_else(no_output)?;
            let (full, compact) = FullOutput::parse_action(out).ok_or_else(no_output)?;
            let addr = Option::<orchard::Address>::from(orchard::Address::from_raw_address_bytes(&r.addr))
                .ok_or_else(mismatch)?;
            let rho = compact.rho();
            let rseed = Option::<RandomSeed>::from(RandomSeed::from_bytes(r.rseed, &rho)).ok_or_else(mismatch)?;
            let version = if r.pool == Pool::Orchard { NoteVersion::V2 } else { NoteVersion::V3 };
            let note = Option::<orchard::Note>::from(orchard::Note::from_parts(
                addr,
                orchard::value::NoteValue::from_raw(0),
                rho,
                rseed,
                version,
            ))
            .ok_or_else(mismatch)?;
            let found = if r.pool == Pool::Orchard {
                let d = OrchardDomain::for_compact_action(&compact);
                let esk = OrchardDomain::derive_esk(&note).ok_or_else(mismatch)?;
                try_output_recovery_with_pkd_esk(&d, OrchardDomain::get_pk_d(&note), esk, &full)
            } else {
                let d = IronwoodDomain::for_compact_action(&compact);
                let esk = IronwoodDomain::derive_esk(&note).ok_or_else(mismatch)?;
                try_output_recovery_with_pkd_esk(&d, IronwoodDomain::get_pk_d(&note), esk, &full)
            };
            let (note, to, memo) = found.ok_or_else(mismatch)?;
            let ua = unified::Address::try_from_items(vec![unified::Receiver::Orchard(to.to_raw_address_bytes())])
                .map_err(|_| mismatch())?;
            (note.value().inner(), ZcashAddress::from_unified(r.network, ua).encode(), memo)
        }
    };
    Ok(VerifiedReceipt {
        network: net_name(r.network),
        pool: r.pool.name(),
        txid: r.txid.clone(),
        index: r.index,
        value,
        recipient,
        memo: memo_text(&memo),
    })
}

/* ------------------------------------------------------------------ full outputs */

#[derive(serde::Deserialize)]
struct TxOutputs {
    /// Set by the caller to get a receipt for each decrypted output.
    #[serde(default)]
    txid: Option<String>,
    sapling: Vec<OutputHex>,
    orchard: Vec<OutputHex>,
    ironwood: Vec<OutputHex>,
}

#[derive(serde::Deserialize)]
struct OutputHex {
    nullifier: Option<String>,
    cm: String,
    cv: String,
    epk: String,
    enc_ciphertext: String,
    out_ciphertext: String,
}

struct FullOutput {
    cm: [u8; 32],
    cv: [u8; 32],
    epk: [u8; 32],
    enc: [u8; ENC_CIPHERTEXT_SIZE],
    out: [u8; 80],
}

impl FullOutput {
    fn parse(o: &OutputHex) -> Option<Self> {
        fn arr<const N: usize>(h: &str) -> Option<[u8; N]> {
            hex::decode(h).ok()?.try_into().ok()
        }
        Some(FullOutput {
            cm: arr(&o.cm)?,
            cv: arr(&o.cv)?,
            epk: arr(&o.epk)?,
            enc: arr(&o.enc_ciphertext)?,
            out: arr(&o.out_ciphertext)?,
        })
    }
}

impl FullOutput {
    /// An Orchard or Ironwood action: its full ciphertexts and the compact form that carries
    /// `rho` (the action's nullifier).
    fn parse_action(o: &OutputHex) -> Option<(Self, CompactAction)> {
        let full = FullOutput::parse(o)?;
        let nf: [u8; 32] = hex::decode(o.nullifier.as_deref()?).ok()?.try_into().ok()?;
        let nf = Option::<OrchardNf>::from(OrchardNf::from_bytes(&nf))?;
        let cmx = Option::<OrchardCmx>::from(OrchardCmx::from_bytes(&full.cm))?;
        let mut prefix = [0u8; 52];
        prefix.copy_from_slice(&full.enc[..52]);
        let compact = CompactAction::from_parts(nf, cmx, EphemeralKeyBytes(full.epk), prefix);
        Some((full, compact))
    }
}

impl ShieldedOutput<SaplingDomain, ENC_CIPHERTEXT_SIZE> for FullOutput {
    fn ephemeral_key(&self) -> EphemeralKeyBytes {
        EphemeralKeyBytes(self.epk)
    }
    fn cmstar_bytes(&self) -> [u8; 32] {
        self.cm
    }
    fn enc_ciphertext(&self) -> &[u8; ENC_CIPHERTEXT_SIZE] {
        &self.enc
    }
}

impl<V> ShieldedOutput<orchard::note_encryption::NoteEncryptionDomain<V>, ENC_CIPHERTEXT_SIZE>
    for FullOutput
where
    orchard::note_encryption::NoteEncryptionDomain<V>:
        zcash_note_encryption::Domain<ExtractedCommitmentBytes = [u8; 32]>,
{
    fn ephemeral_key(&self) -> EphemeralKeyBytes {
        EphemeralKeyBytes(self.epk)
    }
    fn cmstar_bytes(&self) -> [u8; 32] {
        self.cm
    }
    fn enc_ciphertext(&self) -> &[u8; ENC_CIPHERTEXT_SIZE] {
        &self.enc
    }
}

#[derive(Serialize)]
pub struct Decrypted {
    pub pool: &'static str,
    pub index: u32,
    /// "received", "change" or "sent".
    pub kind: &'static str,
    pub value: u64,
    pub memo: Option<String>,
    /// Recipient of a payment this key sent.
    pub to: Option<String>,
    /// Receipt proving this output (see `verify_receipt`), when the caller passed the txid.
    pub receipt: Option<String>,
}

/// ZIP 302 memo: text when the first byte is ≤ 0xF4, nothing for the empty memo (0xF6),
/// otherwise a short hex preview of arbitrary data.
fn memo_text(memo: &[u8; 512]) -> Option<String> {
    match memo[0] {
        0xF6 => None,
        b if b <= 0xF4 => {
            let end = memo.iter().rposition(|&c| c != 0).map_or(0, |i| i + 1);
            let text = String::from_utf8_lossy(&memo[..end]).trim().to_string();
            (!text.is_empty()).then_some(text)
        }
        _ => Some(format!("(binary memo) 0x{}…", hex::encode(&memo[..16]))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchard::keys::SpendingKey;
    use orchard::note::Rho;
    use orchard::note_encryption::IronwoodNoteEncryption;
    use orchard::value::NoteValue;
    use zcash_note_encryption::Domain;

    fn fvk() -> OrchardFvk {
        let sk = Option::<SpendingKey>::from(SpendingKey::from_bytes([7; 32])).unwrap();
        OrchardFvk::from(&sk)
    }

    fn ufvk(fvk: &OrchardFvk) -> String {
        unified::Ufvk::try_from_items(vec![unified::Fvk::Orchard(fvk.to_bytes())])
            .unwrap()
            .encode(&Network::Main)
    }

    fn put_block(out: &mut Vec<u8>, height: u32, txs: &[([u8; 32], Vec<[u8; 148]>)]) {
        out.extend_from_slice(&height.to_le_bytes());
        out.extend_from_slice(&1_790_000_000u32.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(&(txs.len() as u32).to_le_bytes());
        for (i, (txid, actions)) in txs.iter().enumerate() {
            out.extend_from_slice(txid);
            out.extend_from_slice(&(i as u32 + 1).to_le_bytes());
            for n in [0u16, 0, 0, actions.len() as u16] {
                out.extend_from_slice(&n.to_le_bytes());
            }
            for a in actions {
                out.extend_from_slice(a);
            }
        }
    }

    /// An Ironwood note sent to the key is found with its value, its nullifier matches a later
    /// spend, and the full ciphertext yields the memo.
    #[test]
    fn finds_ironwood_note_spend_and_memo() {
        let fvk = fvk();
        let recipient = fvk.address_at(0u32, Scope::External);
        let old_nf = [1u8; 32];
        let mut nf_bytes = [0u8; 32];
        nf_bytes[0] = 1;
        let nf = Option::<OrchardNf>::from(OrchardNf::from_bytes(&nf_bytes)).unwrap();
        let dummy = CompactAction::from_parts(
            nf,
            Option::from(OrchardCmx::from_bytes(&[0; 32])).unwrap(),
            EphemeralKeyBytes([0; 32]),
            [0; 52],
        );
        let rho: Rho = dummy.rho();
        let rseed = Option::<RandomSeed>::from(RandomSeed::from_bytes([3; 32], &rho)).unwrap();
        let note = Option::<orchard::Note>::from(orchard::Note::from_parts(
            recipient,
            NoteValue::from_raw(123_456_789),
            rho,
            rseed,
            NoteVersion::V3,
        ))
        .unwrap();
        let mut memo = [0u8; 512];
        memo[..11].copy_from_slice(b"hello zscan");
        let enc = IronwoodNoteEncryption::new(None, note, memo);
        let ct = enc.encrypt_note_plaintext();
        let epk = IronwoodDomain::epk_bytes(enc.epk()).0;
        let cmx = OrchardCmx::from(note.commitment()).to_bytes();

        let mut action = [0u8; 148];
        action[..32].copy_from_slice(&nf_bytes);
        action[32..64].copy_from_slice(&cmx);
        action[64..96].copy_from_slice(&epk);
        action[96..].copy_from_slice(&ct[..52]);
        // A later transaction spends the note (its nullifier appears as an action nullifier).
        let own_nf = note.nullifier(&fvk).to_bytes();
        let mut spend = [0u8; 148];
        spend[..32].copy_from_slice(&own_nf);
        spend[32..64].copy_from_slice(&[0u8; 32]);
        let _ = old_nf;

        let mut data = b"ZSC1".to_vec();
        put_block(&mut data, 100, &[([9; 32], vec![action])]);
        put_block(&mut data, 101, &[([8; 32], vec![spend])]);

        let key = ufvk(&fvk);
        let mut s = Scanner::parse(&key).unwrap();
        let res = s.scan_blocks(&data).unwrap();
        assert_eq!(res.blocks, 2);
        assert_eq!(res.notes.len(), 1, "one note found");
        let n = &res.notes[0];
        assert_eq!(
            (n.pool, n.value, n.scope, n.height),
            ("ironwood", 123_456_789, "external", 100)
        );
        assert_eq!(n.nf.as_deref(), Some(hex::encode(own_nf).as_str()));

        let spends = s.find_spends(vec![hex::encode(own_nf)]);
        assert_eq!(spends.len(), 1);
        assert_eq!(
            (spends[0].height, spends[0].txid.as_str()),
            (101, hex::encode([8u8; 32]).as_str())
        );

        let outputs = TxOutputs {
            txid: None,
            sapling: vec![],
            orchard: vec![],
            ironwood: vec![OutputHex {
                nullifier: Some(hex::encode(nf_bytes)),
                cm: hex::encode(cmx),
                cv: hex::encode([0u8; 32]),
                epk: hex::encode(epk),
                enc_ciphertext: hex::encode(ct),
                out_ciphertext: hex::encode([0u8; 80]),
            }],
        };
        let d = s.decrypt_outputs(&outputs);
        assert_eq!(d.len(), 1);
        assert_eq!(
            (d[0].kind, d[0].value, d[0].memo.as_deref()),
            ("received", 123_456_789, Some("hello zscan"))
        );
    }

    /// Deterministic RNG for the outgoing ciphertext (its randomness only pads `ock` output).
    struct TestRng(u8);
    impl rand_core::RngCore for TestRng {
        fn next_u32(&mut self) -> u32 {
            self.0 = self.0.wrapping_add(1);
            self.0 as u32
        }
        fn next_u64(&mut self) -> u64 {
            self.next_u32() as u64
        }
        fn fill_bytes(&mut self, dest: &mut [u8]) {
            for b in dest {
                *b = self.next_u32() as u8;
            }
        }
        fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand_core::Error> {
            self.fill_bytes(dest);
            Ok(())
        }
    }

    /// One Orchard or Ironwood action paying `value` with `memo` to `recipient`, with a real
    /// outgoing ciphertext for `sender_ovk`, as `/v1/tx/{txid}/shielded` would serve it.
    fn action_to(
        pool: Pool,
        recipient: orchard::Address,
        sender_ovk: Option<orchard::keys::OutgoingViewingKey>,
        value: u64,
        memo_text: &[u8],
    ) -> OutputHex {
        use orchard::note_encryption::OrchardNoteEncryption;
        use orchard::value::{ValueCommitTrapdoor, ValueCommitment};
        let mut nf_bytes = [0u8; 32];
        nf_bytes[0] = 5;
        let nf = Option::<OrchardNf>::from(OrchardNf::from_bytes(&nf_bytes)).unwrap();
        let dummy = CompactAction::from_parts(
            nf,
            Option::from(OrchardCmx::from_bytes(&[0; 32])).unwrap(),
            EphemeralKeyBytes([0; 32]),
            [0; 52],
        );
        let rho: Rho = dummy.rho();
        let rseed = Option::<RandomSeed>::from(RandomSeed::from_bytes([11; 32], &rho)).unwrap();
        let version = if pool == Pool::Orchard { NoteVersion::V2 } else { NoteVersion::V3 };
        let note = Option::<orchard::Note>::from(orchard::Note::from_parts(
            recipient,
            NoteValue::from_raw(value),
            rho,
            rseed,
            version,
        ))
        .unwrap();
        let mut memo = [0u8; 512];
        memo[..memo_text.len()].copy_from_slice(memo_text);
        let cmx = OrchardCmx::from(note.commitment());
        let rcv = Option::<ValueCommitTrapdoor>::from(ValueCommitTrapdoor::from_bytes([1; 32])).unwrap();
        let cv = ValueCommitment::derive(
            NoteValue::from_raw(value) - NoteValue::from_raw(0),
            rcv,
        );
        let (ct, out, epk) = if pool == Pool::Orchard {
            let enc = OrchardNoteEncryption::new(sender_ovk, note, memo);
            (
                enc.encrypt_note_plaintext(),
                enc.encrypt_outgoing_plaintext(&cv, &cmx, &mut TestRng(0)),
                OrchardDomain::epk_bytes(enc.epk()).0,
            )
        } else {
            let enc = IronwoodNoteEncryption::new(sender_ovk, note, memo);
            (
                enc.encrypt_note_plaintext(),
                enc.encrypt_outgoing_plaintext(&cv, &cmx, &mut TestRng(0)),
                IronwoodDomain::epk_bytes(enc.epk()).0,
            )
        };
        OutputHex {
            nullifier: Some(hex::encode(nf_bytes)),
            cm: hex::encode(cmx.to_bytes()),
            cv: hex::encode(cv.to_bytes()),
            epk: hex::encode(epk),
            enc_ciphertext: hex::encode(ct),
            out_ciphertext: hex::encode(out),
        }
    }

    fn tx_with(pool: Pool, action: OutputHex) -> TxOutputs {
        let (mut orchard, mut ironwood) = (vec![], vec![]);
        if pool == Pool::Orchard { orchard.push(action) } else { ironwood.push(action) }
        TxOutputs { txid: Some(hex::encode([0xab; 32])), sapling: vec![], orchard, ironwood }
    }

    /// The recipient (incoming key) and the sender (outgoing key) can each make a receipt, and
    /// anyone can verify it against the transaction without any key.
    #[test]
    fn receipts_round_trip_for_recipient_and_sender() {
        let recipient_fvk = fvk();
        let to = recipient_fvk.address_at(0u32, Scope::External);
        let sk = Option::<SpendingKey>::from(SpendingKey::from_bytes([9; 32])).unwrap();
        let sender_fvk = OrchardFvk::from(&sk);
        for pool in [Pool::Orchard, Pool::Ironwood] {
            let tx = tx_with(pool, action_to(pool, to, Some(sender_fvk.to_ovk(Scope::External)), 250_000_000, b"invoice 42"));
            for key in [ufvk(&recipient_fvk), ufvk(&sender_fvk)] {
                let s = Scanner::parse(&key).unwrap();
                let d = s.decrypt_outputs(&tx);
                assert_eq!(d.len(), 1, "{pool:?}: output found");
                let token = d[0].receipt.as_deref().expect("receipt");
                let r = Receipt::decode(token).unwrap();
                assert_eq!((r.pool, r.index, r.txid.as_str()), (pool, 0, hex::encode([0xab; 32]).as_str()));
                let v = check_receipt(&r, &tx).unwrap();
                assert_eq!((v.value, v.memo.as_deref(), v.pool), (250_000_000, Some("invoice 42"), pool.name()));
                let want = unified::Address::try_from_items(vec![unified::Receiver::Orchard(to.to_raw_address_bytes())]).unwrap();
                assert_eq!(v.recipient, ZcashAddress::from_unified(Network::Main, want).encode());
            }
        }
    }

    /// A receipt fails once any byte of what it proves is changed, or against another output.
    #[test]
    fn altered_receipts_are_rejected() {
        let fvk = fvk();
        let to = fvk.address_at(0u32, Scope::External);
        let tx = tx_with(Pool::Ironwood, action_to(Pool::Ironwood, to, None, 1_000, b"hi"));
        let s = Scanner::parse(&ufvk(&fvk)).unwrap();
        let token = s.decrypt_outputs(&tx)[0].receipt.clone().unwrap();
        let good = Receipt::decode(&token).unwrap();
        assert!(check_receipt(&good, &tx).is_ok());

        let mut r = Receipt::decode(&token).unwrap();
        r.rseed[0] ^= 1;
        assert!(check_receipt(&r, &tx).is_err(), "altered rseed");

        let other = fvk.address_at(1u32, Scope::External);
        let mut r = Receipt::decode(&token).unwrap();
        r.addr = other.to_raw_address_bytes();
        assert!(check_receipt(&r, &tx).is_err(), "other recipient");

        let mut r = Receipt::decode(&token).unwrap();
        r.pool = Pool::Orchard;
        assert!(check_receipt(&r, &tx).is_err(), "wrong pool");

        let mut r = Receipt::decode(&token).unwrap();
        r.index = 1;
        assert!(check_receipt(&r, &tx).is_err(), "missing output");

        // Another action with the same recipient and seed but a different value.
        let forged = tx_with(Pool::Ironwood, action_to(Pool::Ironwood, to, None, 2_000, b"hi"));
        let v = check_receipt(&good, &forged).unwrap();
        assert_eq!(v.value, 2_000, "the value always comes from the chain, never the receipt");

        assert!(Receipt::decode("not-a-receipt").is_err());
        assert!(Receipt::decode(&token[..token.len() - 4]).is_err());
    }

    #[test]
    fn receipt_encoding_round_trips() {
        let r = Receipt {
            network: Network::Test,
            pool: Pool::Sapling,
            txid: hex::encode([7u8; 32]),
            index: 513,
            addr: [3; 43],
            rseed: [4; 32],
        };
        let token = r.encode().unwrap();
        assert_eq!(token.len(), 150);
        assert!(token.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'));
        assert_eq!(Receipt::decode(&token).unwrap(), r);
    }

    #[test]
    fn refuses_secrets_and_junk() {
        let seed = "abandon ".repeat(24);
        assert!(Scanner::parse(&seed).err().unwrap().contains("seed phrase"));
        assert!(Scanner::parse("secret-extended-key-main1qqq")
            .err()
            .unwrap()
            .contains("spending key"));
        assert!(Scanner::parse("t1abc")
            .err()
            .unwrap()
            .contains("Not a viewing key"));
        let fvk = fvk();
        let s = Scanner::parse(&ufvk(&fvk)).unwrap();
        assert!(s.orchard.as_ref().unwrap().fvk.is_some());
    }
}
