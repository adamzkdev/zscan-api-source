//! Zcash inscriptions ("Zerdinals" / Zordinals) and the NFT collections built on them.
//!
//! The envelope sits in the scriptSig of a transparent input (Zcash has no witness data):
//! `"ord" <total pieces> <content type>` then up to four `<piece index> <piece>` pairs in
//! descending order (index 0 is the end of the content), then the signature and redeem script.
//! Content of more than four pieces continues in reveal transactions that each spend output 0
//! of the previous one; continuations normally repeat the header, legacy ones may omit it.
//!
//! Ownership follows transparent outputs, not zatoshis (no Zcash implementation numbers
//! zatoshis): an inscription starts on the first non-OP_RETURN output of the transaction that
//! completes it and moves to the first such output of whatever spends it. With no such output
//! it ends as `shielded` (the spend has a shielded bundle) or `burned`; both are terminal.
//!
//! Most inscriptions are fungible-token bookkeeping (ZRC-20 JSON with a `tick`). Those keep
//! their sequence number but aren't indexed. NFTs come in three forms: content inscribed on
//! chain (images, SVG, HTML), ZRC-721 collection items (`{"p":"zrc-721","op":"mint",
//! "collection":"ZGODS","id":"9963"}`, with metadata on IPFS under the collection's deploy),
//! and IPFS pointers (`{"type":"Zerdinals_NFT","content":"<cid>","mime":"image/png"}`).
//!
//! References: Universe Zordinals v1 (github.com/bitcoinuniverseio/docs-zerdinals-and-zrunes)
//! and zord (github.com/cloutprotocol/zord).

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::parse::{ParsedTx, TxInput};

/// First block with an inscription envelope.
pub const FIRST_HEIGHT: u32 = 3_132_356;
/// Longest reveal chain followed (the v1 spec's parser bound).
const MAX_CHAIN: usize = 64;
const MAX_PIECE: usize = 240;

pub type OutPoint = (String, u32);

// ---------------------------------------------------------------------------------------------
// Envelope

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok<'a> {
    /// A data push; OP_0 is an empty push.
    Data(&'a [u8]),
    /// OP_1..OP_16.
    Small(u8),
    Op(u8),
}

/// Splits a script into pushes and opcodes. `None` if a push runs past the end.
fn tokens(script: &[u8]) -> Option<Vec<Tok<'_>>> {
    let mut out = Vec::new();
    let mut rest = script;
    while let Some((&op, tail)) = rest.split_first() {
        let (len, tail) = match op {
            0x00..=0x4b => (op as usize, tail),
            0x4c => (*tail.first()? as usize, &tail[1..]),
            0x4d => (
                u16::from_le_bytes([*tail.first()?, *tail.get(1)?]) as usize,
                tail.get(2..)?,
            ),
            0x51..=0x60 => {
                out.push(Tok::Small(op - 0x50));
                rest = tail;
                continue;
            }
            _ => {
                out.push(Tok::Op(op));
                rest = tail;
                continue;
            }
        };
        out.push(Tok::Data(tail.get(..len)?));
        rest = &tail[len..];
    }
    Some(out)
}

/// A piece count or index: OP_0..OP_16, or a one- or two-byte push (legacy encoders push
/// small numbers as single bytes, e.g. 122 as `z`).
fn number(t: &Tok) -> Option<u32> {
    match t {
        Tok::Small(n) => Some(*n as u32),
        Tok::Data([]) => Some(0),
        Tok::Data([b]) => Some(*b as u32),
        Tok::Data([lo, hi]) => Some(u16::from_le_bytes([*lo, *hi]) as u32),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    /// `(total pieces, content type)`; `None` for a legacy continuation without a header.
    pub header: Option<(u8, String)>,
    pub pieces: Vec<(u8, Vec<u8>)>,
}

/// Parses an inscription envelope from a scriptSig. Without the `ord` header this returns the
/// `<index> <piece>` pairs at the start of the script, which only mean something when the input
/// spends a reveal chain's output.
pub fn parse_envelope(script_sig: &[u8]) -> Option<Envelope> {
    let toks = tokens(script_sig)?;
    let mut i = 0;
    let header = if toks.first() == Some(&Tok::Data(b"ord")) {
        let total = number(toks.get(1)?)?;
        let Tok::Data(ct) = toks.get(2)? else {
            return None;
        };
        let ct = std::str::from_utf8(ct).ok()?;
        let valid = (1..=255).contains(&total)
            && (3..=96).contains(&ct.len())
            && ct.contains('/')
            && ct.bytes().all(|b| b.is_ascii_graphic() || b == b' ');
        if !valid {
            return None;
        }
        i = 3;
        Some((total as u8, ct.to_string()))
    } else {
        None
    };
    let mut pieces = Vec::new();
    while let (Some(idx), Some(Tok::Data(d))) = (toks.get(i).and_then(number), toks.get(i + 1)) {
        if idx > 254 || d.is_empty() || d.len() > MAX_PIECE {
            break;
        }
        pieces.push((idx as u8, d.to_vec()));
        i += 2;
    }
    (header.is_some() || !pieces.is_empty()).then_some(Envelope { header, pieces })
}

// ---------------------------------------------------------------------------------------------
// State

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Transparent,
    Shielded,
    Burned,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Transparent => "transparent",
            State::Shielded => "shielded",
            State::Burned => "burned",
        }
    }
}

/// A reveal chain waiting for its next transaction, keyed by the output it must spend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pending {
    pub id: String,
    pub genesis_height: u32,
    pub content_type: String,
    pub total: u8,
    /// Piece index -> bytes (hex).
    pub pieces: BTreeMap<u8, String>,
    pub reveal_txids: Vec<String>,
}

/// An inscription carried by a transparent output.
#[derive(Debug, Clone)]
pub struct Held {
    pub id: String,
    pub owner: Option<String>,
}

/// What an inscription's content is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    /// Content on chain; `media` from [`media_kind`].
    Media(&'static str),
    /// ZRC-721 collection deploy: `meta` is the IPFS directory holding `<id>.json`.
    Deploy {
        collection: String,
        supply: Option<i64>,
        meta: Option<String>,
    },
    /// ZRC-721 collection item.
    Item { collection: String, item: String },
    /// An image or file on IPFS (`Zerdinals_NFT`).
    Ipfs { cid: String, mime: String },
    /// Fungible-token bookkeeping (a JSON document with a `tick`): numbered, not indexed.
    Token,
}

impl Kind {
    /// Media family stored for filters and previews.
    pub fn media(&self) -> &'static str {
        match self {
            Kind::Media(m) => m,
            Kind::Deploy { .. } | Kind::Token => "json",
            Kind::Item { .. } => "collection",
            Kind::Ipfs { .. } => "ipfs",
        }
    }
}

fn json_str(v: &serde_json::Value, k: &str) -> Option<String> {
    match v.get(k)? {
        serde_json::Value::String(s) => Some(s.trim().to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
    .filter(|s| !s.is_empty() && s.len() <= 128)
}

/// Classifies inscribed content.
pub fn classify(content_type: &str, body: &[u8]) -> Kind {
    let media = media_kind(content_type);
    let json = std::str::from_utf8(body)
        .ok()
        .map(str::trim)
        .filter(|t| t.starts_with('{') && matches!(media, "json" | "text"))
        .and_then(|t| serde_json::from_str::<serde_json::Value>(t).ok())
        .filter(|v| v.is_object());
    let Some(v) = json else {
        return Kind::Media(media);
    };
    let p = json_str(&v, "p").unwrap_or_default().to_ascii_lowercase();
    if p == "zrc-721" {
        let collection = json_str(&v, "collection")
            .or_else(|| json_str(&v, "tick"))
            .map(|c| c.to_uppercase());
        match (json_str(&v, "op").as_deref(), collection) {
            (Some("deploy"), Some(collection)) => {
                return Kind::Deploy {
                    collection,
                    supply: json_str(&v, "supply").and_then(|s| s.parse().ok()),
                    // An IPFS folder, or the collection's own HTTPS host (checked when read).
                    meta: json_str(&v, "meta")
                        .filter(|m| is_cid_path(m) || m.starts_with("https://")),
                };
            }
            (Some("mint"), Some(collection)) => {
                if let Some(item) = json_str(&v, "id").filter(|i| i.len() <= 32) {
                    return Kind::Item { collection, item };
                }
            }
            _ => {}
        }
    }
    if json_str(&v, "type").as_deref() == Some("Zerdinals_NFT")
        || json_str(&v, "proto").as_deref() == Some("ipfs")
    {
        if let Some(cid) = json_str(&v, "content").filter(|c| is_cid_path(c)) {
            let mime = json_str(&v, "mime").unwrap_or_else(|| "image/png".into());
            return Kind::Ipfs { cid, mime };
        }
    }
    if v.get("tick").is_some() {
        return Kind::Token;
    }
    Kind::Media(media)
}

/// An IPFS CID, optionally followed by a path, safe to put in a gateway URL.
pub fn is_cid_path(s: &str) -> bool {
    let s = s.strip_prefix("ipfs://").unwrap_or(s);
    (s.starts_with("Qm") || s.starts_with("baf"))
        && s.len() <= 200
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'-' | b'_'))
        && !s.contains("..")
}

#[derive(Debug, Clone)]
pub struct Inscription {
    pub id: String,
    pub number: i64,
    pub genesis_height: u32,
    /// Completion transaction (carries piece 0).
    pub txid: String,
    pub height: u32,
    pub block_index: u32,
    pub time: i64,
    pub content_type: String,
    pub content: Vec<u8>,
    pub sha256: String,
    pub reveal_txids: Vec<String>,
    pub owner: Option<String>,
    pub location: Option<OutPoint>,
    pub state: State,
    pub kind: Kind,
}

/// Something that happened to an inscription: `inscribed`, `transfer`, `sale`, `shielded`,
/// `burned`.
#[derive(Debug, Clone)]
pub struct Event {
    pub inscription_id: String,
    pub kind: &'static str,
    /// Sales: what the seller was paid, zatoshis.
    pub price: Option<i64>,
    pub txid: String,
    pub height: u32,
    pub time: i64,
    pub from: Option<String>,
    pub to: Option<String>,
    pub location: Option<OutPoint>,
    pub state: State,
}

/// A ZRC-721 collection, defined by its first deploy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Collection {
    pub name: String,
    pub supply: Option<i64>,
    pub meta: Option<String>,
    pub deploy_inscription: String,
    pub deployer: Option<String>,
    pub height: u32,
    pub time: i64,
}

/// Rows to write since the last flush. `pending` names outputs whose current value is read from
/// the engine (absent = delete).
#[derive(Debug, Default)]
pub struct Changes {
    pub inscriptions: Vec<Inscription>,
    pub events: Vec<Event>,
    pub pending: HashSet<OutPoint>,
    pub collections: Vec<Collection>,
    pub failed_chains: u32,
    /// Token documents numbered but not indexed.
    pub skipped: u32,
}

impl Changes {
    pub fn is_empty(&self) -> bool {
        self.inscriptions.is_empty()
            && self.events.is_empty()
            && self.pending.is_empty()
            && self.collections.is_empty()
            && self.skipped == 0
    }
}

/// Inscription state, applied transaction by transaction in chain order.
#[derive(Debug, Default)]
pub struct Engine {
    pub next_number: i64,
    pub locations: HashMap<OutPoint, Vec<Held>>,
    pub pending: HashMap<OutPoint, Pending>,
    /// Deployed collection names.
    pub collections: HashSet<String>,
    pub out: Changes,
}

fn outpoint(i: &TxInput) -> Option<OutPoint> {
    Some((i.prev_txid.clone()?, i.prev_n?))
}

/// Where an inscription spent (or completed) in `tx` lands: the first transparent output that
/// isn't OP_RETURN, with its address.
fn successor(tx: &ParsedTx) -> Option<(u32, Option<String>)> {
    tx.outputs
        .iter()
        .find(|o| o.op_return.is_none())
        .map(|o| (o.n, o.address.clone()))
}

fn terminal(tx: &ParsedTx) -> State {
    if tx.has_shielded() {
        State::Shielded
    } else {
        State::Burned
    }
}

struct Ctx<'a> {
    tx: &'a ParsedTx,
    height: u32,
    block_index: u32,
    time: i64,
}

impl Engine {
    pub fn new() -> Self {
        Self {
            next_number: 1,
            ..Default::default()
        }
    }

    /// Applies one transaction. Call for every transaction with transparent inputs, in block
    /// order (others can neither spend an inscription nor carry an envelope).
    pub fn apply_tx(&mut self, height: u32, time: i64, block_index: u32, tx: &ParsedTx) {
        let c = Ctx {
            tx,
            height,
            block_index,
            time,
        };
        self.move_spent(&c);

        // Continuations: an input spending a waiting reveal chain's output.
        let mut continued = false;
        for input in &tx.inputs {
            let Some(prev) = outpoint(input) else {
                continue;
            };
            let Some(mut p) = self.pending.remove(&prev) else {
                continue;
            };
            self.out.pending.insert(prev);
            continued = true;
            let env = parse_envelope(&input.script_sig).filter(|e| !e.pieces.is_empty());
            let Some(env) = env else {
                self.out.failed_chains += 1;
                continue;
            };
            for (idx, piece) in env.pieces {
                if idx < p.total {
                    p.pieces.entry(idx).or_insert_with(|| hex::encode(piece));
                }
            }
            p.reveal_txids.push(tx.txid.clone());
            self.advance(p, &c);
        }
        if continued {
            return;
        }

        // Genesis: the first input carrying an `ord` header.
        let Some((total, content_type, pieces)) = tx.inputs.iter().find_map(|i| {
            let e = parse_envelope(&i.script_sig)?;
            let (total, ct) = e.header?;
            Some((total, ct, e.pieces))
        }) else {
            return;
        };
        let p = Pending {
            id: format!("{}i0", tx.txid),
            genesis_height: height,
            content_type,
            total,
            pieces: pieces
                .into_iter()
                .filter(|(i, _)| *i < total)
                .map(|(i, d)| (i, hex::encode(d)))
                .collect(),
            reveal_txids: vec![tx.txid.clone()],
        };
        self.advance(p, &c);
    }

    /// Completes a reveal chain, or leaves it waiting on output 0 of this transaction.
    fn advance(&mut self, p: Pending, c: &Ctx) {
        if p.pieces.len() == p.total as usize {
            self.complete(p, c);
        } else if p.reveal_txids.len() >= MAX_CHAIN || c.tx.outputs.is_empty() {
            self.out.failed_chains += 1;
        } else {
            let key = (c.tx.txid.clone(), 0);
            self.out.pending.insert(key.clone());
            self.pending.insert(key, p);
        }
    }

    fn complete(&mut self, p: Pending, c: &Ctx) {
        let content: Vec<u8> = p
            .pieces
            .values()
            .rev()
            .flat_map(|h| hex::decode(h).unwrap_or_default())
            .collect();
        let number = self.next_number;
        self.next_number += 1;
        let kind = classify(&p.content_type, &content);
        if kind == Kind::Token {
            self.out.skipped += 1;
            return;
        }
        let (state, owner, location) = match successor(c.tx) {
            Some((n, addr)) => (State::Transparent, addr, Some((c.tx.txid.clone(), n))),
            None => (terminal(c.tx), None, None),
        };
        if let Some(loc) = &location {
            self.locations.entry(loc.clone()).or_default().push(Held {
                id: p.id.clone(),
                owner: owner.clone(),
            });
        }
        if let Kind::Deploy {
            collection,
            supply,
            meta,
        } = &kind
        {
            // The first deploy of a name defines the collection.
            if self.collections.insert(collection.clone()) {
                self.out.collections.push(Collection {
                    name: collection.clone(),
                    supply: *supply,
                    meta: meta.clone(),
                    deploy_inscription: p.id.clone(),
                    deployer: owner.clone(),
                    height: c.height,
                    time: c.time,
                });
            }
        }
        self.out.events.push(Event {
            inscription_id: p.id.clone(),
            kind: "inscribed",
            price: None,
            txid: c.tx.txid.clone(),
            height: c.height,
            time: c.time,
            from: None,
            to: owner.clone(),
            location: location.clone(),
            state,
        });
        self.out.inscriptions.push(Inscription {
            id: p.id,
            number,
            genesis_height: p.genesis_height,
            txid: c.tx.txid.clone(),
            height: c.height,
            block_index: c.block_index,
            time: c.time,
            sha256: hex::encode(Sha256::digest(&content)),
            content_type: p.content_type,
            content,
            reveal_txids: p.reveal_txids,
            owner,
            location,
            state,
            kind,
        });
    }

    /// Moves inscriptions whose carrying outputs this transaction spends.
    fn move_spent(&mut self, c: &Ctx) {
        let spent: Vec<(Held, Option<i64>)> =
            c.tx.inputs
                .iter()
                .enumerate()
                .filter_map(|(n, i)| {
                    let held = self.locations.remove(&outpoint(i)?)?;
                    Some((held, sale_price(c.tx, n)))
                })
                .flat_map(|(held, price)| held.into_iter().map(move |h| (h, price)))
                .collect();
        if spent.is_empty() {
            return;
        }
        let dest = successor(c.tx);
        for (h, price) in spent {
            // Paying yourself (e.g. withdrawing a listing) isn't a sale.
            let price = price.filter(|_| {
                dest.as_ref()
                    .is_some_and(|(_, to)| to.is_some() && *to != h.owner)
            });
            let (kind, state, to, location) = match &dest {
                Some((n, addr)) => (
                    if price.is_some() { "sale" } else { "transfer" },
                    State::Transparent,
                    addr.clone(),
                    Some((c.tx.txid.clone(), *n)),
                ),
                None => {
                    let s = terminal(c.tx);
                    (s.as_str(), s, None, None)
                }
            };
            if let Some(loc) = &location {
                self.locations.entry(loc.clone()).or_default().push(Held {
                    id: h.id.clone(),
                    owner: to.clone(),
                });
            }
            self.out.events.push(Event {
                inscription_id: h.id,
                kind,
                price,
                txid: c.tx.txid.clone(),
                height: c.height,
                time: c.time,
                from: h.owner,
                to,
                location,
                state,
            });
        }
    }
}

/// Sighash type of the signature in a scriptSig: the first push shaped like a DER signature
/// (`0x30 <len> ...`) followed by its hashtype byte.
pub fn sighash_type(script_sig: &[u8]) -> Option<u8> {
    tokens(script_sig)?.into_iter().find_map(|t| match t {
        Tok::Data(d)
            if (9..=73).contains(&d.len()) && d[0] == 0x30 && d[1] as usize == d.len() - 3 =>
        {
            d.last().copied()
        }
        _ => None,
    })
}

/// `SIGHASH_SINGLE | SIGHASH_ANYONECANPAY`: the seller signs only their input and the output at
/// the same index (their payment), so a buyer can complete the transaction. Marketplaces
/// (Zerdinals, Zecscriptions, Universe zmarket) settle sales this way.
const SALE_SIGHASH: u8 = 0x83;

/// The price of a sale settled through input `n`: the value of output `n`, when the input is
/// signed SINGLE|ANYONECANPAY.
fn sale_price(tx: &ParsedTx, n: usize) -> Option<i64> {
    (sighash_type(&tx.inputs[n].script_sig)? == SALE_SIGHASH)
        .then(|| tx.outputs.get(n).map(|o| o.value))
        .flatten()
}

/// The sale settled by `tx`, when exactly one of its inputs is signed SINGLE|ANYONECANPAY:
/// `(input index, price)`. Used to re-check moves recorded before sale detection.
pub fn single_sale(tx: &ParsedTx) -> Option<(usize, i64)> {
    let mut sales = (0..tx.inputs.len()).filter_map(|n| Some((n, sale_price(tx, n)?)));
    let first = sales.next()?;
    sales.next().is_none().then_some(first)
}

/// The launchpad drop of an IPFS NFT: the address its commit transaction paid the most,
/// other than P2SH outputs (the reveal chain) and the minter. Ties go to the smallest address,
/// so every mint of a drop agrees. Empty when the commit pays no one else (a self-mint).
pub fn drop_key(commit: &ParsedTx, minter: Option<&str>) -> String {
    commit
        .outputs
        .iter()
        .filter_map(|o| Some((o.address.as_deref()?, o.value)))
        .filter(|(a, _)| !a.starts_with("t3") && !a.starts_with("t2") && Some(*a) != minter)
        .max_by(|(a1, v1), (a2, v2)| v1.cmp(v2).then(a2.cmp(a1)))
        .map(|(a, _)| a.to_string())
        .unwrap_or_default()
}

/// Coarse media family of a content type, for filters and previews.
pub fn media_kind(content_type: &str) -> &'static str {
    let ct = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    match ct.as_str() {
        "image/svg+xml" => "svg",
        "text/html" => "html",
        "application/json" => "json",
        c if c.ends_with("+json") => "json",
        c if c.starts_with("image/") => "image",
        c if c.starts_with("text/") => "text",
        c if c.starts_with("audio/") => "audio",
        c if c.starts_with("video/") => "video",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_tx;

    fn tx(hex_str: &str) -> ParsedTx {
        parse_tx(&hex::decode(hex_str.trim()).unwrap()).unwrap()
    }

    const TEXT: &str = include_str!("../testdata/inscription_text_3132356.hex");
    const PNG_CHAIN: &str = include_str!("../testdata/inscription_png_chain_3495676.hex");
    const ZRC20_DEPLOY: &str = include_str!("../testdata/zrc20_deploy_3133112.hex");
    const ZGODS_MINT: &str = include_str!("../testdata/zrc721_mint_zgods.hex");
    const IPFS_NFT: &str = include_str!("../testdata/ipfs_nft.hex");

    #[test]
    fn parses_first_inscription() {
        let t = tx(TEXT);
        let env = parse_envelope(&t.inputs[0].script_sig).unwrap();
        assert_eq!(env.header, Some((1, "text/plain".into())));
        assert_eq!(env.pieces, vec![(0, b"Lazo is Cooking".to_vec())]);

        let mut e = Engine::new();
        e.apply_tx(3_132_356, 0, 1, &t);
        let i = &e.out.inscriptions[0];
        assert_eq!(
            i.id,
            "219cadf573630d346484043ab12c1199a78730036852870872eeee682f3496d7i0"
        );
        assert_eq!(i.number, 1);
        assert_eq!(i.content, b"Lazo is Cooking");
        assert_eq!(
            i.sha256,
            "cc24bb0eb02674e155c2fe8a9e5ee33291e3f3b244cc4f33851c3294acd9d814"
        );
        assert_eq!(
            i.owner.as_deref(),
            Some("t1Yidqtcf4FeDzbZi4EBZkn84HyS9sypqWU")
        );
        assert_eq!(i.state, State::Transparent);
        assert_eq!(i.kind, Kind::Media("text"));
        assert!(e.pending.is_empty());
    }

    #[test]
    fn reassembles_multi_transaction_png() {
        let txs: Vec<ParsedTx> = PNG_CHAIN.lines().map(tx).collect();
        assert_eq!(txs.len(), 4);
        let mut e = Engine::new();
        for (k, t) in txs.iter().enumerate() {
            e.apply_tx(3_495_676, 0, k as u32 + 1, t);
            if k < 3 {
                assert!(e.out.inscriptions.is_empty());
                assert_eq!(e.pending.len(), 1);
            }
        }
        let i = &e.out.inscriptions[0];
        assert_eq!(
            i.id,
            "efd3caaf2caa4632a3251ed86842b21794f4c1dcf07cc1fbb0b0ad1a1675d725i0"
        );
        assert_eq!(
            i.txid,
            "38a59cf66d9642949857a73b4eb709cabdb9c6ae5f35c6d6916249138d1c721b"
        );
        assert_eq!(i.content_type, "image/png");
        assert_eq!(i.content.len(), 2936);
        assert!(i.content.starts_with(b"\x89PNG"));
        assert_eq!(
            i.sha256,
            "18dfd6c1fbbc5405b3e674a490b31ac0c1bc574212be53617dc2c44add34fb3a"
        );
        assert_eq!(i.reveal_txids.len(), 4);
        assert_eq!(
            i.owner.as_deref(),
            Some("t1Jxr49mdm5nKwWUuSKNNTofyqn7WiFMnLi")
        );
        assert_eq!(i.kind, Kind::Media("image"));
        assert!(e.pending.is_empty());
    }

    #[test]
    fn token_documents_are_numbered_not_indexed() {
        let mut e = Engine::new();
        e.apply_tx(3_133_112, 0, 1, &tx(ZRC20_DEPLOY));
        assert!(e.out.inscriptions.is_empty() && e.locations.is_empty());
        assert_eq!((e.out.skipped, e.next_number), (1, 2));
    }

    #[test]
    fn classifies_collection_items_and_ipfs_nfts() {
        let mut e = Engine::new();
        e.apply_tx(3_143_217, 0, 1, &tx(ZGODS_MINT));
        e.apply_tx(3_136_751, 0, 1, &tx(IPFS_NFT));
        let [z, n] = &e.out.inscriptions[..] else {
            panic!("expected two inscriptions");
        };
        assert_eq!(
            z.kind,
            Kind::Item {
                collection: "ZGODS".into(),
                item: "9963".into()
            }
        );
        assert_eq!(
            z.owner.as_deref(),
            Some("t1Sg7TeEA9TKjYMwsfFeQ3AqyCZDvPKnsny")
        );
        assert_eq!(
            n.kind,
            Kind::Ipfs {
                cid: "QmQgsPXMM5sZ5b3rxmQkASdz2sfPxp33gkYEkDFTG3TxZU".into(),
                mime: "image/png".into()
            }
        );
    }

    #[test]
    fn classify_rules() {
        let j = |s: &str| classify("application/json", s.as_bytes());
        assert_eq!(
            j(
                r#"{"p":"zrc-721","op":"deploy","collection":"zgods","supply":"10000","meta":"bafybeicq"}"#
            ),
            Kind::Deploy {
                collection: "ZGODS".into(),
                supply: Some(10000),
                meta: Some("bafybeicq".into())
            }
        );
        assert_eq!(
            j(r#"{"p":"zrc-20","op":"mint","tick":"ZERO","amt":"1000"}"#),
            Kind::Token
        );
        assert_eq!(
            classify(
                "text/plain",
                br#"{"amt":1000,"tick":"ZINC","type":"zrc20_mint"}"#
            ),
            Kind::Token
        );
        assert_eq!(j(r#"{"r":1}"#), Kind::Media("json"));
        assert_eq!(classify("text/plain", b"hello"), Kind::Media("text"));
        // A pointer must be a CID, never a URL or a path escape.
        assert_eq!(
            j(r#"{"type":"Zerdinals_NFT","content":"https://evil.example/x.png"}"#),
            Kind::Media("json")
        );
        assert!(!is_cid_path("Qm../../etc"));
        assert!(is_cid_path("bafybeiaq/9963.png"));
    }

    /// A synthetic transaction with the given scriptSigs and output addresses.
    fn synth(txid: &str, inputs: &[(&str, u32, Vec<u8>)], outputs: &[Option<&str>]) -> ParsedTx {
        use crate::parse::TxOutput;
        ParsedTx {
            txid: txid.into(),
            version: 5,
            size: 0,
            is_coinbase: false,
            lock_time: 0,
            expiry_height: None,
            inputs: inputs
                .iter()
                .map(|(p, n, s)| TxInput {
                    prev_txid: Some(p.to_string()),
                    prev_n: Some(*n),
                    coinbase_data: None,
                    script_sig: s.clone(),
                })
                .collect(),
            outputs: outputs
                .iter()
                .enumerate()
                .map(|(n, a)| TxOutput {
                    n: n as u32,
                    value: 546,
                    address: a.map(String::from),
                    script: "76a9".into(),
                    op_return: None,
                })
                .collect(),
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

    fn push(d: &[u8]) -> Vec<u8> {
        let mut v = if d.len() <= 75 {
            vec![d.len() as u8]
        } else {
            vec![0x4c, d.len() as u8]
        };
        v.extend_from_slice(d);
        v
    }

    fn envelope(ct: &str, body: &[u8]) -> Vec<u8> {
        let mut s = push(b"ord");
        s.push(0x51); // one piece
        s.extend(push(ct.as_bytes()));
        s.push(0x00); // index 0
        s.extend(push(body));
        s.extend(push(&[0x30; 71])); // signature
        s.extend(push(&[0x21; 40])); // redeem script
        s
    }

    fn sig() -> Vec<u8> {
        let mut s = push(&[0x30; 71]);
        s.extend(push(&[0x02; 33]));
        s
    }

    #[test]
    fn legacy_continuation_without_header() {
        // Two pieces: "AB" (index 1) then "C" (index 0), second tx without the header.
        let mut first = push(b"ord");
        first.push(0x52);
        first.extend(push(b"text/plain"));
        first.push(0x51);
        first.extend(push(b"AB"));
        first.extend(sig());
        let mut second = vec![0x00];
        second.extend(push(b"C"));
        second.extend(sig());

        let mut e = Engine::new();
        e.apply_tx(1, 0, 1, &synth("aa", &[("00", 0, first)], &[Some("t1a")]));
        assert_eq!(e.pending.len(), 1);
        e.apply_tx(2, 0, 1, &synth("bb", &[("aa", 0, second)], &[Some("t1b")]));
        let i = &e.out.inscriptions[0];
        assert_eq!(i.id, "aai0");
        assert_eq!(i.content, b"ABC");
        assert_eq!(i.owner.as_deref(), Some("t1b"));
    }

    #[test]
    fn broken_chain_fails() {
        let mut first = push(b"ord");
        first.push(0x52);
        first.extend(push(b"text/plain"));
        first.push(0x51);
        first.extend(push(b"AB"));
        first.extend(sig());
        let mut e = Engine::new();
        e.apply_tx(1, 0, 1, &synth("aa", &[("00", 0, first)], &[Some("t1a")]));
        e.apply_tx(2, 0, 1, &synth("bb", &[("aa", 0, sig())], &[Some("t1b")]));
        assert!(e.pending.is_empty());
        assert!(e.out.inscriptions.is_empty());
        assert_eq!(e.out.failed_chains, 1);
    }

    #[test]
    fn ownership_follows_outputs_and_ends_shielded() {
        let mut e = Engine::new();
        let s = envelope("text/plain", b"hi");
        e.apply_tx(1, 0, 1, &synth("aa", &[("00", 0, s)], &[Some("t1a")]));
        // Spent to an OP_RETURN then a transparent output: moves to output 1.
        let mut t = synth("bb", &[("aa", 0, sig())], &[None, Some("t1b")]);
        t.outputs[0].op_return = Some(String::new());
        e.apply_tx(2, 0, 1, &t);
        assert_eq!(e.out.events[1].to.as_deref(), Some("t1b"));
        assert_eq!(e.out.events[1].location, Some(("bb".into(), 1)));
        // Spent into a shielded-only transaction.
        let mut t = synth("cc", &[("bb", 1, sig())], &[]);
        t.sapling_outputs.push(String::new());
        e.apply_tx(3, 0, 1, &t);
        assert_eq!(e.out.events[2].kind, "shielded");
        assert_eq!(e.out.events[2].from.as_deref(), Some("t1b"));
        assert!(e.locations.is_empty());
    }

    #[test]
    fn first_deploy_defines_the_collection() {
        let j = |s: &str| envelope("application/json", s.as_bytes());
        let d =
            r#"{"p":"zrc-721","op":"deploy","collection":"CATS","supply":"3","meta":"bafyone"}"#;
        let d2 =
            r#"{"p":"zrc-721","op":"deploy","collection":"cats","supply":"9","meta":"bafytwo"}"#;
        let mut e = Engine::new();
        e.apply_tx(1, 0, 1, &synth("d1", &[("00", 0, j(d))], &[Some("t1a")]));
        e.apply_tx(2, 0, 1, &synth("d2", &[("00", 1, j(d2))], &[Some("t1b")]));
        assert_eq!(e.out.collections.len(), 1);
        let c = &e.out.collections[0];
        assert_eq!(
            (c.name.as_str(), c.meta.as_deref()),
            ("CATS", Some("bafyone"))
        );
        assert_eq!(e.out.inscriptions.len(), 2);
    }

    #[test]
    fn detects_marketplace_sales() {
        let mut e = Engine::new();
        let s = envelope("text/plain", b"hi");
        e.apply_tx(1, 0, 1, &synth("aa", &[("00", 0, s)], &[Some("t1seller")]));
        // Buyer funds input 0 and receives output 0; the seller's input 1 is signed
        // SINGLE|ANYONECANPAY and paid by output 1.
        let signed = |hashtype: u8| {
            let mut der = vec![0x30, 68];
            der.extend([0x02; 68]);
            der.push(hashtype);
            let mut s = push(&der);
            s.extend(push(&[0x02; 33]));
            s
        };
        let mut t = synth(
            "bb",
            &[("ff", 0, signed(0x01)), ("aa", 0, signed(0x83))],
            &[Some("t1buyer"), Some("t1seller")],
        );
        t.outputs[1].value = 2_500_000;
        e.apply_tx(2, 0, 1, &t);
        let ev = &e.out.events[1];
        assert_eq!((ev.kind, ev.price), ("sale", Some(2_500_000)));
        assert_eq!(ev.from.as_deref(), Some("t1seller"));
        assert_eq!(ev.to.as_deref(), Some("t1buyer"));
        // Paying yourself through a marketplace-style input isn't a sale.
        let own = synth("dd", &[("bb", 0, signed(0x83))], &[Some("t1buyer")]);
        e.apply_tx(3, 0, 1, &own);
        assert_eq!(
            (e.out.events[2].kind, e.out.events[2].price),
            ("transfer", None)
        );
        // An ordinary SIGHASH_ALL spend is a transfer.
        e.apply_tx(
            3,
            0,
            1,
            &synth("cc", &[("bb", 0, signed(0x01))], &[Some("t1c")]),
        );
        assert_eq!(
            (e.out.events[2].kind, e.out.events[2].price),
            ("transfer", None)
        );
    }

    /// A Zerdinals marketplace sale (listed at 0.019 ZEC): the seller's input 2 is signed
    /// SINGLE|ANYONECANPAY and output 2 pays them.
    #[test]
    fn real_marketplace_sale() {
        let t = tx(include_str!("../testdata/sale_zerdinals.hex"));
        let signed: Vec<_> = (0..t.inputs.len())
            .filter(|&n| sighash_type(&t.inputs[n].script_sig) == Some(0x83))
            .collect();
        assert_eq!(signed, [2]);
        assert_eq!(sale_price(&t, 2), Some(1_871_500));
        assert_eq!(
            t.outputs[2].address.as_deref(),
            Some("t1XikaeqGFAo6q8wy2FNG8emFYvGvYp796J")
        );
    }

    #[test]
    fn drop_key_is_the_top_payee() {
        let mut t = synth(
            "c",
            &[("00", 0, sig())],
            &[
                Some("t3p2sh"),
                Some("t1fee"),
                Some("t1creator"),
                Some("t1minter"),
            ],
        );
        t.outputs[0].value = 60_000;
        t.outputs[1].value = 100_000;
        t.outputs[2].value = 800_000;
        t.outputs[3].value = 9_000_000;
        assert_eq!(drop_key(&t, Some("t1minter")), "t1creator");
        // Equal shares: the smallest address, so every mint agrees.
        t.outputs[2].value = 100_000;
        assert_eq!(drop_key(&t, Some("t1minter")), "t1creator");
        let own = synth(
            "d",
            &[("00", 0, sig())],
            &[Some("t3p2sh"), Some("t1minter")],
        );
        assert_eq!(drop_key(&own, Some("t1minter")), "");
    }

    #[test]
    fn media_kinds() {
        assert_eq!(media_kind("image/png"), "image");
        assert_eq!(media_kind("image/svg+xml"), "svg");
        assert_eq!(media_kind("text/html;charset=utf-8"), "html");
        assert_eq!(media_kind("application/json"), "json");
    }
}
