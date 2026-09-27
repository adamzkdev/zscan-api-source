//! Zcash inscriptions ("Zerdinals" / Zordinals) and the ZRC-20 tokens they carry.
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
//! ZRC-20 follows the rules of the open-source zord indexer: lowercased 4–5 byte tickers, first
//! deploy wins, a mint may claim up to the per-mint limit and fails past the max supply, and a
//! transfer inscription commits the amount until its output is spent.
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
    /// ZRC-20 operation named by the body (`deploy`, `mint`, `transfer`), valid or not.
    pub zrc20: Option<String>,
}

/// Something that happened to an inscription: `inscribed`, `transfer`, `shielded`, `burned`.
#[derive(Debug, Clone)]
pub struct Event {
    pub inscription_id: String,
    pub kind: &'static str,
    pub txid: String,
    pub height: u32,
    pub time: i64,
    pub from: Option<String>,
    pub to: Option<String>,
    pub location: Option<OutPoint>,
    pub state: State,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub tick: String,
    pub max: u128,
    pub lim: u128,
    pub dec: u8,
    pub minted: u128,
    pub burned: u128,
    pub shielded: u128,
    pub mints: i64,
    pub deployer: String,
    pub deploy_inscription: String,
    pub deploy_txid: String,
    pub height: u32,
    pub time: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Balance {
    pub available: u128,
    /// Committed by transfer inscriptions not yet sent.
    pub transferable: u128,
}

/// An inscribed ZRC-20 transfer whose output hasn't been spent yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed {
    pub tick: String,
    pub amount: u128,
    pub sender: String,
}

/// A ZRC-20 operation, including the ones that did not count (`valid = false`, with a reason).
/// `op`: deploy, mint, transfer (inscribed), send, shielded, burn (settled).
#[derive(Debug, Clone)]
pub struct ZrcEvent {
    pub tick: String,
    pub op: &'static str,
    pub inscription_id: String,
    pub txid: String,
    pub height: u32,
    pub time: i64,
    pub from: Option<String>,
    pub to: Option<String>,
    pub amount: Option<u128>,
    pub valid: bool,
    pub reason: Option<String>,
}

/// Rows to write since the last flush. Keyed sets name entries whose current value is read
/// from the engine (absent = delete).
#[derive(Debug, Default)]
pub struct Changes {
    pub inscriptions: Vec<Inscription>,
    pub events: Vec<Event>,
    pub pending: HashSet<OutPoint>,
    pub tokens: HashSet<String>,
    pub balances: HashSet<(String, String)>,
    pub committed: HashSet<String>,
    pub zrc_events: Vec<ZrcEvent>,
    pub failed_chains: u32,
}

impl Changes {
    pub fn is_empty(&self) -> bool {
        self.inscriptions.is_empty()
            && self.events.is_empty()
            && self.pending.is_empty()
            && self.tokens.is_empty()
            && self.balances.is_empty()
            && self.committed.is_empty()
            && self.zrc_events.is_empty()
    }
}

/// Inscription and ZRC-20 state, applied transaction by transaction in chain order.
#[derive(Debug, Default)]
pub struct Engine {
    pub next_number: i64,
    pub locations: HashMap<OutPoint, Vec<Held>>,
    pub pending: HashMap<OutPoint, Pending>,
    pub tokens: HashMap<String, Token>,
    pub balances: HashMap<(String, String), Balance>,
    pub committed: HashMap<String, Committed>,
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
        let zrc = zrc20_body(&p.content_type, &content);
        let insc = Inscription {
            id: p.id.clone(),
            number: self.next_number,
            genesis_height: p.genesis_height,
            txid: c.tx.txid.clone(),
            height: c.height,
            block_index: c.block_index,
            time: c.time,
            sha256: hex::encode(Sha256::digest(&content)),
            content_type: p.content_type,
            content,
            reveal_txids: p.reveal_txids,
            owner: owner.clone(),
            location: location.clone(),
            state,
            zrc20: zrc
                .as_ref()
                .and_then(|z| z.as_ref().ok())
                .map(|o| o.op.to_string()),
        };
        self.next_number += 1;
        self.out.events.push(Event {
            inscription_id: p.id.clone(),
            kind: "inscribed",
            txid: c.tx.txid.clone(),
            height: c.height,
            time: c.time,
            from: None,
            to: owner.clone(),
            location,
            state,
        });
        self.out.inscriptions.push(insc);
        if let Some(op) = zrc {
            self.zrc_inscribe(op, &p.id, owner, c);
        }
    }

    /// Moves inscriptions whose carrying outputs this transaction spends.
    fn move_spent(&mut self, c: &Ctx) {
        let spent: Vec<Held> =
            c.tx.inputs
                .iter()
                .filter_map(outpoint)
                .filter_map(|op| self.locations.remove(&op))
                .flatten()
                .collect();
        if spent.is_empty() {
            return;
        }
        let dest = successor(c.tx);
        for h in spent {
            let (kind, state, to, location) = match &dest {
                Some((n, addr)) => (
                    "transfer",
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
            self.zrc_settle(&h.id, state, to.clone(), c);
            self.out.events.push(Event {
                inscription_id: h.id,
                kind,
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

// ---------------------------------------------------------------------------------------------
// ZRC-20

#[derive(Debug, Deserialize)]
struct RawOp {
    p: String,
    op: String,
    tick: String,
    #[serde(default)]
    max: Option<String>,
    #[serde(default)]
    lim: Option<String>,
    #[serde(default)]
    amt: Option<String>,
    #[serde(default)]
    dec: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZrcOp {
    pub op: &'static str,
    /// Lowercased.
    pub tick: String,
    pub max: Option<String>,
    pub lim: Option<String>,
    pub amt: Option<String>,
    pub dec: Option<String>,
}

/// A body that reads as a ZRC-20 document: `None` if it isn't one, `Some(Err)` with the reason
/// if it names `zrc-20` but breaks the rules.
pub fn zrc20_body(content_type: &str, body: &[u8]) -> Option<Result<ZrcOp, String>> {
    let ct = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let text = std::str::from_utf8(body).ok()?.trim();
    let json_like = ct == "application/json"
        || ct.ends_with("+json")
        || (ct.starts_with("text/") && text.starts_with('{'));
    if !json_like || !text.to_ascii_lowercase().contains("zrc-20") {
        return None;
    }
    let raw: RawOp = match serde_json::from_str(text) {
        Ok(r) => r,
        Err(_) => {
            // Only report malformed documents that clearly meant to be ZRC-20.
            let v: serde_json::Value = serde_json::from_str(text).ok()?;
            let p = v.get("p")?.as_str()?;
            return (p.eq_ignore_ascii_case("zrc-20"))
                .then(|| Err("fields must be strings".to_string()));
        }
    };
    if !raw.p.eq_ignore_ascii_case("zrc-20") {
        return None;
    }
    Some(validate(raw))
}

fn validate(raw: RawOp) -> Result<ZrcOp, String> {
    let op = match raw.op.as_str() {
        "deploy" => "deploy",
        "mint" => "mint",
        "transfer" => "transfer",
        _ => return Err(format!("unknown op {:?}", truncate(&raw.op, 16))),
    };
    let tick = raw.tick.to_lowercase();
    if !(4..=5).contains(&tick.len()) {
        return Err("ticker must be 4-5 bytes".into());
    }
    for v in [&raw.max, &raw.lim, &raw.amt].into_iter().flatten() {
        check_number(v, raw.dec.as_deref())?;
    }
    if let Some(d) = &raw.dec {
        if d.is_empty()
            || !d.bytes().all(|b| b.is_ascii_digit())
            || d.parse::<u8>().map_or(true, |d| d > 18)
        {
            return Err("dec must be 0-18".into());
        }
    }
    Ok(ZrcOp {
        op,
        tick,
        max: raw.max,
        lim: raw.lim,
        amt: raw.amt,
        dec: raw.dec,
    })
}

fn truncate(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// zord's numeric-string rules: digits with at most one inner dot, not `0`, no more decimals
/// than declared (18 by default), and below 2^64 with the dot removed.
fn check_number(v: &str, dec: Option<&str>) -> Result<(), String> {
    let bad = |m: &str| Err(format!("{m}: {:?}", truncate(v, 24)));
    if v.is_empty() || v == "0" {
        return bad("amount must be positive");
    }
    if v.matches('.').count() > 1 || v.starts_with('.') || v.ends_with('.') {
        return bad("malformed number");
    }
    if !v.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        return bad("malformed number");
    }
    if let Some(dot) = v.find('.') {
        let max = dec.and_then(|d| d.parse::<usize>().ok()).unwrap_or(18);
        if v.len() - dot - 1 > max {
            return bad("too many decimals");
        }
    }
    if v.replace('.', "").parse::<u64>().is_err() {
        return bad("number too large");
    }
    Ok(())
}

/// A decimal string scaled to `dec` decimals.
pub fn scale_amount(v: &str, dec: u8) -> Option<u128> {
    let (whole, frac) = v.split_once('.').unwrap_or((v, ""));
    if frac.len() > dec as usize {
        return None;
    }
    let whole: u128 = if whole.is_empty() {
        0
    } else {
        whole.parse().ok()?
    };
    let frac: u128 = if frac.is_empty() {
        0
    } else {
        format!("{frac:0<width$}", width = dec as usize)
            .parse()
            .ok()?
    };
    whole.checked_mul(10u128.pow(dec as u32))?.checked_add(frac)
}

/// Formats a scaled amount as a decimal string, without trailing zeros.
pub fn format_amount(v: u128, dec: u8) -> String {
    if dec == 0 {
        return v.to_string();
    }
    let s = format!("{v:0>width$}", width = dec as usize + 1);
    let (whole, frac) = s.split_at(s.len() - dec as usize);
    let frac = frac.trim_end_matches('0');
    if frac.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{frac}")
    }
}

/// What a ZRC-20 operation did: parties, amount, or why it did not count.
#[derive(Default)]
struct Effect {
    from: Option<String>,
    to: Option<String>,
    amount: Option<u128>,
    reason: Option<String>,
}

impl Engine {
    fn zrc_event(&mut self, tick: &str, op: &'static str, id: &str, c: &Ctx, e: Effect) {
        self.out.zrc_events.push(ZrcEvent {
            tick: truncate(tick, 32),
            op,
            inscription_id: id.to_string(),
            txid: c.tx.txid.clone(),
            height: c.height,
            time: c.time,
            from: e.from,
            to: e.to,
            amount: e.amount,
            valid: e.reason.is_none(),
            reason: e.reason,
        });
    }

    fn balance(&mut self, tick: &str, addr: &str) -> &mut Balance {
        let key = (tick.to_string(), addr.to_string());
        self.out.balances.insert(key.clone());
        self.balances.entry(key).or_default()
    }

    fn zrc_inscribe(
        &mut self,
        op: Result<ZrcOp, String>,
        id: &str,
        owner: Option<String>,
        c: &Ctx,
    ) {
        let op = match op {
            Ok(op) => op,
            Err(reason) => {
                let e = Effect {
                    to: owner,
                    reason: Some(reason),
                    ..Default::default()
                };
                self.zrc_event("", "invalid", id, c, e);
                return;
            }
        };
        let Some(owner) = owner else {
            let e = Effect {
                reason: Some("inscription has no transparent owner".into()),
                ..Default::default()
            };
            self.zrc_event(&op.tick, op.op, id, c, e);
            return;
        };
        let result = match op.op {
            "deploy" => self.zrc_deploy(&op, id, &owner, c),
            "mint" => self.zrc_mint(&op, &owner),
            _ => self.zrc_commit(&op, id, &owner),
        };
        let e = match result {
            Ok(amount) if op.op == "mint" => Effect {
                to: Some(owner),
                amount,
                ..Default::default()
            },
            Ok(amount) => Effect {
                from: Some(owner),
                amount,
                ..Default::default()
            },
            Err(reason) => Effect {
                from: Some(owner),
                reason: Some(reason),
                ..Default::default()
            },
        };
        self.zrc_event(&op.tick, op.op, id, c, e);
    }

    fn zrc_deploy(
        &mut self,
        op: &ZrcOp,
        id: &str,
        owner: &str,
        c: &Ctx,
    ) -> Result<Option<u128>, String> {
        if self.tokens.contains_key(&op.tick) {
            return Err("ticker already deployed".into());
        }
        let dec: u8 = op.dec.as_deref().map_or(18, |d| d.parse().unwrap_or(18));
        let max_s = op.max.as_deref().ok_or("deploy without max")?;
        let max = scale_amount(max_s, dec).ok_or("max has too many decimals")?;
        let lim = match op.lim.as_deref() {
            Some(l) => scale_amount(l, dec).ok_or("lim has too many decimals")?,
            None => max,
        };
        self.out.tokens.insert(op.tick.clone());
        self.tokens.insert(
            op.tick.clone(),
            Token {
                tick: op.tick.clone(),
                max,
                lim,
                dec,
                minted: 0,
                burned: 0,
                shielded: 0,
                mints: 0,
                deployer: owner.to_string(),
                deploy_inscription: id.to_string(),
                deploy_txid: c.tx.txid.clone(),
                height: c.height,
                time: c.time,
            },
        );
        Ok(None)
    }

    fn zrc_amount(&self, op: &ZrcOp) -> Result<(u128, Token), String> {
        let t = self
            .tokens
            .get(&op.tick)
            .ok_or("ticker not deployed")?
            .clone();
        let amt = op.amt.as_deref().ok_or("missing amt")?;
        let amt = scale_amount(amt, t.dec).ok_or("amt has too many decimals")?;
        Ok((amt, t))
    }

    fn zrc_mint(&mut self, op: &ZrcOp, owner: &str) -> Result<Option<u128>, String> {
        let (amt, t) = self.zrc_amount(op)?;
        if amt > t.lim {
            return Err("amount above the mint limit".into());
        }
        if t.minted + amt > t.max {
            return Err("max supply reached".into());
        }
        let tok = self.tokens.get_mut(&op.tick).expect("checked");
        tok.minted += amt;
        tok.mints += 1;
        self.out.tokens.insert(op.tick.clone());
        self.balance(&op.tick, owner).available += amt;
        Ok(Some(amt))
    }

    fn zrc_commit(&mut self, op: &ZrcOp, id: &str, owner: &str) -> Result<Option<u128>, String> {
        let (amt, _) = self.zrc_amount(op)?;
        let b = self
            .balances
            .get(&(op.tick.clone(), owner.to_string()))
            .copied()
            .unwrap_or_default();
        if b.available < amt {
            return Err("insufficient available balance".into());
        }
        let b = self.balance(&op.tick, owner);
        b.available -= amt;
        b.transferable += amt;
        self.out.committed.insert(id.to_string());
        self.committed.insert(
            id.to_string(),
            Committed {
                tick: op.tick.clone(),
                amount: amt,
                sender: owner.to_string(),
            },
        );
        Ok(Some(amt))
    }

    /// Delivers a committed transfer when its inscription's output is first spent.
    fn zrc_settle(&mut self, id: &str, state: State, to: Option<String>, c: &Ctx) {
        let Some(t) = self.committed.remove(id) else {
            return;
        };
        self.out.committed.insert(id.to_string());
        let b = self.balance(&t.tick, &t.sender);
        b.transferable = b.transferable.saturating_sub(t.amount);
        let op = match (state, &to) {
            (State::Transparent, Some(r)) => {
                self.balance(&t.tick, r).available += t.amount;
                "send"
            }
            (State::Shielded, _) => {
                if let Some(tok) = self.tokens.get_mut(&t.tick) {
                    tok.shielded += t.amount;
                }
                "shielded"
            }
            // Burned, or a transparent output without an address.
            _ => {
                if let Some(tok) = self.tokens.get_mut(&t.tick) {
                    tok.burned += t.amount;
                }
                "burn"
            }
        };
        self.out.tokens.insert(t.tick.clone());
        let e = Effect {
            from: Some(t.sender),
            to,
            amount: Some(t.amount),
            reason: None,
        };
        self.zrc_event(&t.tick, op, id, c, e);
    }
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
    const DEPLOY: &str = include_str!("../testdata/zrc20_deploy_3133112.hex");
    const MINT: &str = include_str!("../testdata/zrc20_mint_3133115.hex");

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
        assert!(e.pending.is_empty());
    }

    #[test]
    fn deploys_and_mints_zrc20() {
        let mut e = Engine::new();
        e.apply_tx(3_133_112, 0, 1, &tx(DEPLOY));
        e.apply_tx(3_133_115, 0, 1, &tx(MINT));
        let t = &e.tokens["zero"];
        assert_eq!(t.max, 21_000_000 * 10u128.pow(18));
        assert_eq!(t.lim, 1_000 * 10u128.pow(18));
        assert_eq!(t.minted, 1_000 * 10u128.pow(18));
        let minter = e.out.inscriptions[1].owner.clone().unwrap();
        assert_eq!(
            e.balances[&("zero".to_string(), minter)].available,
            1_000 * 10u128.pow(18)
        );
        assert!(e.out.zrc_events.iter().all(|z| z.valid));
        assert_eq!(e.out.inscriptions[1].zrc20.as_deref(), Some("mint"));
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
    fn zrc20_transfer_commits_then_sends() {
        let j = |s: &str| envelope("application/json", s.as_bytes());
        let mut e = Engine::new();
        let deploy =
            r#"{"p":"zrc-20","op":"deploy","tick":"TEST","max":"100","lim":"10","dec":"0"}"#;
        e.apply_tx(
            1,
            0,
            1,
            &synth("d0", &[("00", 0, j(deploy))], &[Some("t1dep")]),
        );
        let mint = r#"{"p":"zrc-20","op":"mint","tick":"test","amt":"10"}"#;
        e.apply_tx(2, 0, 1, &synth("m0", &[("00", 1, j(mint))], &[Some("t1a")]));
        let over = r#"{"p":"zrc-20","op":"mint","tick":"test","amt":"11"}"#;
        e.apply_tx(2, 0, 2, &synth("m1", &[("00", 2, j(over))], &[Some("t1a")]));
        let xfer = r#"{"p":"zrc-20","op":"transfer","tick":"test","amt":"4"}"#;
        e.apply_tx(3, 0, 1, &synth("x0", &[("00", 3, j(xfer))], &[Some("t1a")]));
        let bal = |e: &Engine, a: &str| e.balances[&("test".to_string(), a.to_string())];
        assert_eq!(
            bal(&e, "t1a"),
            Balance {
                available: 6,
                transferable: 4
            }
        );
        // Spend the transfer inscription to t1b.
        e.apply_tx(4, 0, 1, &synth("s0", &[("x0", 0, sig())], &[Some("t1b")]));
        assert_eq!(
            bal(&e, "t1a"),
            Balance {
                available: 6,
                transferable: 0
            }
        );
        assert_eq!(
            bal(&e, "t1b"),
            Balance {
                available: 4,
                transferable: 0
            }
        );
        // Moving it again does nothing to balances.
        e.apply_tx(5, 0, 1, &synth("s1", &[("s0", 0, sig())], &[Some("t1c")]));
        assert!(!e
            .balances
            .contains_key(&("test".to_string(), "t1c".to_string())));
        let t = &e.tokens["test"];
        assert_eq!((t.minted, t.mints), (10, 1));
        let ops: Vec<_> = e.out.zrc_events.iter().map(|z| (z.op, z.valid)).collect();
        assert_eq!(
            ops,
            [
                ("deploy", true),
                ("mint", true),
                ("mint", false),
                ("transfer", true),
                ("send", true)
            ]
        );
    }

    #[test]
    fn zrc20_rules() {
        let body = |s: &str| zrc20_body("text/plain", s.as_bytes());
        assert!(body("hello zrc-20").is_none());
        assert!(body(r#"{"p":"brc-20","op":"mint","tick":"ordi","amt":"1"}"#).is_none());
        assert!(body(r#"{"p":"zrc-20","op":"mint","tick":"abc","amt":"1"}"#)
            .unwrap()
            .is_err());
        assert!(
            body(r#"{"p":"zrc-20","op":"mint","tick":"abcd","amt":"0"}"#)
                .unwrap()
                .is_err()
        );
        assert!(body(r#"{"p":"zrc-20","op":"mint","tick":"abcd","amt":1}"#)
            .unwrap()
            .is_err());
        let ok = body(r#"{"p":"ZRC-20","op":"mint","tick":"ABCD","amt":"1.5"}"#)
            .unwrap()
            .unwrap();
        assert_eq!(ok.tick, "abcd");
        assert_eq!(scale_amount("1.5", 18), Some(15 * 10u128.pow(17)));
        assert_eq!(scale_amount("1.5", 0), None);
        assert_eq!(format_amount(15 * 10u128.pow(17), 18), "1.5");
        assert_eq!(format_amount(21_000_000 * 10u128.pow(18), 18), "21000000");
        assert_eq!(format_amount(5, 2), "0.05");
    }

    #[test]
    fn media_kinds() {
        assert_eq!(media_kind("image/png"), "image");
        assert_eq!(media_kind("image/svg+xml"), "svg");
        assert_eq!(media_kind("text/html;charset=utf-8"), "html");
        assert_eq!(media_kind("application/json"), "json");
    }
}
