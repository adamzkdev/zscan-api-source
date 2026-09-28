//! ZEC crossing chains: Maya Protocol swaps and NEAR Intents (Omni bridge) mints and burns,
//! read from each protocol's public API. Everything here is best effort: a protocol API being
//! down only delays its events.

use anyhow::{Context, Result};
use serde::Deserialize;

const MAYA_MIDGARD: &str = "https://midgard.mayachain.info";
const NEAR_RPC: &[&str] = &[
    "https://rpc.mainnet.near.org",
    "https://free.rpc.fastnear.com",
];
const NEARBLOCKS: &str = "https://api.nearblocks.io";
/// ZEC on NEAR (NEAR Intents / Omni bridge), a NEP-141 token with 8 decimals like ZEC.
pub const NEAR_ZEC_TOKEN: &str = "zec.omft.near";
const ZEC: &str = "ZEC.ZEC";

/// One cross-chain movement of ZEC, as reported by the protocol.
#[derive(Debug, Clone, PartialEq)]
pub struct BridgeEvent {
    pub protocol: &'static str,
    pub external_id: String,
    /// Relative to Zcash: "in" = ZEC arrives on Zcash, "out" = ZEC leaves Zcash.
    pub direction: &'static str,
    /// ZEC moved, zatoshis.
    pub amount: i64,
    pub counter_asset: String,
    pub counter_chain: String,
    pub usd: Option<f64>,
    pub zcash_txid: Option<String>,
    pub external_url: Option<String>,
    pub status: String,
    /// Unix seconds.
    pub time: i64,
    /// Amount of `counter_asset` on the other side, in its own units, when reported.
    pub counter_amount: Option<f64>,
    /// The Zcash address ZEC was sent to (in) or refunded to (out), when reported.
    pub zcash_address: Option<String>,
    /// transparent | unified | sapling, from `zcash_address`.
    pub address_kind: Option<&'static str>,
    /// The app or wallet that routed the swap (NEAR Intents referral).
    pub referral: Option<String>,
}

/* ------------------------------------------------------------------ Maya */

#[derive(Debug, Deserialize)]
pub struct MayaActions {
    #[serde(default)]
    actions: Vec<MayaAction>,
    #[serde(default)]
    meta: MayaMeta,
}

#[derive(Debug, Default, Deserialize)]
struct MayaMeta {
    #[serde(rename = "nextPageToken", default)]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MayaAction {
    date: String,
    status: String,
    #[serde(default)]
    pools: Vec<String>,
    #[serde(rename = "in", default)]
    inputs: Vec<MayaTx>,
    #[serde(rename = "out", default)]
    outputs: Vec<MayaTx>,
    #[serde(default)]
    metadata: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct MayaTx {
    #[serde(rename = "txID", default)]
    tx_id: String,
    #[serde(default)]
    coins: Vec<MayaCoin>,
}

#[derive(Debug, Deserialize)]
struct MayaCoin {
    asset: String,
    amount: String,
}

fn zec_in(txs: &[MayaTx]) -> Option<(i64, &str)> {
    txs.iter().find_map(|t| {
        let c = t.coins.iter().find(|c| c.asset == ZEC)?;
        Some((c.amount.parse().ok()?, t.tx_id.as_str()))
    })
}

/// Maps Midgard swap actions touching the ZEC pool to bridge events. Swaps that neither send
/// nor receive ZEC (none expected with `asset=ZEC.ZEC`) are skipped.
pub fn maya_events(page: &MayaActions) -> Vec<BridgeEvent> {
    page.actions
        .iter()
        .filter_map(|a| {
            let swap = &a.metadata["swap"];
            let price = |k: &str| swap[k].as_str().and_then(|v| v.parse::<f64>().ok());
            let inbound_id = a
                .inputs
                .first()
                .map(|t| t.tx_id.clone())
                .filter(|s| !s.is_empty())?;
            let (direction, amount, zcash_txid, usd_price) =
                if let Some((amt, tx)) = zec_in(&a.inputs) {
                    ("out", amt, Some(tx.to_string()), price("inPriceUSD"))
                } else if let Some((amt, tx)) = zec_in(&a.outputs) {
                    (
                        "in",
                        amt,
                        (!tx.is_empty()).then(|| tx.to_string()),
                        price("outPriceUSD"),
                    )
                } else {
                    // Pending ZEC-bound swap: Maya only reports the expected output.
                    let amt = swap["streamingSwapMeta"]["outCoin"]
                        .as_object()
                        .filter(|c| c["asset"] == ZEC)
                        .and_then(|c| c["amount"].as_str()?.parse().ok())?;
                    ("in", amt, None, price("outPriceUSD"))
                };
            let counter_asset = a
                .pools
                .iter()
                .find(|p| p.as_str() != ZEC)
                .cloned()
                .unwrap_or_else(|| "MAYA.CACAO".into());
            let counter_chain = counter_asset
                .split('.')
                .next()
                .unwrap_or_default()
                .to_string();
            let time = a.date.parse::<i64>().ok()? / 1_000_000_000;
            Some(BridgeEvent {
                protocol: "maya",
                external_url: Some(format!(
                    "https://www.explorer.mayachain.info/tx/{inbound_id}"
                )),
                external_id: inbound_id,
                direction,
                amount,
                counter_asset,
                counter_chain,
                usd: usd_price.map(|p| p * amount as f64 / 1e8),
                zcash_txid: zcash_txid.map(|t| t.to_ascii_lowercase()),
                status: a.status.clone(),
                time,
                counter_amount: None,
                zcash_address: None,
                address_kind: None,
                referral: None,
            })
        })
        .collect()
}

/// One page of Maya swaps touching ZEC, newest first, and the token for the next (older) page.
pub async fn maya_swaps(
    http: &reqwest::Client,
    page_token: Option<&str>,
) -> Result<(Vec<BridgeEvent>, Option<String>)> {
    let mut url = format!("{MAYA_MIDGARD}/v2/actions?asset={ZEC}&type=swap&limit=50");
    if let Some(t) = page_token {
        url.push_str(&format!("&nextPageToken={t}"));
    }
    let page: MayaActions = http
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .context("maya actions")?;
    Ok((maya_events(&page), page.meta.next_page_token.clone()))
}

/* ------------------------------------------------------------------ NEAR */

/// Total ZEC minted on NEAR (`ft_total_supply` of zec.omft.near), zatoshis: the ZEC the
/// NEAR Intents bridge holds on Zcash for it.
pub async fn near_zec_supply(http: &reqwest::Client) -> Result<i64> {
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": "zscan", "method": "query",
        "params": {
            "request_type": "call_function", "finality": "final",
            "account_id": NEAR_ZEC_TOKEN, "method_name": "ft_total_supply", "args_base64": "e30=",
        },
    });
    let mut last = None;
    for rpc in NEAR_RPC {
        let res: Result<i64> = async {
            let v: serde_json::Value = http
                .post(*rpc)
                .json(&body)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let bytes: Vec<u8> = serde_json::from_value(v["result"]["result"].clone())
                .with_context(|| format!("unexpected NEAR RPC answer: {v}"))?;
            let s: String = serde_json::from_slice(&bytes)?;
            Ok(s.parse()?)
        }
        .await;
        match res {
            Ok(v) => return Ok(v),
            Err(e) => last = Some(e.context(rpc.to_string())),
        }
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("no NEAR RPC")))
}

#[derive(Debug, Deserialize)]
pub struct NearTxns {
    #[serde(default)]
    txns: Vec<NearTxn>,
}

#[derive(Debug, Deserialize)]
struct NearTxn {
    event_index: String,
    affected_account_id: String,
    delta_amount: String,
    cause: String,
    transaction_hash: String,
    block_timestamp: String,
}

/// Mints (ZEC deposited from Zcash into NEAR) and burns (ZEC withdrawn back to Zcash) of
/// zec.omft.near. Transfers inside NEAR are not cross-chain and are skipped.
pub fn near_events(page: &NearTxns) -> Vec<BridgeEvent> {
    page.txns
        .iter()
        .filter_map(|t| {
            let direction = match t.cause.as_str() {
                "MINT" => "out",
                "BURN" => "in",
                _ => return None,
            };
            let amount = t.delta_amount.parse::<i64>().ok()?.abs();
            Some(BridgeEvent {
                protocol: "near-intents",
                external_id: t.event_index.clone(),
                direction,
                amount,
                counter_asset: format!("NEAR.{NEAR_ZEC_TOKEN}"),
                counter_chain: "NEAR".into(),
                usd: None,
                zcash_txid: None,
                external_url: Some(format!("https://nearblocks.io/txns/{}", t.transaction_hash)),
                status: if t.affected_account_id.is_empty() {
                    "unknown"
                } else {
                    "success"
                }
                .into(),
                time: t.block_timestamp.parse::<i64>().ok()? / 1_000_000_000,
                counter_amount: None,
                zcash_address: None,
                address_kind: None,
                referral: None,
            })
        })
        .collect()
}

/// The latest mints and burns of zec.omft.near (NearBlocks' `event` filter: without it a page is
/// mostly transfers inside NEAR). Two calls; its free tier allows only a few a minute, so
/// callers poll it slowly.
pub async fn near_recent(http: &reqwest::Client) -> Result<Vec<BridgeEvent>> {
    let mut out = Vec::new();
    for event in ["MINT", "BURN"] {
        let page: NearTxns = http
            .get(format!(
                "{NEARBLOCKS}/v1/fts/{NEAR_ZEC_TOKEN}/txns?per_page=100&event={event}"
            ))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("nearblocks txns")?;
        out.extend(near_events(&page));
    }
    Ok(out)
}

/* ------------------------------------------------------- NEAR Intents 1Click */

const NEAR_INTENTS_EXPLORER: &str = "https://explorer.near-intents.org";
const ONE_CLICK_TOKENS: &str = "https://1click.chaindefuser.com/v0/tokens";
/// ZEC on Zcash as NEAR Intents names it: deposits and withdrawals of this asset cross Zcash.
const INTENTS_ZEC: &str = "nep141:zec.omft.near";

/// Chain and symbol of each NEAR Intents asset id, from the public 1Click token list.
pub type IntentTokens = std::collections::HashMap<String, (String, String)>;

#[derive(Debug, Deserialize)]
struct IntentToken {
    #[serde(rename = "assetId")]
    asset_id: String,
    blockchain: String,
    symbol: String,
}

pub async fn intent_tokens(http: &reqwest::Client) -> Result<IntentTokens> {
    let list: Vec<IntentToken> = http
        .get(ONE_CLICK_TOKENS)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .context("1click tokens")?;
    Ok(list
        .into_iter()
        .map(|t| {
            // "BTC(OMNI)" → "BTC"
            let symbol = t
                .symbol
                .split('(')
                .next()
                .unwrap_or_default()
                .trim()
                .to_string();
            (t.asset_id, (t.blockchain.to_uppercase(), symbol))
        })
        .collect())
}

/// Chain and symbol of an asset, from the token list or, for an unlisted one, its id
/// (`nep141:eth-0x….omft.near`, `1cs_v1:btc:native:coin`).
fn intent_asset(tokens: &IntentTokens, id: &str) -> (String, String) {
    if let Some(t) = tokens.get(id) {
        return t.clone();
    }
    let chain = match id.split(':').collect::<Vec<_>>().as_slice() {
        ["1cs_v1", chain, ..] => chain.to_string(),
        [_, rest] => rest
            .split(['-', '.'])
            .next()
            .unwrap_or_default()
            .to_string(),
        _ => "near".into(),
    };
    (chain.to_uppercase(), "?".into())
}

/// transparent | unified | sapling for a Zcash address; None for anything else.
pub fn zcash_address_kind(addr: &str) -> Option<&'static str> {
    if addr.starts_with("t1") || addr.starts_with("t3") || addr.starts_with("tex1") {
        Some("transparent")
    } else if addr.starts_with("u1") {
        Some("unified")
    } else if addr.starts_with("zs1") {
        Some("sapling")
    } else {
        None
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntentTx {
    origin_asset: String,
    destination_asset: String,
    pub deposit_address: String,
    #[serde(default)]
    pub deposit_memo: Option<String>,
    #[serde(default)]
    recipient: String,
    status: String,
    pub created_at_timestamp: i64,
    #[serde(default)]
    referral: Option<String>,
    #[serde(default)]
    amount_in: String,
    #[serde(default)]
    amount_in_formatted: String,
    #[serde(default)]
    amount_in_usd: Option<String>,
    #[serde(default)]
    amount_out: String,
    #[serde(default)]
    amount_out_formatted: String,
    #[serde(default)]
    amount_out_usd: Option<String>,
    #[serde(default)]
    origin_chain_tx_hashes: Vec<String>,
    #[serde(default)]
    destination_chain_tx_hashes: Vec<String>,
    #[serde(default)]
    refund_to: Option<String>,
    #[serde(default)]
    deposit_type: String,
    #[serde(default)]
    recipient_type: String,
}

/// Maps a 1Click transaction to a bridge event when ZEC crosses Zcash: deposited from a Zcash
/// address (`out`) or delivered to one (`in`). Swaps from or to ZEC held inside NEAR Intents
/// don't touch Zcash and are skipped.
impl IntentTx {
    /// The event id: deposit address, plus `:memo` for memo deposits.
    pub fn id(&self) -> String {
        match self.deposit_memo.as_deref().filter(|m| !m.is_empty()) {
            Some(m) => format!("{}:{m}", self.deposit_address),
            None => self.deposit_address.clone(),
        }
    }
}

pub fn intent_event(tokens: &IntentTokens, t: &IntentTx) -> Option<BridgeEvent> {
    // Zero means not known yet (an amount out while the swap is processing).
    let num = |s: &str| s.parse::<f64>().ok().filter(|v| *v > 0.0);
    let usd = |s: &Option<String>| s.as_deref().and_then(num);
    let intents = ("NEAR".to_string(), "ZEC".to_string());
    let (
        direction,
        amount,
        usd,
        counter_id,
        counter_via_intents,
        counter_amount,
        zcash_txid,
        zaddr,
    ) = if t.origin_asset == INTENTS_ZEC && t.deposit_type == "ORIGIN_CHAIN" {
        (
            "out",
            t.amount_in.parse::<i64>().ok()?,
            usd(&t.amount_in_usd),
            &t.destination_asset,
            t.recipient_type == "INTENTS",
            num(&t.amount_out_formatted),
            t.origin_chain_tx_hashes.first(),
            t.refund_to.clone(),
        )
    } else if t.destination_asset == INTENTS_ZEC && t.recipient_type == "DESTINATION_CHAIN" {
        (
            "in",
            t.amount_out.parse::<i64>().ok()?,
            usd(&t.amount_out_usd),
            &t.origin_asset,
            t.deposit_type == "INTENTS",
            num(&t.amount_in_formatted),
            t.destination_chain_tx_hashes.first(),
            Some(t.recipient.clone()),
        )
    } else {
        return None;
    };
    // ZEC moved between Zcash and a NEAR Intents balance is a bridge transfer, not a swap.
    let (counter_chain, symbol) = if counter_via_intents && counter_id == INTENTS_ZEC {
        intents
    } else if counter_via_intents {
        ("NEAR".into(), intent_asset(tokens, counter_id).1)
    } else {
        intent_asset(tokens, counter_id)
    };
    let zaddr = zaddr.filter(|a| zcash_address_kind(a).is_some());
    let external_id = t.id();
    Some(BridgeEvent {
        protocol: "near-intents",
        external_url: Some(format!(
            "{NEAR_INTENTS_EXPLORER}/transactions/{}",
            t.deposit_address
        )),
        external_id,
        direction,
        amount,
        counter_asset: format!("{counter_chain}.{symbol}"),
        counter_chain,
        usd,
        zcash_txid: zcash_txid
            .filter(|h| h.len() == 64)
            .map(|h| h.to_ascii_lowercase()),
        status: t.status.to_ascii_lowercase(),
        time: t.created_at_timestamp,
        counter_amount,
        address_kind: zaddr.as_deref().and_then(zcash_address_kind),
        zcash_address: zaddr,
        referral: t.referral.clone().filter(|r| !r.is_empty()),
    })
}

/// Which side of the swap ZEC is on: NEAR Intents' explorer ANDs its filters, so ZEC leaving
/// Zcash and ZEC arriving on it are two queries.
#[derive(Debug, Clone, Copy)]
pub enum IntentSide {
    FromZcash,
    ToZcash,
}

/// One page (100) of 1Click transactions with ZEC on `side`, newest first, older than the
/// given deposit when paging. The partner API allows one call every 5 seconds.
pub async fn intent_txs(
    http: &reqwest::Client,
    jwt: &str,
    side: IntentSide,
    before: Option<&str>,
) -> Result<Vec<IntentTx>> {
    let filter = match side {
        IntentSide::FromZcash => "fromChainId=zec",
        IntentSide::ToZcash => "toChainId=zec",
    };
    let mut req = http
        .get(format!(
            "{NEAR_INTENTS_EXPLORER}/api/v0/transactions?numberOfTransactions=100&{filter}"
        ))
        .bearer_auth(jwt);
    // `before` is an event id: the deposit address, then `:memo` when there is one.
    if let Some(b) = before {
        let (addr, memo) = b.split_once(':').unwrap_or((b, ""));
        req = req.query(&[("lastDepositAddress", addr), ("direction", "next")]);
        if !memo.is_empty() {
            req = req.query(&[("lastDepositMemo", memo)]);
        }
    }
    req.send()
        .await?
        .error_for_status()?
        .json()
        .await
        .context("near intents transactions")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maya_swaps_both_ways() {
        let page: MayaActions = serde_json::from_value(serde_json::json!({
            "actions": [
                {"date": "1787070345831626451", "status": "pending", "pools": ["ZEC.ZEC", "ETH.ETH"],
                 "in": [{"txID": "406DC0", "coins": [{"asset": "ZEC.ZEC", "amount": "100000000"}]}],
                 "out": [], "metadata": {"swap": {"inPriceUSD": "500", "outPriceUSD": "2000"}}},
                {"date": "1787070000000000000", "status": "success", "pools": ["ETH.ETH", "ZEC.ZEC"],
                 "in": [{"txID": "2F859A", "coins": [{"asset": "ETH.ETH", "amount": "600000"}]}],
                 "out": [{"txID": "DA57BD", "coins": [{"asset": "ZEC.ZEC", "amount": "2199981"}]}],
                 "metadata": {"swap": {"inPriceUSD": "2000", "outPriceUSD": "500"}}},
                {"date": "1787060000000000000", "status": "success", "pools": ["ZEC.ZEC"],
                 "in": [{"txID": "AAAA", "coins": [{"asset": "MAYA.CACAO", "amount": "1"}]}],
                 "out": [], "metadata": {"swap": {}}}
            ],
            "meta": {"nextPageToken": "1"}
        }))
        .unwrap();
        let ev = maya_events(&page);
        assert_eq!(ev.len(), 2);
        assert_eq!((ev[0].direction, ev[0].amount), ("out", 100_000_000));
        assert_eq!(ev[0].counter_chain, "ETH");
        assert_eq!(ev[0].zcash_txid.as_deref(), Some("406dc0"));
        assert_eq!(ev[0].usd, Some(500.0));
        assert_eq!(ev[0].time, 1_787_070_345);
        assert_eq!((ev[1].direction, ev[1].amount), ("in", 2_199_981));
        assert_eq!(ev[1].zcash_txid.as_deref(), Some("da57bd"));
    }

    #[test]
    fn near_mints_and_burns() {
        let page: NearTxns = serde_json::from_value(serde_json::json!({"txns": [
            {"event_index": "1", "affected_account_id": "omni.bridge.near", "delta_amount": "-91179059",
             "cause": "BURN", "transaction_hash": "8G8S", "block_timestamp": "1790475038000000000"},
            {"event_index": "2", "affected_account_id": "a.near", "delta_amount": "5",
             "cause": "TRANSFER", "transaction_hash": "x", "block_timestamp": "1790475038000000000"},
            {"event_index": "3", "affected_account_id": "intents.near", "delta_amount": "700",
             "cause": "MINT", "transaction_hash": "y", "block_timestamp": "1790475038000000000"}
        ]}))
        .unwrap();
        let ev = near_events(&page);
        assert_eq!(ev.len(), 2);
        assert_eq!((ev[0].direction, ev[0].amount), ("in", 91_179_059));
        assert_eq!((ev[1].direction, ev[1].amount), ("out", 700));
    }

    #[test]
    fn near_intents_swaps() {
        let tokens: IntentTokens = [(
            "nep141:tron-d28a265909efecdcee7c5028585214ea0b96f015.omft.near".to_string(),
            ("TRON".to_string(), "USDT".to_string()),
        )]
        .into();
        let txs: Vec<IntentTx> = serde_json::from_value(serde_json::json!([
            {"originAsset": "1cs_v1:sol:spl:A7bd", "destinationAsset": "nep141:zec.omft.near",
             "depositAddress": "3RuE", "depositMemo": null, "recipient": "u1wl3rnd",
             "status": "SUCCESS", "createdAtTimestamp": 1790636999, "referral": "snarkstr",
             "amountIn": "5145143", "amountInFormatted": "0.05145143", "amountInUsd": "76.14",
             "amountOut": "5113143", "amountOutFormatted": "0.05113143", "amountOutUsd": "75.67",
             "originChainTxHashes": ["3GKp"],
             "destinationChainTxHashes": ["201FC0BF64837862A9B05CA26DC79FB5E5EF63F7FBCBE8FEBDA69FF54A2F69BE"],
             "refundTo": "Ca7H", "depositType": "ORIGIN_CHAIN", "recipientType": "DESTINATION_CHAIN"},
            {"originAsset": "nep141:zec.omft.near",
             "destinationAsset": "nep141:tron-d28a265909efecdcee7c5028585214ea0b96f015.omft.near",
             "depositAddress": "t1Rp", "depositMemo": null, "recipient": "TXyz",
             "status": "PROCESSING", "createdAtTimestamp": 1790636776, "referral": "",
             "amountIn": "35464000", "amountInFormatted": "0.35464", "amountInUsd": "524.66",
             "amountOut": "520184592", "amountOutFormatted": "520.184592", "amountOutUsd": "520.00",
             "originChainTxHashes": ["5c6f7c8a5c6f7c8a5c6f7c8a5c6f7c8a5c6f7c8a5c6f7c8a5c6f7c8a5c6f7c8a"],
             "destinationChainTxHashes": [], "refundTo": "t1ZVbW2s",
             "depositType": "ORIGIN_CHAIN", "recipientType": "DESTINATION_CHAIN"},
            {"originAsset": "nep141:zec.omft.near", "destinationAsset": "nep141:zec.omft.near",
             "depositAddress": "4f16", "recipient": "t1dZxngM", "status": "SUCCESS",
             "createdAtTimestamp": 1790626486, "amountIn": "52710267", "amountOut": "52678267",
             "depositType": "INTENTS", "recipientType": "DESTINATION_CHAIN"},
            {"originAsset": "nep141:zec.omft.near", "destinationAsset": "nep141:sol.omft.near",
             "depositAddress": "x", "recipient": "y", "status": "SUCCESS",
             "createdAtTimestamp": 1, "amountIn": "1", "amountOut": "1",
             "depositType": "INTENTS", "recipientType": "DESTINATION_CHAIN"}
        ]))
        .unwrap();
        let ev: Vec<_> = txs
            .iter()
            .filter_map(|t| intent_event(&tokens, t))
            .collect();
        assert_eq!(ev.len(), 3);
        assert_eq!((ev[0].direction, ev[0].amount), ("in", 5_113_143));
        assert_eq!(
            (ev[0].counter_chain.as_str(), ev[0].counter_asset.as_str()),
            ("SOL", "SOL.?")
        );
        assert_eq!(ev[0].address_kind, Some("unified"));
        assert_eq!(ev[0].usd, Some(75.67));
        assert_eq!(ev[0].referral.as_deref(), Some("snarkstr"));
        assert!(ev[0].zcash_txid.as_deref().unwrap().starts_with("201fc0"));
        assert_eq!((ev[1].direction, ev[1].amount), ("out", 35_464_000));
        assert_eq!(ev[1].counter_asset, "TRON.USDT");
        assert_eq!(ev[1].counter_amount, Some(520.184592));
        assert_eq!(ev[1].status, "processing");
        assert_eq!(ev[1].address_kind, Some("transparent"));
        assert_eq!(ev[1].referral, None);
        assert_eq!(
            (ev[2].direction, ev[2].counter_asset.as_str()),
            ("in", "NEAR.ZEC")
        );
    }
}
