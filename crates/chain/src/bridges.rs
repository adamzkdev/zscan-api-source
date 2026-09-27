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
            })
        })
        .collect()
}

/// The latest zec.omft.near events (one NearBlocks page; its free tier allows only a few calls
/// a minute, so callers poll it slowly).
pub async fn near_recent(http: &reqwest::Client) -> Result<Vec<BridgeEvent>> {
    let page: NearTxns = http
        .get(format!(
            "{NEARBLOCKS}/v1/fts/{NEAR_ZEC_TOKEN}/txns?per_page=100"
        ))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .context("nearblocks txns")?;
    Ok(near_events(&page))
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
}
