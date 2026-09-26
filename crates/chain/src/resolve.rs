//! Resolution of spent transparent outputs (value + address), needed for fees and
//! address history. Callers look up their own index first; the rest is fetched from
//! lightwalletd and cached.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use futures::{stream, StreamExt, TryStreamExt};
use lru::LruCache;
use serde::Serialize;

use crate::parse::{parse_tx, TxInput};
use crate::Lwd;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrevOut {
    pub value: i64,
    pub address: Option<String>,
}

pub type OutPoint = (String, u32);

#[derive(Clone)]
pub struct Resolver {
    lwd: Lwd,
    cache: Arc<Mutex<LruCache<String, Arc<Vec<PrevOut>>>>>,
}

impl Resolver {
    pub fn new(lwd: Lwd, capacity: usize) -> Self {
        Self {
            lwd,
            cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(capacity.max(1)).unwrap(),
            ))),
        }
    }

    /// Records outputs of a transaction we just decoded, so later spends resolve locally.
    pub fn remember(&self, txid: &str, outputs: Vec<PrevOut>) {
        self.cache
            .lock()
            .unwrap()
            .put(txid.to_string(), Arc::new(outputs));
    }

    async fn outputs_of(&self, txid: &str) -> Result<Option<Arc<Vec<PrevOut>>>> {
        if let Some(o) = self.cache.lock().unwrap().get(txid) {
            return Ok(Some(o.clone()));
        }
        let Some(raw) = self.lwd.raw_tx(txid).await? else {
            return Ok(None);
        };
        let tx = parse_tx(&raw.data)?;
        let outs: Arc<Vec<PrevOut>> = Arc::new(
            tx.outputs
                .into_iter()
                .map(|o| PrevOut {
                    value: o.value,
                    address: o.address,
                })
                .collect(),
        );
        self.cache
            .lock()
            .unwrap()
            .put(txid.to_string(), outs.clone());
        Ok(Some(outs))
    }

    /// Resolves each input's spent output. `known` holds outputs the caller already found.
    pub async fn resolve(
        &self,
        inputs: &[TxInput],
        known: &HashMap<OutPoint, PrevOut>,
    ) -> Result<Vec<Option<PrevOut>>> {
        let missing: HashSet<String> = inputs
            .iter()
            .filter_map(|i| {
                let (t, n) = (i.prev_txid.as_deref()?, i.prev_n?);
                (!known.contains_key(&(t.to_string(), n))).then(|| t.to_string())
            })
            .collect();

        // Owned values keep this future `Send` for tokio::spawn.
        let fetched: HashMap<String, Arc<Vec<PrevOut>>> = stream::iter(missing)
            .map(|t| {
                let this = self.clone();
                async move {
                    let o = this.outputs_of(&t).await?;
                    Ok::<_, anyhow::Error>((t, o))
                }
            })
            .buffer_unordered(8)
            .try_filter_map(|(t, o)| async move { Ok(o.map(|o| (t, o))) })
            .try_collect()
            .await?;

        Ok(inputs
            .iter()
            .map(|i| {
                let (t, n) = (i.prev_txid.as_deref()?, i.prev_n?);
                known
                    .get(&(t.to_string(), n))
                    .cloned()
                    .or_else(|| fetched.get(t)?.get(n as usize).cloned())
            })
            .collect())
    }
}
