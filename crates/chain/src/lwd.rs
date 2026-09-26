//! lightwalletd / Zaino gRPC client (CompactTxStreamer).

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use hyper_util::client::legacy::connect::HttpConnector;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::hash;
use crate::limiter::Limiter;
use crate::proto::{
    compact_tx_streamer_client::CompactTxStreamerClient, AddressList, BlockId, BlockRange,
    ChainSpec, CompactBlock, Empty, LightdInfo, PoolType, RawTransaction, SendResponse,
    TransparentAddressBlockFilter, TxFilter,
};

const MAX_MESSAGE: usize = 64 * 1024 * 1024;
const MAX_RETRIES: u32 = 8;

/// Pool types requested from `GetBlockRange` so compact blocks include every transaction,
/// with transparent inputs/outputs.
const ALL_POOLS: [PoolType; 4] = [
    PoolType::Transparent,
    PoolType::Sapling,
    PoolType::Orchard,
    PoolType::Ironwood,
];

#[derive(Clone)]
pub struct Lwd {
    clients: Arc<Vec<CompactTxStreamerClient<Channel>>>,
    next: Arc<AtomicUsize>,
    limiter: Limiter,
}

type Client = CompactTxStreamerClient<Channel>;

impl Lwd {
    /// Connects lazily to one or more comma-separated gRPC endpoints such as
    /// `https://zec.rocks:443,https://na.zec.rocks:443`. Requests are spread round-robin and
    /// limited to `per_second_each` per endpoint (0 = unlimited, e.g. a self-hosted Zaino).
    pub fn connect_with(urls: &str, per_second_each: f64) -> Result<Self> {
        let clients = urls
            .split(',')
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .map(|url| {
                let mut endpoint = Endpoint::from_shared(url.to_string())
                    .context("invalid lightwalletd url")?
                    .connect_timeout(Duration::from_secs(4))
                    .timeout(Duration::from_secs(120))
                    .tcp_keepalive(Some(Duration::from_secs(30)))
                    .http2_keep_alive_interval(Duration::from_secs(30));
                if url.starts_with("https") {
                    endpoint = endpoint.tls_config(ClientTlsConfig::new().with_webpki_roots())?;
                }
                Ok(
                    Client::new(endpoint.connect_with_connector_lazy(tcp_connector()))
                        .max_decoding_message_size(MAX_MESSAGE),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        anyhow::ensure!(!clients.is_empty(), "no lightwalletd url configured");
        let limiter = Limiter::per_second(per_second_each * clients.len() as f64);
        Ok(Self {
            clients: Arc::new(clients),
            next: Arc::new(AtomicUsize::new(0)),
            limiter,
        })
    }

    /// Single endpoint, no client-side rate limit.
    pub fn connect(url: &str) -> Result<Self> {
        Self::connect_with(url, 0.0)
    }

    fn c(&self) -> Client {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        self.clients[i].clone()
    }

    /// Runs a unary call with rate limiting and retries on throttling / transient errors.
    async fn call<T, F, Fut>(&self, f: F) -> Result<T, tonic::Status>
    where
        F: Fn(Client) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    {
        let mut delay = Duration::from_millis(250);
        let mut attempt = 0;
        loop {
            self.limiter.wait().await;
            match f(self.c()).await {
                Ok(r) => return Ok(r.into_inner()),
                Err(s) if is_retryable(&s) && attempt < MAX_RETRIES => {
                    tracing::debug!(code = ?s.code(), attempt, "lightwalletd retry: {}", s.message());
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(10));
                    attempt += 1;
                }
                Err(s) => return Err(s),
            }
        }
    }

    pub async fn info(&self) -> Result<LightdInfo> {
        Ok(self
            .call(|mut c| async move { c.get_lightd_info(Empty {}).await })
            .await?)
    }

    /// Returns the tip height and hash (display hex).
    pub async fn tip(&self) -> Result<(u32, String)> {
        let id = self
            .call(|mut c| async move { c.get_latest_block(ChainSpec {}).await })
            .await?;
        Ok((id.height as u32, hash::to_display(&id.hash)))
    }

    /// Streams compact blocks for `[start, end]` inclusive, with transparent data.
    pub async fn block_range(
        &self,
        start: u32,
        end: u32,
    ) -> Result<tonic::Streaming<CompactBlock>> {
        let req = BlockRange {
            start: Some(BlockId {
                height: start as u64,
                hash: vec![],
            }),
            end: Some(BlockId {
                height: end as u64,
                hash: vec![],
            }),
            pool_types: ALL_POOLS.iter().map(|p| *p as i32).collect(),
        };
        Ok(self
            .call(|mut c| {
                let req = req.clone();
                async move { c.get_block_range(req).await }
            })
            .await?)
    }

    /// Fetches a single compact block with transparent data.
    pub async fn block(&self, height: u32) -> Result<Option<CompactBlock>> {
        let mut s = match self.block_range(height, height).await {
            Ok(s) => s,
            Err(e) => {
                if is_not_found(&e) {
                    return Ok(None);
                }
                return Err(e);
            }
        };
        Ok(s.message().await?)
    }

    /// Fetches a raw transaction by display-order txid. Returns bytes and the mined height
    /// (0 = mempool).
    pub async fn raw_tx(&self, txid: &str) -> Result<Option<RawTransaction>> {
        let hash = hash::from_display(txid).context("invalid txid")?;
        let filter = TxFilter {
            block: None,
            index: 0,
            hash,
        };
        match self
            .call(|mut c| {
                let filter = filter.clone();
                async move { c.get_transaction(filter).await }
            })
            .await
        {
            Ok(r) => Ok(Some(r)),
            Err(s)
                if matches!(
                    s.code(),
                    tonic::Code::NotFound | tonic::Code::InvalidArgument
                ) =>
            {
                Ok(None)
            }
            Err(s)
                if s.message().contains("No such mempool or")
                    || s.message().contains("not found") =>
            {
                Ok(None)
            }
            Err(s) => Err(s.into()),
        }
    }

    pub async fn taddr_balance(&self, address: &str) -> Result<i64> {
        let list = AddressList {
            addresses: vec![address.to_string()],
        };
        let r = self
            .call(|mut c| {
                let list = list.clone();
                async move { c.get_taddress_balance(list).await }
            })
            .await?;
        Ok(r.value_zat)
    }

    /// Raw transactions touching a t-address in `[start, end]`.
    pub async fn taddr_txs(
        &self,
        address: &str,
        start: u32,
        end: u32,
    ) -> Result<Vec<RawTransaction>> {
        let req = TransparentAddressBlockFilter {
            address: address.to_string(),
            range: Some(BlockRange {
                start: Some(BlockId {
                    height: start as u64,
                    hash: vec![],
                }),
                end: Some(BlockId {
                    height: end as u64,
                    hash: vec![],
                }),
                pool_types: vec![],
            }),
        };
        let mut s = self
            .call(|mut c| {
                let req = req.clone();
                async move { c.get_taddress_txids(req).await }
            })
            .await?;
        let mut out = Vec::new();
        while let Some(tx) = s.message().await? {
            out.push(tx);
        }
        Ok(out)
    }

    /// Streams mempool transactions; the stream ends when a new block is mined.
    pub async fn mempool_stream(&self) -> Result<tonic::Streaming<RawTransaction>> {
        Ok(self
            .call(|mut c| async move { c.get_mempool_stream(Empty {}).await })
            .await?)
    }

    pub async fn send(&self, raw: Vec<u8>) -> Result<SendResponse> {
        Ok(self
            .c()
            .send_transaction(RawTransaction {
                data: raw,
                height: 0,
            })
            .await?
            .into_inner())
    }
}

/// TCP connector for gRPC channels. Hosts without IPv6 routing get `AddrNotAvailable` when a
/// server advertises AAAA records, so by default we bind IPv4, which makes hyper skip IPv6
/// addresses. Set `LWD_IPV4_ONLY=0` to allow IPv6.
fn tcp_connector() -> HttpConnector {
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    http.set_nodelay(true);
    http.set_keepalive(Some(Duration::from_secs(30)));
    if std::env::var("LWD_IPV4_ONLY")
        .map(|v| v != "0")
        .unwrap_or(true)
    {
        http.set_local_address(Some(IpAddr::V4(Ipv4Addr::UNSPECIFIED)));
    }
    http
}

fn is_retryable(s: &tonic::Status) -> bool {
    matches!(
        s.code(),
        tonic::Code::ResourceExhausted | tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
    ) || (s.code() == tonic::Code::Unknown
        && (s.message().contains("rate limit") || s.message().contains("transport error")))
}

fn is_not_found(e: &anyhow::Error) -> bool {
    e.downcast_ref::<tonic::Status>()
        .map(|s| {
            matches!(
                s.code(),
                tonic::Code::NotFound | tonic::Code::OutOfRange | tonic::Code::InvalidArgument
            )
        })
        .unwrap_or(false)
}
