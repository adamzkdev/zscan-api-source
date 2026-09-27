//! Off-chain reads for NFTs whose image lives outside the chain: ZRC-721 collection metadata
//! (on IPFS, or on the collection's own HTTPS host) and Zerdinals IPFS pointers. Fetched
//! server-side so visitors never contact these hosts, and kept in small in-memory caches.

use std::collections::HashMap;
use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use lru::LruCache;
use zscan_chain::inscriptions::is_cid_path;

/// Largest file relayed.
const MAX_BYTES: usize = 8 << 20;
/// Memory for cached images.
const IMAGE_BUDGET: usize = 96 << 20;
const TIMEOUT: Duration = Duration::from_secs(15);
/// A throttling gateway is skipped this long.
const COOLDOWN: Duration = Duration::from_secs(120);
/// A file no source could deliver isn't requested again for this long.
const RETRY_AFTER: Duration = Duration::from_secs(600);

const DEFAULT_GATEWAYS: &str = "https://gateway.pinit.io/ipfs,https://gateway.pinata.cloud/ipfs";

/// Gateways tried in order (`IPFS_GATEWAY`, comma-separated). Public gateways throttle cloud
/// addresses unevenly, so one answering 429 is skipped for a while.
fn gateways() -> &'static [String] {
    static G: OnceLock<Vec<String>> = OnceLock::new();
    G.get_or_init(|| {
        std::env::var("IPFS_GATEWAY")
            .unwrap_or_else(|_| DEFAULT_GATEWAYS.into())
            .split(',')
            .map(|g| g.trim().trim_end_matches('/').to_string())
            .filter(|g| !g.is_empty())
            .collect()
    })
}

fn until_map(which: u8) -> &'static Mutex<HashMap<String, Instant>> {
    static COOLING: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();
    static FAILED: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();
    match which {
        0 => COOLING.get_or_init(Default::default),
        _ => FAILED.get_or_init(Default::default),
    }
}

fn blocked(which: u8, key: &str) -> bool {
    let mut m = until_map(which).lock().unwrap();
    match m.get(key) {
        Some(until) if *until > Instant::now() => true,
        Some(_) => {
            m.remove(key);
            false
        }
        None => false,
    }
}

fn block(which: u8, key: &str, d: Duration) {
    let mut m = until_map(which).lock().unwrap();
    if m.len() > 50_000 {
        m.retain(|_, until| *until > Instant::now());
    }
    m.insert(key.to_string(), Instant::now() + d);
}

/// `ipfs://X`, `https://<any gateway>/ipfs/X` or a bare CID path, as a checked CID path.
pub fn cid_path(s: &str) -> Option<String> {
    let s = s.trim();
    let p = s
        .strip_prefix("ipfs://")
        .or_else(|| s.split_once("/ipfs/").map(|(_, p)| p))
        .unwrap_or(s);
    is_cid_path(p).then(|| p.to_string())
}

/// Where an off-chain file lives.
#[derive(Debug, PartialEq)]
pub enum Loc {
    Ipfs(String),
    /// A plain HTTPS URL on a named host (never an IP literal, credentials or another port).
    Https(reqwest::Url),
}

impl Loc {
    pub fn parse(s: &str) -> Option<Loc> {
        if let Some(p) = cid_path(s) {
            return Some(Loc::Ipfs(p));
        }
        let url = reqwest::Url::parse(s.trim()).ok()?;
        let named = matches!(url.host(), Some(url::Host::Domain(d)) if d.contains('.'));
        let ok = url.scheme() == "https"
            && named
            && url.port().is_none()
            && url.username().is_empty()
            && url.password().is_none()
            && url.as_str().len() <= 300;
        ok.then_some(Loc::Https(url))
    }

    fn key(&self) -> String {
        match self {
            Loc::Ipfs(p) => format!("ipfs://{p}"),
            Loc::Https(u) => u.to_string(),
        }
    }

    /// `<self>/<item>.json`, for a metadata folder. `None` when `self` is a file.
    fn join_json(&self, item: &str) -> Option<Loc> {
        let base = match self {
            Loc::Ipfs(p) => format!("ipfs://{p}"),
            Loc::Https(u) => u.to_string(),
        };
        if base.ends_with(".json") {
            return None;
        }
        Loc::parse(&format!("{}/{item}.json", base.trim_end_matches('/')))
    }
}

/// Addresses a metadata host may resolve to: public unicast only, so a deploy inscription
/// can't make the API call into private networks.
fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => {
            let o = v.octets();
            !(v.is_private()
                || v.is_loopback()
                || v.is_link_local()
                || v.is_unspecified()
                || v.is_broadcast()
                || v.is_multicast()
                || v.is_documentation()
                || o[0] == 0
                || (o[0] == 100 && (64..128).contains(&o[1]))
                || (o[0] == 198 && (18..20).contains(&o[1]))
                || o[0] >= 240)
        }
        IpAddr::V6(v) => {
            let s = v.segments();
            if let Some(v4) = v.to_ipv4_mapped() {
                return is_public(IpAddr::V4(v4));
            }
            !(v.is_loopback()
                || v.is_unspecified()
                || v.is_multicast()
                || (s[0] & 0xfe00) == 0xfc00
                || (s[0] & 0xffc0) == 0xfe80)
        }
    }
}

/// Caps concurrent off-chain requests so a gallery page doesn't trip rate limits.
fn slots() -> &'static tokio::sync::Semaphore {
    static S: OnceLock<tokio::sync::Semaphore> = OnceLock::new();
    S.get_or_init(|| tokio::sync::Semaphore::new(8))
}

fn gateway_client() -> &'static reqwest::Client {
    static C: OnceLock<reqwest::Client> = OnceLock::new();
    C.get_or_init(|| {
        zscan_chain::http_client_builder()
            .timeout(TIMEOUT)
            .user_agent("zscan/0.1")
            .build()
            .expect("http client")
    })
}

async fn read(res: reqwest::Response) -> Result<(Option<String>, Vec<u8>)> {
    let mut res = res.error_for_status()?;
    let ctype = res
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string());
    let mut body = Vec::new();
    while let Some(chunk) = res.chunk().await? {
        body.extend_from_slice(&chunk);
        if body.len() > MAX_BYTES {
            bail!("file too large");
        }
    }
    Ok((ctype, body))
}

async fn fetch_gateway(gateway: &str, path: &str) -> Result<(Option<String>, Vec<u8>)> {
    let _slot = slots().acquire().await?;
    let res = gateway_client()
        .get(format!("{gateway}/{path}"))
        .send()
        .await?;
    if res.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        block(0, gateway, COOLDOWN);
    }
    read(res).await
}

/// One HTTPS GET pinned to a checked public address, without following redirects.
async fn fetch_https(url: &reqwest::Url) -> Result<(Option<String>, Vec<u8>)> {
    let host = url.host_str().context("no host")?.to_string();
    let addr = tokio::net::lookup_host((host.as_str(), 443))
        .await?
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !addr.is_empty() && addr.iter().all(|a| is_public(a.ip())),
        "{host} does not resolve to a public address"
    );
    let client = zscan_chain::http_client_builder()
        .timeout(TIMEOUT)
        .user_agent("zscan/0.1")
        .redirect(reqwest::redirect::Policy::none())
        .resolve(&host, addr[0])
        .build()?;
    let _slot = slots().acquire().await?;
    read(client.get(url.clone()).send().await?).await
}

/// What a caller expects back; a source answering with something else (e.g. an HTML error
/// page with status 200) doesn't count.
#[derive(Clone, Copy, PartialEq)]
enum Want {
    Json,
    File,
}

fn wanted(want: Want, ctype: Option<&str>, body: &[u8]) -> bool {
    match want {
        Want::Json => {
            serde_json::from_slice::<serde_json::Value>(body).is_ok_and(|v| v.is_object())
        }
        Want::File => !body.is_empty() && !ctype.is_some_and(|c| c.starts_with("text/html")),
    }
}

/// Reads a file. `Ok(None)` when its source answered without it; `Err` when no source could
/// answer (throttled, down, timed out).
async fn get(loc: &Loc, want: Want) -> Result<Option<(Option<String>, Vec<u8>)>> {
    match loc {
        Loc::Https(url) => {
            let (ctype, body) = fetch_https(url).await?;
            Ok(wanted(want, ctype.as_deref(), &body).then_some((ctype, body)))
        }
        Loc::Ipfs(path) => {
            let mut answered = false;
            let mut last_err = None;
            for g in gateways() {
                if blocked(0, g) {
                    continue;
                }
                match fetch_gateway(g, path).await {
                    Ok((ctype, body)) => {
                        answered = true;
                        if wanted(want, ctype.as_deref(), &body) {
                            return Ok(Some((ctype, body)));
                        }
                    }
                    Err(e) => last_err = Some(e),
                }
            }
            match (answered, last_err) {
                (false, Some(e)) => Err(e),
                (false, None) => bail!("all IPFS gateways are cooling down"),
                _ => Ok(None),
            }
        }
    }
}

type Cache<V> = Mutex<LruCache<String, V>>;

fn metadata_cache() -> &'static Cache<serde_json::Value> {
    static C: OnceLock<Cache<serde_json::Value>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(LruCache::new(NonZeroUsize::new(20_000).unwrap())))
}

/// Metadata of a collection item: `<meta>/<item>.json`, where `meta` comes from the deploy.
/// `None` when it can't be read; failures aren't retried for a while.
pub async fn metadata(meta: &str, item: &str) -> Option<serde_json::Value> {
    if !item
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return None;
    }
    let loc = Loc::parse(meta)?.join_json(item)?;
    let key = loc.key();
    if let Some(v) = metadata_cache().lock().unwrap().get(&key) {
        return Some(v.clone());
    }
    // A folder whose files keep failing (e.g. no longer hosted anywhere) is paused as a whole,
    // so each item doesn't wait for its own timeout.
    if blocked(1, &key) || blocked(1, meta) {
        return None;
    }
    match get(&loc, Want::Json).await {
        Ok(Some((_, body))) => {
            let v: serde_json::Value = serde_json::from_slice(&body).ok()?;
            metadata_cache().lock().unwrap().put(key, v.clone());
            Some(v)
        }
        Ok(None) => {
            block(1, &key, RETRY_AFTER);
            None
        }
        Err(e) => {
            tracing::warn!("metadata {key}: {e:#}");
            block(1, &key, RETRY_AFTER);
            if folder_failure(meta) {
                block(1, meta, RETRY_AFTER);
            }
            None
        }
    }
}

/// Counts a failed read under a metadata folder; true once three in a row have failed.
fn folder_failure(meta: &str) -> bool {
    static C: OnceLock<Mutex<LruCache<String, u32>>> = OnceLock::new();
    let mut c = C
        .get_or_init(|| Mutex::new(LruCache::new(NonZeroUsize::new(1_000).unwrap())))
        .lock()
        .unwrap();
    let n = c.get(meta).copied().unwrap_or(0) + 1;
    c.put(meta.to_string(), if n >= 3 { 0 } else { n });
    n >= 3
}

/// The image a metadata document points to (`image`, `img` or `image_url`).
pub fn image_of(metadata: &serde_json::Value) -> Option<String> {
    ["image", "img", "image_url"]
        .iter()
        .find_map(|k| metadata.get(*k)?.as_str())
        .filter(|s| Loc::parse(s).is_some())
        .map(String::from)
}

struct Images {
    lru: LruCache<String, (String, Vec<u8>)>,
    bytes: usize,
}

fn images() -> &'static Mutex<Images> {
    static C: OnceLock<Mutex<Images>> = OnceLock::new();
    C.get_or_init(|| {
        Mutex::new(Images {
            lru: LruCache::unbounded(),
            bytes: 0,
        })
    })
}

/// An image file (IPFS path or HTTPS URL), with its content type. Anything that isn't an
/// image is refused.
pub async fn image(src: &str, declared: Option<&str>) -> Result<(String, Vec<u8>)> {
    let loc = Loc::parse(src).context("unsupported image location")?;
    let key = loc.key();
    if let Some(hit) = images().lock().unwrap().lru.get(&key) {
        return Ok(hit.clone());
    }
    if blocked(1, &key) {
        bail!("image unavailable (retrying later)");
    }
    let found = match get(&loc, Want::File).await {
        Ok(f) => f,
        Err(e) => {
            block(1, &key, RETRY_AFTER);
            return Err(e);
        }
    };
    let Some((ctype, body)) = found else {
        block(1, &key, RETRY_AFTER);
        bail!("no source has this image");
    };
    let ctype = ctype
        .filter(|c| c.starts_with("image/"))
        .or_else(|| {
            declared
                .filter(|c| c.starts_with("image/"))
                .map(String::from)
        })
        .context("file is not an image")?;
    let mut c = images().lock().unwrap();
    c.bytes += body.len();
    if let Some((_, old)) = c.lru.put(key, (ctype.clone(), body.clone())) {
        c.bytes -= old.len();
    }
    while c.bytes > IMAGE_BUDGET {
        match c.lru.pop_lru() {
            Some((_, (_, b))) => c.bytes -= b.len(),
            None => break,
        }
    }
    Ok((ctype, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_ipfs_paths() {
        assert_eq!(
            cid_path("https://ipfs.io/ipfs/bafyimg/9963.png").as_deref(),
            Some("bafyimg/9963.png")
        );
        assert_eq!(cid_path("ipfs://QmAbc").as_deref(), Some("QmAbc"));
        assert_eq!(cid_path("https://evil.example/x.png"), None);
        assert_eq!(cid_path("https://x/ipfs/bafy/../../secret"), None);
    }

    #[test]
    fn https_locations_are_restricted() {
        let ok = |s: &str| Loc::parse(s).is_some();
        assert!(ok("https://zecfruits-assets.pages.dev/v1/metadata/"));
        assert!(!ok("http://example.com/x.json"));
        assert!(!ok("https://127.0.0.1/x.json"));
        assert!(!ok("https://[::1]/x.json"));
        assert!(!ok("https://localhost/x.json"));
        assert!(!ok("https://example.com:8443/x.json"));
        assert!(!ok("https://user:pw@example.com/x.json"));
        assert!(!ok("file:///etc/passwd"));
    }

    #[test]
    fn metadata_paths() {
        let meta = Loc::parse("https://zecfruits-assets.pages.dev/v1/metadata/").unwrap();
        assert_eq!(
            meta.join_json("7").unwrap().key(),
            "https://zecfruits-assets.pages.dev/v1/metadata/7.json"
        );
        let meta = Loc::parse("bafymeta").unwrap();
        assert_eq!(meta.join_json("7").unwrap().key(), "ipfs://bafymeta/7.json");
        // A manifest file isn't a folder of item metadata.
        assert!(Loc::parse("ipfs://bafyx/collection.json")
            .unwrap()
            .join_json("0")
            .is_none());
    }

    #[test]
    fn private_addresses_are_refused() {
        for ip in [
            "10.0.0.1",
            "192.168.1.1",
            "127.0.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "fd00::1",
            "::1",
            "::ffff:10.0.0.1",
        ] {
            assert!(!is_public(ip.parse().unwrap()), "{ip}");
        }
        assert!(is_public("104.16.0.1".parse().unwrap()));
    }
}
