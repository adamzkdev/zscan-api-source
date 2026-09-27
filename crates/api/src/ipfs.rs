//! IPFS reads for NFTs whose image lives off chain (ZRC-721 collection metadata, Zerdinals IPFS
//! pointers). Fetched server-side through public gateways so visitors never contact them, and
//! kept in small in-memory caches: IPFS content is immutable.

use std::num::NonZeroUsize;
use std::sync::{Mutex, OnceLock};

use anyhow::{bail, Context, Result};
use lru::LruCache;
use zscan_chain::inscriptions::is_cid_path;

/// Largest file relayed.
const MAX_BYTES: usize = 8 << 20;
/// Memory for cached images.
const IMAGE_BUDGET: usize = 96 << 20;

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

const DEFAULT_GATEWAYS: &str = "https://gateway.pinit.io/ipfs,https://gateway.pinata.cloud/ipfs";
const COOLDOWN: std::time::Duration = std::time::Duration::from_secs(120);

fn cooling() -> &'static Mutex<std::collections::HashMap<String, std::time::Instant>> {
    static C: OnceLock<Mutex<std::collections::HashMap<String, std::time::Instant>>> =
        OnceLock::new();
    C.get_or_init(Default::default)
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

/// Gateways are slow on first reads; requests are capped so a gallery page doesn't trip their
/// rate limits.
fn client() -> &'static (reqwest::Client, tokio::sync::Semaphore) {
    static C: OnceLock<(reqwest::Client, tokio::sync::Semaphore)> = OnceLock::new();
    C.get_or_init(|| {
        let http = zscan_chain::http_client_builder()
            .timeout(std::time::Duration::from_secs(30))
            .user_agent("zscan/0.1")
            .build()
            .expect("http client");
        (http, tokio::sync::Semaphore::new(8))
    })
}

/// What a caller expects back; a gateway answering with something else (e.g. an HTML error
/// page with status 200) is skipped.
#[derive(Clone, Copy, PartialEq)]
enum Want {
    Json,
    File,
}

async fn fetch(gateway: &str, path: &str) -> Result<(Option<String>, Vec<u8>)> {
    let (http, slots) = client();
    let _slot = slots.acquire().await?;
    let res = http.get(format!("{gateway}/{path}")).send().await?;
    if res.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        cooling()
            .lock()
            .unwrap()
            .insert(gateway.to_string(), std::time::Instant::now() + COOLDOWN);
    }
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
            bail!("IPFS file too large");
        }
    }
    Ok((ctype, body))
}

/// Reads a file from the first gateway that returns what's wanted. `Ok(None)` when every
/// gateway answered but none had it; `Err` when none could answer (throttled, down).
async fn get(path: &str, want: Want) -> Result<Option<(Option<String>, Vec<u8>)>> {
    let Some(path) = cid_path(path) else {
        bail!("not an IPFS path");
    };
    let mut answered = false;
    let mut last_err = None;
    for g in gateways() {
        let cool = cooling().lock().unwrap().get(g).copied();
        if cool.is_some_and(|until| until > std::time::Instant::now()) {
            continue;
        }
        match fetch(g, &path).await {
            Ok((ctype, body)) => {
                answered = true;
                let html = ctype.as_deref().is_some_and(|c| c.starts_with("text/html"));
                let ok = match want {
                    Want::Json => serde_json::from_slice::<serde_json::Value>(&body)
                        .is_ok_and(|v| v.is_object()),
                    Want::File => !html && !body.is_empty(),
                };
                if ok {
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

type Cache<V> = Mutex<LruCache<String, V>>;

fn metadata_cache() -> &'static Cache<Option<serde_json::Value>> {
    static C: OnceLock<Cache<Option<serde_json::Value>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(LruCache::new(NonZeroUsize::new(20_000).unwrap())))
}

/// Metadata of a collection item: `<meta>/<item>.json`. `None` when it can't be read (cached
/// too, so a missing file isn't requested again).
pub async fn metadata(meta: &str, item: &str) -> Option<serde_json::Value> {
    if !item
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return None;
    }
    let key = format!("{meta}/{item}.json");
    if let Some(v) = metadata_cache().lock().unwrap().get(&key) {
        return v.clone();
    }
    let v = match get(&key, Want::Json).await {
        Ok(found) => found.and_then(|(_, body)| serde_json::from_slice(&body).ok()),
        Err(e) => {
            tracing::warn!("ipfs metadata {key}: {e:#}");
            // Transient failures aren't cached.
            return None;
        }
    };
    metadata_cache().lock().unwrap().put(key, v.clone());
    v
}

/// The image path a metadata document points to (`image` or `img`).
pub fn image_of(metadata: &serde_json::Value) -> Option<String> {
    ["image", "img", "image_url"]
        .iter()
        .find_map(|k| metadata.get(*k)?.as_str())
        .and_then(cid_path)
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

/// An image file, with its content type. Anything that isn't an image is refused.
pub async fn image(path: &str, declared: Option<&str>) -> Result<(String, Vec<u8>)> {
    if let Some(hit) = images().lock().unwrap().lru.get(path) {
        return Ok(hit.clone());
    }
    let (ctype, body) = get(path, Want::File)
        .await?
        .context("no IPFS gateway has this file")?;
    let ctype = ctype
        .filter(|c| c.starts_with("image/"))
        .or_else(|| {
            declared
                .filter(|c| c.starts_with("image/"))
                .map(String::from)
        })
        .context("IPFS file is not an image")?;
    let mut c = images().lock().unwrap();
    c.bytes += body.len();
    if let Some((_, old)) = c.lru.put(path.to_string(), (ctype.clone(), body.clone())) {
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
}
