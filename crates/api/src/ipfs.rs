//! IPFS reads for NFTs whose image lives off chain (ZRC-721 collection metadata, Zerdinals IPFS
//! pointers). Fetched server-side through one gateway so visitors never contact it, and kept in
//! small in-memory caches: IPFS content is immutable.

use std::num::NonZeroUsize;
use std::sync::{Mutex, OnceLock};

use anyhow::{bail, Context, Result};
use lru::LruCache;
use zscan_chain::inscriptions::is_cid_path;

/// Largest file relayed.
const MAX_BYTES: usize = 8 << 20;
/// Memory for cached images.
const IMAGE_BUDGET: usize = 96 << 20;

fn gateway() -> &'static str {
    static G: OnceLock<String> = OnceLock::new();
    G.get_or_init(|| {
        std::env::var("IPFS_GATEWAY")
            .unwrap_or_else(|_| "https://gateway.pinata.cloud/ipfs".into())
            .trim_end_matches('/')
            .to_string()
    })
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

async fn get(path: &str) -> Result<(Option<String>, Vec<u8>)> {
    let Some(path) = cid_path(path) else {
        bail!("not an IPFS path");
    };
    let (http, slots) = client();
    let _slot = slots.acquire().await?;
    let mut res = http
        .get(format!("{}/{path}", gateway()))
        .send()
        .await?
        .error_for_status()?;
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
    let v = match get(&key).await {
        Ok((_, body)) => serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .filter(|v| v.is_object()),
        Err(e) => {
            tracing::debug!("ipfs metadata {key}: {e:#}");
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
    let (ctype, body) = get(path).await?;
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
