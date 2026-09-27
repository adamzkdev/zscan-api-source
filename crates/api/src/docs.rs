use axum::{response::Html, Json};
use serde::Serialize;
use serde_json::{json, Value};
use utoipa::{
    openapi::{
        server::{Server, ServerBuilder},
        ContactBuilder, LicenseBuilder,
    },
    OpenApi, ToSchema,
};

use crate::models::{BlockSummary, PoolValues, TxRow};
use crate::routes::{
    address, blocks, bridges, compact, fees, inscriptions, labels, network, pools, price,
    protocols, rankings, search, status, supply, tx, upgrades,
};

/// Error body returned with every 4xx/5xx response.
#[derive(Serialize, ToSchema)]
pub struct ErrorBody {
    /// Machine-readable code, e.g. `not_found`, `invalid_txid`, `shielded_address`, `rate_limited`.
    pub error: String,
    pub message: String,
}

const GUIDE: &str = r#"The free, open data API behind [ZScan](https://zscan.cash): blocks, transactions (including Ironwood v6),
transparent addresses, value pools, the Orchard → Ironwood migration, supply & issuance with an independent audit,
grants and lockbox flows, anonymity sets, fees, miners, mempool and live events.

## Quick start

```bash
curl https://zscan.cash/v1/supply
curl https://zscan.cash/v1/tx/39d1e0130e5f8381c6960cdf6e0930ea9351f3fd1b405243ebf3561d733b294c
curl "https://zscan.cash/v1/txs?kind=migration&limit=5"
```

```js
const supply = await fetch("https://zscan.cash/v1/supply").then((r) => r.json());
console.log(supply.supply / 1e8, "ZEC");
```

```python
import requests
audit = requests.get("https://zscan.cash/v1/supply/audit").json()
print(audit["summary"])   # {'checks': ..., 'failed': 0, ...}
```

## Conventions

- **Amounts are integers in zatoshis** (1 ZEC = 100,000,000 zat). Value balances are positive when value *leaves* a pool.
- **Hashes and txids** are hex in display order, as shown by every Zcash explorer and wallet.
- **Times** are ISO-8601 UTC.
- **Pagination**: list endpoints take `limit` (max 100) and either `before` (a block height, for time-ordered lists)
  or `page` (zero-based).
- **Coverage**: the index starts at `GET /v1/status → indexed_low`. Transactions, blocks and address balances outside
  it are still served, decoded live from the network; per-address history and aggregates cover the indexed range.

## Transaction kinds

`coinbase` · `transparent` · `shielding` (transparent → shielded) · `deshielding` (shielded → transparent) ·
`shielded` (fully private) · `migration` (Orchard → Ironwood) · `mixed`.

## Rate limits

Anonymous access is free: **10 requests/second per IP, bursts of 30**. Exceeding it returns `429` with
`{"error": "rate_limited"}`. Need more? API keys with higher limits are coming. Or run your own instance: ZScan is MIT
licensed and ships with Docker Compose.

## Errors

Every error uses the same body: `{"error": "<code>", "message": "<human text>"}`. Codes include `not_found`,
`invalid_id`, `invalid_txid`, `invalid_address`, `shielded_address`, `invalid_unified_address`, `invalid_period`,
`invalid_interval`, `rate_limited` and `internal`.

## Live events (WebSocket)

Connect to `wss://zscan.cash/v1/ws`. Messages are JSON:

```json
{"type": "block",   "data": {"height": 3497265, "hash": "0000…", "tx_count": 14}}
{"type": "mempool", "data": {"txid": "…", "kind": "shielded", "fee": 10000, "size": 9312}}
```

## Networks

Mainnet is served at `https://zscan.cash`, testnet at `https://testnet.zscan.cash`, with the same API.
`GET /v1/health → network` tells you which one you're talking to.

## Verify, don't trust

`GET /v1/supply/audit` reconciles every node snapshot against pool changes decoded from each block, to the zatoshi.
Reproduce it independently with the open-source `zscan-verify` CLI.

## Privacy

No API keys required, no IP logging, no cookies. Shielded data stays shielded: the API only exposes what the Zcash
protocol makes public."#;

#[derive(OpenApi)]
#[openapi(
    info(title = "ZScan API", version = "1.1.0"),
    paths(
        network::status, blocks::list, blocks::detail, tx::list, tx::detail, tx::broadcast,
        address::detail, address::csv, address::unified, search::search,
        pools::current, pools::history, pools::migration,
        supply::supply, supply::audit, supply::funding, supply::anonymity,
        network::mempool, network::network, network::daily, fees::fees,
        rankings::miners, rankings::accounts, price::price, price::history, status::health, upgrades::list, labels::list, protocols::list, protocols::detail,
        compact::range, compact::tx_outputs, bridges::overview,
        inscriptions::list, inscriptions::stats, inscriptions::detail, inscriptions::content,
        inscriptions::image, inscriptions::by_address, inscriptions::collections,
        inscriptions::collection,
        crate::ws::handler,
    ),
    components(schemas(BlockSummary, TxRow, PoolValues, ErrorBody)),
    tags(
        (name = "blocks", description = "Blocks, with decoded transactions. Older blocks are decoded live."),
        (name = "transactions", description = "Decoded transactions (v1–v6, including Ironwood), lists by kind, broadcast."),
        (name = "addresses", description = "Transparent address balances and history, CSV export, Unified Address inspection. Shielded addresses are private by design."),
        (name = "supply", description = "ZEC supply and issuance from consensus rules, the independent supply audit, grants and lockbox flows and anonymity sets."),
        (name = "pools", description = "Value pool balances over time and the Orchard → Ironwood migration (NU6.3)."),
        (name = "network", description = "Network stats, fees (ZIP-317), mempool, miners, price, service health and live events."),
        (name = "search", description = "Resolve any height, hash, txid or address."),
        (name = "protocols", description = "Holdings and flows of protocols and entities touching ZEC: shielded pools, treasuries, bridges, mining pools, exchanges, and cross-chain flows."),
        (name = "inscriptions", description = "Inscriptions (Zerdinals) inscribed in transparent scriptSigs: on-chain images and files, ZRC-721 collections and IPFS NFTs. Ownership follows transparent outputs; spending into a shielded transaction ends tracking. Token documents (ZRC-20) are numbered but not indexed."),
        (name = "labels", description = "Address name tags, each with a verifiable source."),
        (name = "shielded", description = "Compact blocks and full note ciphertexts for client-side scanning with a viewing key. Keys never reach the server."),
    )
)]
struct ApiDoc;

pub fn openapi() -> utoipa::openapi::OpenApi {
    let mut doc = ApiDoc::openapi();
    doc.info.description = Some(GUIDE.to_string());
    doc.info.contact = Some(
        ContactBuilder::new()
            .name(Some("ZScan"))
            .url(Some("https://zscan.cash"))
            .build(),
    );
    doc.info.license = Some(
        LicenseBuilder::new()
            .name("MIT")
            .identifier(Some("MIT"))
            .build(),
    );
    let server = |url: &str, desc: &str| -> Server {
        ServerBuilder::new()
            .url(url)
            .description(Some(desc))
            .build()
    };
    doc.servers = Some(vec![
        server("https://zscan.cash", "Mainnet"),
        server("https://testnet.zscan.cash", "Testnet"),
        server("http://localhost:8080", "Local instance"),
    ]);
    doc
}

/// `GET /v1`: a machine-readable index of the API.
pub async fn index() -> Json<Value> {
    Json(json!({
        "name": "ZScan API",
        "version": "1.1.0",
        "network": if zscan_chain::is_testnet() { "testnet" } else { "mainnet" },
        "docs": "/docs",
        "openapi": "/openapi.json",
        "websocket": "/v1/ws",
        "endpoints": [
            "/v1/status", "/v1/health", "/v1/blocks", "/v1/blocks/{id}", "/v1/txs", "/v1/tx/{txid}", "/v1/tx/broadcast",
            "/v1/address/{address}", "/v1/address/{address}/csv", "/v1/unified/{address}", "/v1/search?q=",
            "/v1/pools", "/v1/pools/history", "/v1/ironwood/migration", "/v1/supply", "/v1/supply/audit", "/v1/funding",
            "/v1/privacy/anonymity-sets", "/v1/network", "/v1/stats/daily", "/v1/fees", "/v1/mempool", "/v1/miners",
            "/v1/accounts", "/v1/price", "/v1/price/history", "/v1/upgrades", "/v1/labels", "/v1/protocols", "/v1/protocols/{slug}", "/v1/bridges"
        ],
        "amounts": "zatoshis (1 ZEC = 100,000,000 zat)",
        "rate_limit": "10 req/s per IP, burst 30"
    }))
}

pub async fn page() -> Html<&'static str> {
    Html(
        r##"<!doctype html>
<html><head><title>ZScan API Reference</title><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<meta name="description" content="Free, open Zcash data API: blocks, transactions, supply audit, Ironwood migration, fees, miners."/></head>
<body>
<script id="api-reference" data-url="/openapi.json" data-configuration='{"theme":"default","layout":"modern","hideDownloadButton":false,"defaultHttpClient":{"targetKey":"shell","clientKey":"curl"},"metaData":{"title":"ZScan API Reference"},"customCss":":root{--scalar-color-accent:#0784c3}"}'></script>
<!-- Served by the web app (web/public/vendor) so readers never hit a third-party CDN. -->
<script src="/vendor/scalar-api-reference-1.72.1.js"></script>
</body></html>"##,
    )
}
