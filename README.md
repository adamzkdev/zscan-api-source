# ZScan

Open Zcash block explorer and data API — **[zscan.cash](https://zscan.cash)**.

- **Explorer**: blocks, transactions (v1–v6, including Ironwood), transparent addresses, mempool, network stats.
- **Ironwood tracker**: Orchard → Ironwood migration (NU6.3), pool balances over time, daily flows, supply-integrity checks.
- **Public API**: REST + WebSocket, OpenAPI spec at `/openapi.json`, interactive docs at `/docs`.
- **Supply & issuance**: live supply, inflation, per-block split (miners / ZCG / coinholder fund), NU7 fee burn,
  halving countdown — computed from Zcash consensus rules (`zebra-chain`).
- **Independent supply audit**: every node snapshot is reconciled against block-by-block decoded pool changes
  (7 dimensions, to the zatoshi). Reproduce it yourself with `zscan-verify`.
- **Dev fund tracker, anonymity sets, Orchard exit flows, ZIP-317 fee tracker, Nakamoto coefficient,
  Unified Address inspector, CSV export, reorg log.**
- **Privacy-respecting**: no IP logs, no trackers, no ads, no cookies. Shielded data stays shielded; we only show
  what the chain makes public. MIT licensed.

## Verify the supply yourself

```bash
cargo run --release -p zscan-verify -- 2     # waits for 2 new blocks, decodes them, compares with the node
```

## Architecture

```
crates/
  chain/    lightwalletd gRPC client (round-robin, rate limit, retries), zebrad JSON-RPC client,
            tx decoding via zebra-chain (v6/Ironwood), classification, prevout resolution
  db/       Postgres schema + writes, block builder shared by indexer and API
  indexer/  follows the tip (reorg-safe), backfills down to START_HEIGHT, pool snapshots, mempool
  api/      axum REST API, WebSocket (Postgres LISTEN/NOTIFY), OpenAPI (utoipa), per-IP rate limit
  verify/   zscan-verify CLI: trustless reproduction of the supply audit
labels/     open address labels (mining pools from coinbase signatures), PRs welcome
web/        Next.js 16 frontend (SSR), Recharts
```

**Data sources** (all configurable):

| Source | Used for | Default |
|---|---|---|
| lightwalletd / Zaino gRPC | blocks, raw txs, address balances, mempool, broadcast | `zec.rocks`, `na.zec.rocks`, `eu.zec.rocks` (50 req/s each) |
| zebrad JSON-RPC | authoritative value-pool balances, difficulty | Tatum public gateway (4 req/min) |

Pool history is **derived exactly**: every block's pool deltas come from decoded value balances and resolved
transparent inputs, anchored to the node's `valuePools`. Verified to the zatoshi against consecutive node snapshots.

## Run locally

```bash
docker compose up -d db
export DATABASE_URL=postgres://zscan:zscan@127.0.0.1:5433/zscan
cargo run --release -p zscan-indexer     # starts at the tip, backfills to NU6.3
cargo run --release -p zscan-api         # http://127.0.0.1:8080/docs
cd web && npm install && npm run dev     # http://localhost:3000
```

Everything in Docker, mainnet **and** testnet, built on the host (works even when Docker's build network is
restricted by a proxy; needs `zig`, `cargo-zigbuild` and the Linux Rust target):

```bash
./scripts/docker-local.sh
# Mainnet  http://localhost:3000   API http://localhost:8080/docs
# Testnet  http://localhost:3001   API http://localhost:8090/docs
```

Or build the images inside Docker:

```bash
cp .env.example .env
docker compose up -d --build             # web :3000, API :8080
docker compose --profile prod up -d      # + Caddy (HTTPS for $SITE_DOMAIN) and a Tor onion service
docker compose --profile testnet up -d   # testnet instance on :3001 (NU7 readiness)
```

## Self-hosted node

Public endpoints are rate-limited and only practical for recent history. For full history and no limits, run the
Zcash Foundation's [Z3](https://github.com/ZcashFoundation/z3) stack (Zebra + Zaino) and attach ZScan to it:

```bash
# in z3/
docker compose --profile indexer up -d
# in zscan/
START_HEIGHT=0 API_PORT=8081 docker compose -f docker-compose.yml -f docker-compose.selfhost.yml up -d --build
```

## Configuration

| Variable | Service | Default |
|---|---|---|
| `DATABASE_URL` | indexer, api | — |
| `ZSCAN_NETWORK` | indexer, api | `mainnet` (or `testnet`) |
| `LWD_URL` | indexer, api | zec.rocks endpoints (comma-separated) |
| `LWD_RATE_PER_SEC` | indexer, api | 50 / 20 per endpoint, `0` = unlimited |
| `LWD_IPV4_ONLY` | indexer, api | `1` (skip IPv6 on hosts without IPv6 routing) |
| `RPC_URL`, `RPC_RATE_PER_MIN` | indexer | Tatum zebrad, 4 |
| `START_HEIGHT` | indexer | NU6.3 activation of the network; or `tip-N` |
| `BACKFILL_CHUNK`, `BACKFILL_CONCURRENCY` | indexer | 200, 4 |
| `API_ADDR`, `API_RATE_PER_SEC` | api | `0.0.0.0:8080`, 10 |
| `API_INTERNAL_URL` | web | `http://127.0.0.1:8080` (read at runtime) |
| `ZSCAN_NETWORK`, `PUBLIC_WS_URL`, `MAINNET_URL`, `TESTNET_URL` | web | network badge, live events, network switcher |
| `ZSCAN_HTTP_PROXY` | indexer, api | none (ambient `HTTP(S)_PROXY` is ignored) |

## Tests

```bash
cargo test
cd web && npx tsc --noEmit && npx eslint .
```
