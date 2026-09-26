#!/usr/bin/env bash
# Build ZScan on the host and run it in Docker: mainnet (web :3000, API :8080) and testnet
# (web :3001, API :8090). Works even when Docker's build network is blocked by a proxy.
#
# Requirements: rustup target aarch64/x86_64-unknown-linux-gnu, zig + cargo-zigbuild, node 20+.
set -euo pipefail
cd "$(dirname "$0")/.."

case "$(docker version --format '{{.Server.Arch}}')" in
  arm64|aarch64) TARGET=aarch64-unknown-linux-gnu ;;
  *) TARGET=x86_64-unknown-linux-gnu ;;
esac

echo "==> Rust binaries for $TARGET (glibc 2.36, distroless debian12)"
cargo zigbuild --release --target "$TARGET.2.36" -p zscan-indexer -p zscan-api -p zscan-verify
mkdir -p docker/bin
cp "target/$TARGET/release/"zscan-{indexer,api,verify} docker/bin/

echo "==> Web standalone build"
(cd web && npm ci --silent && npm run build)

PROFILES=(--profile testnet)
[[ "${NO_TESTNET:-}" == "1" ]] && PROFILES=()

echo "==> docker compose up"
docker compose -f docker-compose.yml -f docker-compose.local.yml build indexer web
docker compose -f docker-compose.yml -f docker-compose.local.yml ${PROFILES[@]+"${PROFILES[@]}"} up -d
docker compose -f docker-compose.yml -f docker-compose.local.yml ${PROFILES[@]+"${PROFILES[@]}"} ps
echo
echo "Mainnet: http://localhost:${WEB_PORT:-3000}   API docs: http://localhost:${API_PORT:-8080}/docs"
[[ ${#PROFILES[@]} -gt 0 ]] && echo "Testnet: http://localhost:${TESTNET_WEB_PORT:-3001}   API docs: http://localhost:${TESTNET_API_PORT:-8090}/docs"
