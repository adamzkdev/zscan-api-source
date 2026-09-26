# Builds the indexer and API binaries into one small runtime image.
FROM rust:1-bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Dependencies are cached in their own layer.
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release -p zscan-indexer -p zscan-api

# Distroless: glibc + CA certificates, no shell or package manager.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /app/target/release/zscan-indexer /app/target/release/zscan-api /usr/local/bin/
