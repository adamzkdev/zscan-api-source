-- ZEC markets: centralized and on-chain venues, spot and perpetuals. The latest figures per venue,
-- refreshed every few minutes from each venue's public API.
CREATE TABLE market_venues (
    venue          TEXT NOT NULL,
    -- 'spot' or 'perp'.
    market         TEXT NOT NULL,
    pair           TEXT NOT NULL,
    -- 'cex' or 'dex' (on-chain order books and AMMs).
    kind           TEXT NOT NULL,
    price          DOUBLE PRECISION,
    volume_24h_usd DOUBLE PRECISION,
    bid            DOUBLE PRECISION,
    ask            DOUBLE PRECISION,
    -- Perpetuals: open interest and the funding rate normalized to 8 hours.
    oi_usd         DOUBLE PRECISION,
    oi_zec         DOUBLE PRECISION,
    funding_8h     DOUBLE PRECISION,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (venue, market)
);

-- Daily history per venue: close and USD volume from the venue's daily candles, open interest
-- (end of day) and the mean 8-hour funding rate from its history endpoints or our snapshots.
CREATE TABLE market_daily (
    venue      TEXT NOT NULL,
    market     TEXT NOT NULL,
    day        DATE NOT NULL,
    close      DOUBLE PRECISION,
    volume_usd DOUBLE PRECISION,
    oi_usd     DOUBLE PRECISION,
    funding_8h DOUBLE PRECISION,
    PRIMARY KEY (venue, market, day)
);
CREATE INDEX market_daily_day_idx ON market_daily (day);

-- ZEC on other chains (labels/zec_assets.json): supply read from each chain, DEX liquidity and
-- volume across its pools.
CREATE TABLE zec_assets_latest (
    asset          TEXT PRIMARY KEY,
    -- ZEC (not zatoshis: these are other chains' tokens, often with 18 decimals).
    supply         DOUBLE PRECISION,
    -- Part of the supply locked to back other listed assets (their `backed_by`).
    locked         DOUBLE PRECISION,
    price_usd      DOUBLE PRECISION,
    liquidity_usd  DOUBLE PRECISION,
    volume_24h_usd DOUBLE PRECISION,
    -- Top pools: [{name, dex, liquidity_usd, volume_24h_usd, url}].
    pools          JSONB,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE zec_assets_daily (
    asset         TEXT NOT NULL,
    day           DATE NOT NULL,
    supply        DOUBLE PRECISION,
    liquidity_usd DOUBLE PRECISION,
    volume_usd    DOUBLE PRECISION,
    PRIMARY KEY (asset, day)
);
