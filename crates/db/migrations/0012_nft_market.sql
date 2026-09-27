-- Marketplace history for NFT projects, as each venue reports it. Venues publish only their
-- current order book and latest sales, so ZScan keeps the history itself.

-- Order-book figures, sampled every few minutes.
CREATE TABLE nft_market_snapshots (
    project TEXT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    floor BIGINT,
    listings INTEGER,
    bids INTEGER,
    volume BIGINT,
    sales BIGINT,
    owners INTEGER,
    PRIMARY KEY (project, time)
);

-- Sales of items kept off chain (on-chain sales are in inscription_events). Amounts in zatoshis.
CREATE TABLE nft_market_sales (
    project TEXT NOT NULL,
    item TEXT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    price BIGINT NOT NULL,
    -- ask (bought at the listed price) | offer (a bid accepted)
    via TEXT,
    PRIMARY KEY (project, item, time)
);
CREATE INDEX nft_market_sales_time_idx ON nft_market_sales (project, time DESC);

-- Progress of the one-time import of each project's past sales (next item id to read).
CREATE TABLE nft_market_backfill (
    project TEXT PRIMARY KEY,
    next_item BIGINT NOT NULL,
    done BOOLEAN NOT NULL DEFAULT false
);

-- Pictures of off-chain items, kept so pages don't depend on the venue being reachable.
CREATE TABLE nft_item_images (
    project TEXT NOT NULL,
    item TEXT NOT NULL,
    content_type TEXT NOT NULL,
    body BYTEA NOT NULL,
    PRIMARY KEY (project, item)
);
