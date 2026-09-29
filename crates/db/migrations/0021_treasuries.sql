-- Treasuries made public with a viewing key (labels/treasuries.json), scanned by the indexer.
-- Amounts in zatoshis.

-- Scan progress per treasury. A changed viewing key restarts the scan.
CREATE TABLE treasury_sync (
    slug TEXT PRIMARY KEY,
    ufvk TEXT NOT NULL,
    next_height INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Notes the account received: payments (scope external) and its own change (internal).
CREATE TABLE treasury_notes (
    slug TEXT NOT NULL,
    txid TEXT NOT NULL,
    pool TEXT NOT NULL,
    idx INTEGER NOT NULL,
    height INTEGER NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    value BIGINT NOT NULL,
    scope TEXT NOT NULL,
    nf TEXT NOT NULL,
    memo TEXT,
    spent_txid TEXT,
    spent_height INTEGER,
    spent_time TIMESTAMPTZ,
    PRIMARY KEY (slug, txid, pool, idx)
);
CREATE INDEX treasury_notes_unspent_idx ON treasury_notes (slug) WHERE spent_txid IS NULL;

-- Payments the account sent to others (recovered with its outgoing viewing key).
CREATE TABLE treasury_sends (
    slug TEXT NOT NULL,
    txid TEXT NOT NULL,
    pool TEXT NOT NULL,
    idx INTEGER NOT NULL,
    height INTEGER NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    value BIGINT NOT NULL,
    recipient TEXT,
    memo TEXT,
    PRIMARY KEY (slug, txid, pool, idx)
);
