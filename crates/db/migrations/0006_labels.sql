-- Address name tags. Every label records where it comes from, so it can be verified.
CREATE TABLE labels (
    address     TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    category    TEXT NOT NULL,
    -- Slug of the protocol / entity this address belongs to (Protocols page).
    protocol    TEXT,
    source      TEXT NOT NULL,
    -- curated (labels/addresses.json) | consensus | coinbase | api
    origin      TEXT NOT NULL,
    -- False once the protocol stops using the address (e.g. a retired bridge vault).
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    balance     BIGINT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX labels_protocol_idx ON labels (protocol);

-- Latest figures published by a protocol's own API (pool depth, volume, status…).
CREATE TABLE protocol_stats (
    slug        TEXT PRIMARY KEY,
    data        JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
