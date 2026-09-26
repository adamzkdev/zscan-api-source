-- Blocks indexed by ZScan. Hashes are display-order hex.
CREATE TABLE blocks (
    height              INTEGER PRIMARY KEY,
    hash                TEXT NOT NULL UNIQUE,
    prev_hash           TEXT NOT NULL,
    time                TIMESTAMPTZ NOT NULL,
    tx_count            INTEGER NOT NULL,
    size                INTEGER NOT NULL,
    fees                BIGINT,
    miner               TEXT,
    coinbase_value      BIGINT NOT NULL DEFAULT 0,
    sapling_tree_size   INTEGER,
    orchard_tree_size   INTEGER,
    ironwood_tree_size  INTEGER,
    -- Net change of each value pool in this block (zatoshis).
    d_transparent       BIGINT NOT NULL DEFAULT 0,
    d_sprout            BIGINT NOT NULL DEFAULT 0,
    d_sapling           BIGINT NOT NULL DEFAULT 0,
    d_orchard           BIGINT NOT NULL DEFAULT 0,
    d_ironwood          BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX blocks_time_idx ON blocks (time);

CREATE TABLE transactions (
    txid                TEXT PRIMARY KEY,
    height              INTEGER NOT NULL REFERENCES blocks (height) ON DELETE CASCADE,
    block_index         INTEGER NOT NULL,
    time                TIMESTAMPTZ NOT NULL,
    version             INTEGER NOT NULL,
    size                INTEGER NOT NULL,
    kind                TEXT NOT NULL,
    fee                 BIGINT,
    transparent_in      BIGINT NOT NULL,
    transparent_out     BIGINT NOT NULL,
    sprout_vb           BIGINT NOT NULL,
    sapling_vb          BIGINT NOT NULL,
    orchard_vb          BIGINT NOT NULL,
    ironwood_vb         BIGINT NOT NULL,
    n_tin               INTEGER NOT NULL,
    n_tout              INTEGER NOT NULL,
    n_joinsplits        INTEGER NOT NULL,
    n_sapling_spends    INTEGER NOT NULL,
    n_sapling_outputs   INTEGER NOT NULL,
    n_orchard_actions   INTEGER NOT NULL,
    n_ironwood_actions  INTEGER NOT NULL
);
CREATE INDEX transactions_height_idx ON transactions (height, block_index);
CREATE INDEX transactions_kind_time_idx ON transactions (kind, time);
CREATE INDEX transactions_time_idx ON transactions (time);

CREATE TABLE tx_outputs (
    txid     TEXT NOT NULL REFERENCES transactions (txid) ON DELETE CASCADE,
    n        INTEGER NOT NULL,
    value    BIGINT NOT NULL,
    address  TEXT,
    PRIMARY KEY (txid, n)
);
CREATE INDEX tx_outputs_address_idx ON tx_outputs (address) WHERE address IS NOT NULL;

CREATE TABLE tx_inputs (
    txid       TEXT NOT NULL REFERENCES transactions (txid) ON DELETE CASCADE,
    n          INTEGER NOT NULL,
    prev_txid  TEXT NOT NULL,
    prev_n     INTEGER NOT NULL,
    value      BIGINT,
    address    TEXT,
    PRIMARY KEY (txid, n)
);
CREATE INDEX tx_inputs_prev_idx ON tx_inputs (prev_txid, prev_n);
CREATE INDEX tx_inputs_address_idx ON tx_inputs (address) WHERE address IS NOT NULL;

-- Authoritative value-pool balances from the node (getblockchaininfo).
CREATE TABLE pool_snapshots (
    height       INTEGER PRIMARY KEY,
    time         TIMESTAMPTZ NOT NULL DEFAULT now(),
    transparent  BIGINT NOT NULL,
    sprout       BIGINT NOT NULL,
    sapling      BIGINT NOT NULL,
    orchard      BIGINT NOT NULL,
    ironwood     BIGINT NOT NULL,
    lockbox      BIGINT NOT NULL,
    supply       BIGINT NOT NULL
);

CREATE TABLE network_snapshots (
    time        TIMESTAMPTZ PRIMARY KEY DEFAULT now(),
    height      INTEGER NOT NULL,
    difficulty  DOUBLE PRECISION NOT NULL,
    solps       DOUBLE PRECISION
);

CREATE TABLE mempool (
    txid        TEXT PRIMARY KEY,
    seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    size        INTEGER NOT NULL,
    kind        TEXT NOT NULL,
    fee         BIGINT,
    transparent_out BIGINT NOT NULL,
    n_actions   INTEGER NOT NULL
);

-- Indexer progress: the contiguous indexed range is [low, high].
CREATE TABLE sync_state (
    id    BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    low   INTEGER,
    high  INTEGER
);
INSERT INTO sync_state (id) VALUES (TRUE);

-- Reserved for API monetization tiers.
CREATE TABLE api_keys (
    key         TEXT PRIMARY KEY,
    owner       TEXT NOT NULL,
    tier        TEXT NOT NULL DEFAULT 'free',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
