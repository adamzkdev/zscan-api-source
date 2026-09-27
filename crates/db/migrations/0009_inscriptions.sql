-- Inscriptions (Zerdinals / Zordinals) and ZRC-20 tokens, projected by the indexer's own scan
-- from the first envelope block (3,132,356). Independent of `transactions` and never pruned.

-- Next height to scan.
CREATE TABLE inscription_sync (
    id          BOOLEAN PRIMARY KEY DEFAULT true CHECK (id),
    height      INTEGER NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE inscriptions (
    -- Genesis txid + 'i0'.
    id              TEXT PRIMARY KEY,
    -- Ordered by completion height, then position in the block; from 1.
    number          BIGINT NOT NULL UNIQUE,
    genesis_height  INTEGER NOT NULL,
    -- Completion transaction, carrying piece 0.
    txid            TEXT NOT NULL,
    height          INTEGER NOT NULL,
    block_index     INTEGER NOT NULL,
    time            TIMESTAMPTZ NOT NULL,
    content_type    TEXT NOT NULL,
    -- image, svg, html, text, json, audio, video, other.
    media           TEXT NOT NULL,
    content_len     INTEGER NOT NULL,
    -- NULL above INSCRIPTION_CONTENT_MAX: the API reassembles it from the reveal transactions.
    content         BYTEA,
    sha256          TEXT NOT NULL,
    reveal_txids    TEXT[] NOT NULL,
    -- ZRC-20 op named by the body, valid or not.
    zrc20           TEXT,
    owner           TEXT,
    location_txid   TEXT,
    location_vout   INTEGER,
    -- transparent, shielded (spent into a shielded transaction), burned. The last two are final.
    state           TEXT NOT NULL,
    transfers       INTEGER NOT NULL DEFAULT 0,
    last_height     INTEGER NOT NULL,
    -- Moderation: hidden content is never served.
    hidden          BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX inscriptions_owner_idx ON inscriptions (owner, number DESC) WHERE owner IS NOT NULL;
CREATE INDEX inscriptions_media_idx ON inscriptions (media, number DESC);
CREATE INDEX inscriptions_txid_idx ON inscriptions (txid);

-- Inscribed, moved, shielded or burned.
CREATE TABLE inscription_events (
    inscription_id  TEXT NOT NULL REFERENCES inscriptions (id) ON DELETE CASCADE,
    kind            TEXT NOT NULL,
    txid            TEXT NOT NULL,
    height          INTEGER NOT NULL,
    time            TIMESTAMPTZ NOT NULL,
    from_addr       TEXT,
    to_addr         TEXT,
    PRIMARY KEY (inscription_id, txid)
);
CREATE INDEX inscription_events_txid_idx ON inscription_events (txid);
CREATE INDEX inscription_events_height_idx ON inscription_events (height DESC);

-- Reveal chains waiting for their next transaction, keyed by the output it must spend.
CREATE TABLE inscription_pending (
    txid    TEXT NOT NULL,
    vout    INTEGER NOT NULL,
    data    JSONB NOT NULL,
    PRIMARY KEY (txid, vout)
);

-- Amounts are integers scaled by the token's decimals.
CREATE TABLE zrc20_tokens (
    tick                TEXT PRIMARY KEY,
    max                 NUMERIC(40, 0) NOT NULL,
    lim                 NUMERIC(40, 0) NOT NULL,
    dec                 SMALLINT NOT NULL,
    minted              NUMERIC(40, 0) NOT NULL,
    burned              NUMERIC(40, 0) NOT NULL,
    shielded            NUMERIC(40, 0) NOT NULL,
    mints               BIGINT NOT NULL,
    holders             INTEGER NOT NULL DEFAULT 0,
    deployer            TEXT NOT NULL,
    deploy_inscription  TEXT NOT NULL,
    deploy_txid         TEXT NOT NULL,
    height              INTEGER NOT NULL,
    time                TIMESTAMPTZ NOT NULL
);

CREATE TABLE zrc20_balances (
    tick            TEXT NOT NULL,
    address         TEXT NOT NULL,
    available       NUMERIC(40, 0) NOT NULL,
    transferable    NUMERIC(40, 0) NOT NULL,
    PRIMARY KEY (tick, address)
);
CREATE INDEX zrc20_balances_address_idx ON zrc20_balances (address);
CREATE INDEX zrc20_balances_total_idx ON zrc20_balances (tick, (available + transferable) DESC);

-- Transfer inscriptions whose output hasn't been spent: the amount is committed.
CREATE TABLE zrc20_committed (
    inscription_id  TEXT PRIMARY KEY,
    tick            TEXT NOT NULL,
    amount          NUMERIC(40, 0) NOT NULL,
    sender          TEXT NOT NULL
);

-- Every operation, including the ones that did not count (valid = false, with the reason).
-- op: deploy, mint, transfer (inscribed), send / shielded / burn (settled), invalid.
CREATE TABLE zrc20_events (
    id              BIGSERIAL PRIMARY KEY,
    tick            TEXT NOT NULL,
    op              TEXT NOT NULL,
    inscription_id  TEXT NOT NULL,
    txid            TEXT NOT NULL,
    height          INTEGER NOT NULL,
    time            TIMESTAMPTZ NOT NULL,
    from_addr       TEXT,
    to_addr         TEXT,
    amount          NUMERIC(40, 0),
    valid           BOOLEAN NOT NULL,
    reason          TEXT
);
CREATE INDEX zrc20_events_tick_idx ON zrc20_events (tick, id DESC);
CREATE INDEX zrc20_events_txid_idx ON zrc20_events (txid);
