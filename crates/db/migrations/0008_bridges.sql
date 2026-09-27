-- ZEC crossing chains through bridges and swap protocols (Maya swaps, NEAR Intents mints and
-- burns), read from each protocol's public API. Pruned after ~90 days.
CREATE TABLE bridge_events (
    protocol      TEXT NOT NULL,
    -- The protocol's own id for the event (inbound tx id, NEAR event index), unique per protocol.
    external_id   TEXT NOT NULL,
    -- Relative to Zcash: 'in' = ZEC arrives on Zcash, 'out' = ZEC leaves Zcash.
    direction     TEXT NOT NULL,
    -- ZEC moved, zatoshis.
    amount        BIGINT NOT NULL,
    -- The other side, e.g. 'ETH.ETH' / chain 'ETH'.
    counter_asset TEXT NOT NULL,
    counter_chain TEXT NOT NULL,
    usd           DOUBLE PRECISION,
    -- The Zcash transaction, when the protocol reports it.
    zcash_txid    TEXT,
    external_url  TEXT,
    status        TEXT NOT NULL,
    time          TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (protocol, external_id)
);
CREATE INDEX bridge_events_time_idx ON bridge_events (time DESC);

-- End-of-day ZEC held by a bridge according to its own chain or API (e.g. zec.omft.near
-- total supply, Maya's ZEC pool depth). The latest value of the day wins.
CREATE TABLE bridge_snapshots (
    protocol    TEXT NOT NULL,
    day         DATE NOT NULL,
    amount      BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (protocol, day)
);
