-- Per-day rollup of `transactions`, maintained by the indexer. Unlike the rows themselves it is
-- never pruned (TX_RETENTION_DAYS), so daily transaction series keep their whole history.
CREATE TABLE daily_txs (
    day                 TIMESTAMPTZ PRIMARY KEY,
    -- Non-coinbase transactions, and their split by kind.
    txs                 BIGINT NOT NULL,
    transparent         BIGINT NOT NULL,
    shielding           BIGINT NOT NULL,
    deshielding         BIGINT NOT NULL,
    shielded            BIGINT NOT NULL,
    migration           BIGINT NOT NULL,
    mixed               BIGINT NOT NULL,
    fees                BIGINT NOT NULL,
    tx_bytes            BIGINT NOT NULL,
    transparent_inputs  BIGINT NOT NULL,
    transparent_outputs BIGINT NOT NULL,
    sapling_spends      BIGINT NOT NULL,
    sapling_outputs     BIGINT NOT NULL,
    orchard_actions     BIGINT NOT NULL,
    ironwood_actions    BIGINT NOT NULL,
    -- Zatoshis moved into / out of the shielded pools, summed per transaction (net of its own
    -- value balances), the only shielded volume the chain makes public.
    shielded_in         BIGINT NOT NULL,
    shielded_out        BIGINT NOT NULL
);
