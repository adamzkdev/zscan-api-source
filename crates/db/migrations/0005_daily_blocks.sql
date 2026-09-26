-- Per-day rollup of `blocks`, maintained by the indexer, so daily series don't rescan the index.
CREATE TABLE daily_blocks (
    day                TIMESTAMPTZ PRIMARY KEY,
    blocks             INTEGER NOT NULL,
    -- Blocks whose protocol economics are known (subsidy IS NOT NULL); the sums below cover these.
    econ_blocks        INTEGER NOT NULL,
    issued             BIGINT NOT NULL,
    funding_paid       BIGINT NOT NULL,
    deferred           BIGINT NOT NULL,
    fees               BIGINT NOT NULL,
    burned             BIGINT NOT NULL,
    sapling_tree_size  BIGINT,
    orchard_tree_size  BIGINT,
    ironwood_tree_size BIGINT,
    d_transparent      BIGINT NOT NULL,
    d_sprout           BIGINT NOT NULL,
    d_sapling          BIGINT NOT NULL,
    d_orchard          BIGINT NOT NULL,
    d_ironwood         BIGINT NOT NULL
);
