-- The durable daily rollup also carries the block-level figures (blocks, bytes, pool deltas), so
-- every daily series keeps the same history, including where `blocks` starts later (a hosted
-- instance started from a recent START_HEIGHT and seeded with older days).
ALTER TABLE daily_txs RENAME TO daily_stats;
ALTER TABLE daily_stats
    ADD COLUMN blocks        BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN block_bytes   BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN last_height   INTEGER,
    ADD COLUMN d_transparent BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN d_sprout      BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN d_sapling     BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN d_orchard     BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN d_ironwood    BIGINT NOT NULL DEFAULT 0;
