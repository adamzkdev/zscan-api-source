-- Mining pool identification from coinbase data.
ALTER TABLE blocks ADD COLUMN miner_tag TEXT;
ALTER TABLE blocks ADD COLUMN miner_pool TEXT;
CREATE INDEX blocks_miner_idx ON blocks (miner);
CREATE INDEX blocks_miner_pool_idx ON blocks (miner_pool);
