-- Lowest height whose transactions are still stored. With TX_RETENTION_DAYS set, the indexer
-- deletes older transactions (inputs and outputs cascade) but keeps their blocks, so charts
-- and rollups built on `blocks` keep their history.
ALTER TABLE sync_state ADD COLUMN tx_low INTEGER;
