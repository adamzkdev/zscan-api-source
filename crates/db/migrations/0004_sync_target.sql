-- Lowest height the indexer backfills to (START_HEIGHT), for progress reporting.
ALTER TABLE sync_state ADD COLUMN target INTEGER;
