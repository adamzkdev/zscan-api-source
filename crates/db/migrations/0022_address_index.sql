-- Per-address index: one row per (address, transaction) with what the address received and
-- sent in it, and running totals per address. Address pages, CSV exports and the accounts
-- ranking read these instead of re-aggregating tx_outputs/tx_inputs, so addresses with
-- millions of transactions stay fast once the whole chain is indexed.

CREATE TABLE address_txs (
    address      TEXT NOT NULL,
    txid         TEXT NOT NULL REFERENCES transactions (txid) ON DELETE CASCADE,
    height       INTEGER NOT NULL,
    block_index  INTEGER NOT NULL,
    time         TIMESTAMPTZ NOT NULL,
    kind         TEXT NOT NULL,
    received     BIGINT NOT NULL,
    sent         BIGINT NOT NULL,
    PRIMARY KEY (address, height, block_index)
);
-- For the cascade when a transaction is deleted (reorgs, backfill restarts, retention).
CREATE INDEX address_txs_txid_idx ON address_txs (txid);

CREATE TABLE address_stats (
    address     TEXT PRIMARY KEY,
    received    BIGINT NOT NULL,
    sent        BIGINT NOT NULL,
    tx_count    BIGINT NOT NULL,
    first_seen  TIMESTAMPTZ,
    last_seen   TIMESTAMPTZ
);
CREATE INDEX address_stats_received_idx ON address_stats (received DESC);

INSERT INTO address_txs (address, txid, height, block_index, time, kind, received, sent)
SELECT x.address, x.txid, t.height, t.block_index, t.time, t.kind, sum(x.r), sum(x.s)
FROM (
    SELECT address, txid, value AS r, 0::bigint AS s FROM tx_outputs WHERE address IS NOT NULL
    UNION ALL
    SELECT address, txid, 0, COALESCE(value, 0) FROM tx_inputs WHERE address IS NOT NULL
) x
JOIN transactions t USING (txid)
GROUP BY x.address, x.txid, t.height, t.block_index, t.time, t.kind;

INSERT INTO address_stats
SELECT address, sum(received), sum(sent), count(*), min(time), max(time)
FROM address_txs GROUP BY address;

-- Totals follow address_txs. Rows are locked in address order so concurrent writers (tip
-- follower and backfill) don't deadlock on busy addresses.
CREATE FUNCTION address_stats_add() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO address_stats AS s (address, received, sent, tx_count, first_seen, last_seen)
    SELECT address, sum(received), sum(sent), count(*), min(time), max(time)
    FROM new_rows GROUP BY address ORDER BY address
    ON CONFLICT (address) DO UPDATE SET
        received = s.received + EXCLUDED.received,
        sent = s.sent + EXCLUDED.sent,
        tx_count = s.tx_count + EXCLUDED.tx_count,
        first_seen = LEAST(s.first_seen, EXCLUDED.first_seen),
        last_seen = GREATEST(s.last_seen, EXCLUDED.last_seen);
    RETURN NULL;
END $$;

CREATE FUNCTION address_stats_remove() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE address_stats s SET
        received = s.received - d.received,
        sent = s.sent - d.sent,
        tx_count = s.tx_count - d.n,
        first_seen = (SELECT a.time FROM address_txs a WHERE a.address = s.address
                      ORDER BY a.height, a.block_index LIMIT 1),
        last_seen = (SELECT a.time FROM address_txs a WHERE a.address = s.address
                     ORDER BY a.height DESC, a.block_index DESC LIMIT 1)
    FROM (SELECT address, sum(received) AS received, sum(sent) AS sent, count(*) AS n
          FROM old_rows GROUP BY address ORDER BY address) d
    WHERE s.address = d.address;
    DELETE FROM address_stats s USING (SELECT DISTINCT address FROM old_rows) d
    WHERE s.address = d.address AND s.tx_count <= 0;
    RETURN NULL;
END $$;

CREATE TRIGGER address_txs_added AFTER INSERT ON address_txs
    REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION address_stats_add();
CREATE TRIGGER address_txs_removed AFTER DELETE ON address_txs
    REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION address_stats_remove();

-- Miner pages count blocks and pick the pool per miner address: an index-only scan with both.
CREATE INDEX blocks_miner_pool_cover_idx ON blocks (miner, miner_pool);
DROP INDEX blocks_miner_idx;

-- Pool name (most common tag) and block count per miner address, refreshed by the indexer, so
-- block lists and miner pages don't re-aggregate every block of a miner on each request.
CREATE TABLE miner_pools (
    miner   TEXT PRIMARY KEY,
    pool    TEXT,
    blocks  BIGINT NOT NULL
);
INSERT INTO miner_pools
SELECT miner, mode() WITHIN GROUP (ORDER BY miner_pool) FILTER (WHERE miner_pool IS NOT NULL), count(*)
FROM blocks WHERE miner IS NOT NULL GROUP BY miner;
