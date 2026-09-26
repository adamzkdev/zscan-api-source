-- Protocol economics per block (zatoshis).
ALTER TABLE blocks ADD COLUMN subsidy BIGINT;          -- new coins created
ALTER TABLE blocks ADD COLUMN funding_paid BIGINT;     -- coinbase outputs to funding-stream addresses (ZCG)
ALTER TABLE blocks ADD COLUMN deferred BIGINT;         -- amount deferred to the lockbox / coinholder fund
ALTER TABLE blocks ADD COLUMN burned BIGINT;           -- fees + subsidy - deferred - coinbase outputs (NU7 fee removal)
CREATE INDEX blocks_subsidy_null_idx ON blocks (height) WHERE subsidy IS NULL;

-- Chain reorganizations seen by the indexer.
CREATE TABLE reorgs (
    id           BIGSERIAL PRIMARY KEY,
    detected_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    height       INTEGER NOT NULL,
    old_hash     TEXT NOT NULL,
    new_hash     TEXT NOT NULL
);

-- Independent supply audit: the change in the node's pool balances between two snapshots
-- must equal the sum of pool deltas we decoded from every block in between.
CREATE TABLE audit_checks (
    height        INTEGER PRIMARY KEY,
    prev_height   INTEGER NOT NULL,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    diff_transparent BIGINT NOT NULL,
    diff_sprout      BIGINT NOT NULL,
    diff_sapling     BIGINT NOT NULL,
    diff_orchard     BIGINT NOT NULL,
    diff_ironwood    BIGINT NOT NULL,
    diff_lockbox     BIGINT NOT NULL,
    diff_supply      BIGINT NOT NULL,
    ok            BOOLEAN NOT NULL
);
