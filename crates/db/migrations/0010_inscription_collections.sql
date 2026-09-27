-- Inscriptions without ZRC-20: token documents keep their sequence number but aren't indexed,
-- which leaves NFTs (on-chain content, ZRC-721 collection items, IPFS pointers). The changed
-- classification needs a rescan from the first envelope block.
DROP TABLE zrc20_events, zrc20_balances, zrc20_committed, zrc20_tokens;
TRUNCATE inscription_events, inscriptions, inscription_pending, inscription_sync;
DROP INDEX inscription_events_height_idx;
-- One event per inscription and transaction by construction; a plain index on the id is a
-- fraction of the (id, txid) key's size (duplicate ids deduplicate).
ALTER TABLE inscription_events DROP CONSTRAINT inscription_events_pkey;
CREATE INDEX inscription_events_inscription_idx ON inscription_events (inscription_id);

ALTER TABLE inscriptions
    DROP COLUMN zrc20,
    -- ZRC-721: collection name (uppercase) on deploys and items, and the item id.
    ADD COLUMN collection TEXT,
    ADD COLUMN item TEXT,
    -- IPFS pointer: CID (optionally with a path) and the media type it declares.
    ADD COLUMN ipfs TEXT,
    ADD COLUMN ipfs_mime TEXT;
CREATE INDEX inscriptions_collection_idx ON inscriptions (collection, number)
    WHERE collection IS NOT NULL;

-- Numbers include token documents that aren't stored, so the next one is kept here.
ALTER TABLE inscription_sync ADD COLUMN next_number BIGINT NOT NULL DEFAULT 1;

-- ZRC-721 collections, defined by the first deploy of a name.
CREATE TABLE collections (
    name                TEXT PRIMARY KEY,
    supply              BIGINT,
    -- IPFS directory with `<item>.json` metadata.
    meta                TEXT,
    deploy_inscription  TEXT NOT NULL,
    deployer            TEXT,
    height              INTEGER NOT NULL,
    time                TIMESTAMPTZ NOT NULL
);
