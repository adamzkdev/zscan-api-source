-- NFT sales and launchpad drops.

-- A sale is a move through an input signed SINGLE|ANYONECANPAY (how Zcash inscription
-- marketplaces settle): `price` is what the seller was paid, zatoshis.
ALTER TABLE inscription_events ADD COLUMN price BIGINT;
CREATE INDEX inscription_events_sales_idx ON inscription_events (height DESC)
    WHERE price IS NOT NULL;

-- Launchpad drop: the address paid the most by the inscription's commit transaction (the
-- creator or launchpad). NULL = not computed yet, '' = none. Unverified by nature.
ALTER TABLE inscriptions ADD COLUMN drop_key TEXT;
CREATE INDEX inscriptions_drop_idx ON inscriptions (drop_key, number)
    WHERE drop_key IS NOT NULL AND drop_key <> '';

-- Moves recorded before sale detection existed are re-checked up to `sales_until`.
ALTER TABLE inscription_sync ADD COLUMN sales_cursor INTEGER, ADD COLUMN sales_until INTEGER;
