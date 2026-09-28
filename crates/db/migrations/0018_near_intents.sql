-- NEAR Intents events now come from its explorer API (1Click swaps with both sides), replacing
-- zec.omft.near mints and burns, which only knew "NEAR" as the other side.
ALTER TABLE bridge_events
    -- Amount of counter_asset on the other side, in its own units.
    ADD COLUMN counter_amount DOUBLE PRECISION,
    -- The Zcash address ZEC was sent to (in) or refunded to (out), when reported.
    ADD COLUMN zcash_address  TEXT,
    -- transparent | unified | sapling
    ADD COLUMN address_kind   TEXT,
    -- The app or wallet that routed the swap.
    ADD COLUMN referral       TEXT;

DELETE FROM bridge_events WHERE protocol = 'near-intents';
