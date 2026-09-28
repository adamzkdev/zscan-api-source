-- Holder counts and concentration of ZEC assets on other chains (GeckoTerminal token info).
ALTER TABLE zec_assets_latest
    ADD COLUMN holders       BIGINT,
    -- Share of the supply held by the 10 largest holders, 0–1.
    ADD COLUMN holders_top10 DOUBLE PRECISION;
