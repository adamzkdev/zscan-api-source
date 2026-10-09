-- ZStreet's wallet towers (ZODL, Vizor): what the wallets say about themselves, read every quarter
-- hour from X, GitHub and the App Store and kept here so a restart, or a source that blocks us for
-- a while, still leaves the towers their posters.
CREATE TABLE street_wallet_feed (
    wallet      TEXT PRIMARY KEY,
    feed        JSONB NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
