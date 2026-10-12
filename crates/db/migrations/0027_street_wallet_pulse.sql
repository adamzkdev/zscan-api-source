-- ZStreet's wallet towers, live: every commit, release, pull request and tweet of the towers'
-- wallets as it happens (read every minute), so a zkSNARK in the tower can tell whoever is inside.
-- Only the last hours are kept: the posters on the walls come from street_wallet_feed.
CREATE TABLE street_wallet_pulse (
    id      BIGSERIAL PRIMARY KEY,
    uid     TEXT NOT NULL UNIQUE,
    wallet  TEXT NOT NULL,
    kind    TEXT NOT NULL,
    repo    TEXT,
    actor   TEXT,
    avatar  TEXT,
    title   TEXT NOT NULL,
    url     TEXT,
    at      TIMESTAMPTZ NOT NULL,
    seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX street_wallet_pulse_wallet ON street_wallet_pulse (wallet, id);

-- the ZApp tower is ZLab's now: its feed is read again under the new key
DELETE FROM street_wallet_feed WHERE wallet = 'zapp';
