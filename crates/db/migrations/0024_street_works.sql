-- ZStreet's city works: every round of blocks the day's agent architects propose three buildings,
-- visitors vote, and the winner goes up on the next site, a little more with each mined block.
CREATE TABLE street_projects (
    round       INTEGER NOT NULL,
    idx         SMALLINT NOT NULL CHECK (idx BETWEEN 0 AND 2),
    architect   INTEGER NOT NULL,
    name        TEXT NOT NULL,
    pitch       TEXT NOT NULL,
    plan        JSONB NOT NULL,
    backers     JSONB NOT NULL DEFAULT '[]',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (round, idx)
);

CREATE TABLE street_votes (
    round       INTEGER NOT NULL,
    token_hash  BYTEA NOT NULL,
    idx         SMALLINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (round, token_hash)
);

-- A winning project and where it stands; a site taken again later shows its newest building.
CREATE TABLE street_sites (
    round         INTEGER PRIMARY KEY,
    idx           SMALLINT NOT NULL,
    site          SMALLINT NOT NULL,
    start_height  INTEGER NOT NULL,
    votes         INTEGER NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
