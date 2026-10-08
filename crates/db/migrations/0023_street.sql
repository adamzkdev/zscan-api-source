-- ZStreet: the daily newspaper, what characters remember of each visitor, and the voxel
-- buildings players put up on the city's free plots.
CREATE TABLE street_papers (
    day         DATE PRIMARY KEY,
    data        JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A visitor is only a client-side secret: its SHA-256 identifies them, the secret never leaves
-- their browser's storage.
CREATE TABLE street_memories (
    snark_id    INTEGER NOT NULL,
    token_hash  BYTEA NOT NULL,
    summary     TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (snark_id, token_hash)
);

CREATE TABLE street_plots (
    plot        INTEGER PRIMARY KEY CHECK (plot BETWEEN 0 AND 23),
    token_hash  BYTEA NOT NULL,
    name        TEXT NOT NULL,
    model       JSONB NOT NULL,
    reports     INTEGER NOT NULL DEFAULT 0,
    hidden      BOOLEAN NOT NULL DEFAULT false,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
