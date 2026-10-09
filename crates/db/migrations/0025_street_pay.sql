-- ZStreet's wallet layer: a visitor asks for an intent (an action and where it happens), pays
-- ZStreet's shielded address from any wallet with the intent's reference in the memo, and the
-- indexer, scanning that address with its viewing key, records the payment; the API then plays
-- the action for everyone: a message on the ground, a guestbook line, a sealed vote, a tip.
CREATE TABLE street_intents (
    ref          TEXT PRIMARY KEY,
    action       TEXT NOT NULL CHECK (action IN ('mark', 'guestbook', 'vote', 'tip')),
    payload      JSONB NOT NULL DEFAULT '{}',
    token_hash   BYTEA NOT NULL,
    min_zat      BIGINT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX street_intents_token ON street_intents (token_hash, created_at DESC);

-- Every note received by ZStreet's address, with its memo. `ref` is set when the memo carries
-- a known intent; `applied` once the API has played it (or found it short).
CREATE TABLE street_payments (
    txid        TEXT NOT NULL,
    pool        TEXT NOT NULL,
    idx         INTEGER NOT NULL,
    height      INTEGER NOT NULL,
    time        TIMESTAMPTZ NOT NULL,
    value       BIGINT NOT NULL,
    memo        TEXT,
    ref         TEXT,
    applied     BOOLEAN NOT NULL DEFAULT false,
    outcome     TEXT,
    PRIMARY KEY (txid, pool, idx)
);
CREATE INDEX street_payments_pending ON street_payments (applied) WHERE NOT applied;
CREATE INDEX street_payments_ref ON street_payments (ref);

CREATE TABLE street_pay_sync (
    ufvk         TEXT PRIMARY KEY,
    next_height  INTEGER NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Messages sealed on-chain and left on the ground (or on the Reserve's guestbook wall).
-- `place` is 'street' or a walk-in place key; `floor` its floor.
CREATE TABLE street_marks (
    id          BIGSERIAL PRIMARY KEY,
    ref         TEXT NOT NULL UNIQUE,
    kind        TEXT NOT NULL CHECK (kind IN ('mark', 'guestbook')),
    place       TEXT NOT NULL DEFAULT 'street',
    floor       SMALLINT NOT NULL DEFAULT 0,
    x           REAL NOT NULL DEFAULT 0,
    y           REAL NOT NULL DEFAULT 0,
    z           REAL NOT NULL DEFAULT 0,
    text        TEXT NOT NULL,
    txid        TEXT NOT NULL,
    height      INTEGER NOT NULL,
    zat         BIGINT NOT NULL,
    likes       INTEGER NOT NULL DEFAULT 0,
    reports     INTEGER NOT NULL DEFAULT 0,
    hidden      BOOLEAN NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX street_marks_place ON street_marks (place, floor) WHERE NOT hidden;

CREATE TABLE street_mark_votes (
    mark_id     BIGINT NOT NULL REFERENCES street_marks (id) ON DELETE CASCADE,
    token_hash  BYTEA NOT NULL,
    kind        TEXT NOT NULL CHECK (kind IN ('like', 'report')),
    PRIMARY KEY (mark_id, token_hash, kind)
);

-- City Works votes paid on-chain, counted apart from the free ones: one per payment.
CREATE TABLE street_sealed_votes (
    ref         TEXT PRIMARY KEY,
    round       INTEGER NOT NULL,
    idx         SMALLINT NOT NULL,
    txid        TEXT NOT NULL,
    zat         BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX street_sealed_votes_round ON street_sealed_votes (round);

-- Tips: fireworks for everyone, and a sponsor name on the buses above a threshold.
CREATE TABLE street_tips (
    ref         TEXT PRIMARY KEY,
    name        TEXT,
    txid        TEXT NOT NULL,
    height      INTEGER NOT NULL,
    zat         BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
