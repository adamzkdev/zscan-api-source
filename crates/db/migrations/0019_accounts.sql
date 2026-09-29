-- ZScan accounts (console.zscan.cash): email sign-in by magic link, API keys, a watchlist of
-- transparent addresses and the alerts sent for it. Secrets (login links, sessions, API keys)
-- are stored as SHA-256 hashes only.
CREATE TABLE users (
    id                BIGSERIAL PRIMARY KEY,
    -- Lower-cased.
    email             TEXT NOT NULL UNIQUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_login_at     TIMESTAMPTZ,
    email_alerts      BOOLEAN NOT NULL DEFAULT true,
    telegram_chat_id  BIGINT,
    telegram_username TEXT
);

-- One-time sign-in links, valid 15 minutes.
CREATE TABLE login_tokens (
    token_hash  TEXT PRIMARY KEY,
    email       TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL,
    used_at     TIMESTAMPTZ
);
CREATE INDEX login_tokens_email_idx ON login_tokens (email, created_at);

CREATE TABLE sessions (
    token_hash  TEXT PRIMARY KEY,
    user_id     BIGINT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX sessions_user_idx ON sessions (user_id);

-- Replaces the placeholder from 0001 (never used).
DROP TABLE IF EXISTS api_keys;
CREATE TABLE api_keys (
    id            BIGSERIAL PRIMARY KEY,
    user_id       BIGINT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    name          TEXT NOT NULL,
    -- The key's first characters, shown to tell keys apart.
    prefix        TEXT NOT NULL,
    key_hash      TEXT NOT NULL UNIQUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at  TIMESTAMPTZ,
    requests      BIGINT NOT NULL DEFAULT 0,
    revoked_at    TIMESTAMPTZ
);
CREATE INDEX api_keys_user_idx ON api_keys (user_id);

-- Watched transparent addresses (TEX addresses are stored as the P2PKH address they pay).
CREATE TABLE watchlist (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    address     TEXT NOT NULL,
    label       TEXT,
    notify_in   BOOLEAN NOT NULL DEFAULT true,
    notify_out  BOOLEAN NOT NULL DEFAULT true,
    -- Only movements of at least this many zatoshis alert.
    min_zat     BIGINT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, address)
);
CREATE INDEX watchlist_address_idx ON watchlist (address);

-- Codes behind the "Connect Telegram" link (t.me/<bot>?start=<code>), valid 30 minutes.
CREATE TABLE telegram_links (
    code        TEXT PRIMARY KEY,
    user_id     BIGINT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    expires_at  TIMESTAMPTZ NOT NULL
);

-- One alert per watched address and transaction.
CREATE TABLE alerts (
    id                BIGSERIAL PRIMARY KEY,
    watch_id          BIGINT NOT NULL REFERENCES watchlist (id) ON DELETE CASCADE,
    user_id           BIGINT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    address           TEXT NOT NULL,
    txid              TEXT NOT NULL,
    height            INTEGER NOT NULL,
    -- 'in' or 'out' for the watched address; amount is the net change, zatoshis.
    direction         TEXT NOT NULL,
    amount            BIGINT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    email_sent_at     TIMESTAMPTZ,
    telegram_sent_at  TIMESTAMPTZ,
    error             TEXT,
    UNIQUE (watch_id, txid)
);
CREATE INDEX alerts_user_idx ON alerts (user_id, created_at DESC);

-- Where each background job stopped (the alert scanner's last block, the Telegram update id).
CREATE TABLE job_cursors (
    job    TEXT PRIMARY KEY,
    value  BIGINT NOT NULL
);
