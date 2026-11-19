-- Run BEFORE deploying the matching main.py. Safe to run more than once.
-- A webhook that fails (HTTP 500) is kept here with its full payload so it can be processed again
-- (POST /cron/retry-webhooks) instead of being lost. The same payload sent twice is stored once.
CREATE TABLE IF NOT EXISTS webhook_dead_letters (
    id               BIGSERIAL PRIMARY KEY,
    source           TEXT NOT NULL,
    payload          JSONB NOT NULL,
    payload_hash     TEXT NOT NULL,
    error            TEXT,
    attempts         INTEGER NOT NULL DEFAULT 0,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_attempt_at  TIMESTAMPTZ,
    resolved_at      TIMESTAMPTZ,
    result           JSONB,
    UNIQUE (source, payload_hash)
);
CREATE INDEX IF NOT EXISTS webhook_dead_letters_open_idx ON webhook_dead_letters (id) WHERE resolved_at IS NULL;
