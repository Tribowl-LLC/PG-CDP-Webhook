-- Run BEFORE deploying the matching main.py. Safe to run more than once.
-- A webhook that fails (HTTP 500) is kept here with its full payload so it can be processed again
-- (POST /cron/retry-webhooks) instead of being lost. The same payload sent twice is stored once.
-- record_key (e.g. "event:123") ties it to its Tripleseat record: a newer webhook for that record that
-- succeeds marks it superseded, so an old payload is never replayed over newer data.
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
    record_key       TEXT,
    UNIQUE (source, payload_hash)
);
CREATE INDEX IF NOT EXISTS webhook_dead_letters_open_idx ON webhook_dead_letters (id) WHERE resolved_at IS NULL;
ALTER TABLE webhook_dead_letters ADD COLUMN IF NOT EXISTS record_key TEXT;
CREATE INDEX IF NOT EXISTS webhook_dead_letters_open_record_idx ON webhook_dead_letters (source, record_key)
    WHERE resolved_at IS NULL;
