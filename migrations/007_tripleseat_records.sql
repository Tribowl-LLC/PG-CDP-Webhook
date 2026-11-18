-- Run BEFORE the first /cron/sync-tripleseat. Safe to run more than once.
-- Tripleseat records read from its API. Dollars here are kept apart from lifetime spend: ~99% of
-- Tripleseat payments settle through Square and are already counted there.
CREATE TABLE IF NOT EXISTS tripleseat_bookings (
    booking_id         BIGINT PRIMARY KEY,
    cluster_id         BIGINT,
    status             TEXT,
    name               TEXT,
    account_name       TEXT,
    start_date         DATE,
    end_date           DATE,
    market_segment     TEXT,
    lead_sources       JSONB NOT NULL DEFAULT '[]'::jsonb,
    payments           JSONB,          -- NULL when Tripleseat sent no payment data
    paid_total         NUMERIC(12,2),  -- paid minus refunded, every method
    paid_not_card      NUMERIC(12,2),  -- the part paid by check / ACH / cash / other (not through Square)
    refunded_total     NUMERIC(12,2),
    amount_due         NUMERIC(12,2),
    ts_created_at      TIMESTAMPTZ,
    ts_updated_at      TIMESTAMPTZ,
    synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS tripleseat_bookings_cluster_idx ON tripleseat_bookings (cluster_id);

CREATE TABLE IF NOT EXISTS tripleseat_events (
    event_id           BIGINT PRIMARY KEY,
    booking_id         BIGINT,
    cluster_id         BIGINT,
    status             TEXT,
    name               TEXT,
    event_date         DATE,
    guest_count        INTEGER,
    event_type_id      BIGINT,
    grand_total        NUMERIC(12,2),
    actual_amount      NUMERIC(12,2),
    amount_due         NUMERIC(12,2),
    deposit_amount     NUMERIC(12,2),
    lost_reason        TEXT,
    ts_created_at      TIMESTAMPTZ,
    ts_updated_at      TIMESTAMPTZ,
    synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS tripleseat_events_cluster_idx ON tripleseat_events (cluster_id);
CREATE INDEX IF NOT EXISTS tripleseat_events_booking_idx ON tripleseat_events (booking_id);

CREATE TABLE IF NOT EXISTS tripleseat_leads (
    lead_id            BIGINT PRIMARY KEY,
    cluster_id         BIGINT,
    booking_id         BIGINT,
    company            TEXT,
    event_description  TEXT,
    event_date         DATE,
    guest_count        INTEGER,
    email_opt_in       BOOLEAN,
    campaign           JSONB NOT NULL DEFAULT '{}'::jsonb,   -- campaign_source/medium/name/term/content
    lead_sources       JSONB NOT NULL DEFAULT '[]'::jsonb,
    converted_at       TIMESTAMPTZ,
    turned_down_at     TIMESTAMPTZ,
    ts_created_at      TIMESTAMPTZ,
    ts_updated_at      TIMESTAMPTZ,
    synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS tripleseat_leads_cluster_idx ON tripleseat_leads (cluster_id);
