-- Birthday party bookings made in the venue's own booking tool (parties.myperfectgame.com), received on
-- POST /webhooks/website-party once Square has confirmed the deposit. Apply BEFORE deploying the code that
-- writes it. Safe to run more than once.
--
-- One row per booking, keyed by the tool's booking ID (the webhook's dedupe key). The parent's name, email and
-- phone live on the customer (master_customers), not here. Children's names, ages and form answers are never
-- sent by the tool and never stored. The ledger row for the booking is unified_transactions 'web_<booking_id>'
-- (source_system 'website'), which carries the party date for the one-Qualified_Bowler-per-party rule.
CREATE TABLE IF NOT EXISTS website_bookings (
    booking_id        TEXT PRIMARY KEY,
    cluster_id        BIGINT,
    square_payment_id TEXT,
    order_total       NUMERIC NOT NULL,        -- the full party price at the moment of booking; never updated
    currency          TEXT NOT NULL DEFAULT 'USD',
    party_date        DATE,
    party_time        TEXT,                    -- venue local, 24-hour HH:MM
    package           TEXT,                    -- Bowling / Extravaganza / Teen
    guest_count       INTEGER,
    fbp               TEXT,                    -- the browser's _fbp at booking (also kept on the customer)
    fbc               TEXT,                    -- the browser's _fbc at booking, when there was an ad click
    user_agent        TEXT,
    source            TEXT NOT NULL DEFAULT 'website',
    received_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS website_bookings_cluster_idx ON website_bookings (cluster_id);
CREATE INDEX IF NOT EXISTS website_bookings_square_payment_idx ON website_bookings (square_payment_id);
