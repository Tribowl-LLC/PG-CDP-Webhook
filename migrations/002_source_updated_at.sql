-- Run BEFORE deploying the matching main.py. Safe to run more than once.
-- Lets the app ignore an older Square notice that arrives after a newer one (e.g. after a refund).
ALTER TABLE unified_transactions ADD COLUMN IF NOT EXISTS source_updated_at TIMESTAMPTZ;
