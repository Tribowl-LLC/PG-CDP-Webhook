-- Meta event time and browser identifiers. Apply BEFORE deploying the code that writes them. Safe to run more
-- than once.

-- The event_time sent to Meta (the transaction time), stored at first log so a retry or a release after the
-- enrichment hold reuses it instead of being stamped at the retry time. NULL = unknown: "now" on retry, as before.
ALTER TABLE capi_events_log ADD COLUMN IF NOT EXISTS event_time TIMESTAMPTZ;

-- Backfill from the source where it can be derived. Only Square payments have their real transaction time on the
-- ledger (unified_transactions.transaction_date = the payment's created_at, set by the webhook and the history
-- import). Partywirks/Tripleseat/WebRes ledger dates are record dates, so those rows stay NULL. Already-sent rows
-- are never re-sent, so this matters only for unsent ones; rows older than Meta's 7 days are left NULL.
UPDATE capi_events_log l
SET event_time = u.transaction_date
FROM unified_transactions u
WHERE l.event_time IS NULL
  AND l.event_id LIKE 'sq\_pay\_%'
  AND u.transaction_id = l.event_id
  AND u.transaction_date IS NOT NULL
  AND u.transaction_date <= NOW()
  AND u.transaction_date > NOW() - INTERVAL '7 days';

-- fbp / fbc / user agent from the Tripleseat lead form (see site-snippets/), each with when it was captured: a
-- newer capture that lacks one of them (no ad click = no fbc) never erases or "refreshes" the stored one.
-- Sent only on that customer's Tripleseat events and only while that value's capture is younger than 90 days.
ALTER TABLE master_customers ADD COLUMN IF NOT EXISTS meta_fbp TEXT;
ALTER TABLE master_customers ADD COLUMN IF NOT EXISTS meta_fbp_at TIMESTAMPTZ;
ALTER TABLE master_customers ADD COLUMN IF NOT EXISTS meta_fbc TEXT;
ALTER TABLE master_customers ADD COLUMN IF NOT EXISTS meta_fbc_at TIMESTAMPTZ;
ALTER TABLE master_customers ADD COLUMN IF NOT EXISTS meta_client_user_agent TEXT;
ALTER TABLE master_customers ADD COLUMN IF NOT EXISTS meta_client_user_agent_at TIMESTAMPTZ;
