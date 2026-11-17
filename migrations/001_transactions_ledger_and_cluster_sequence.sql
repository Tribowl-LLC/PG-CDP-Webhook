-- Run this BEFORE deploying the matching main.py (Render auto-deploys main on merge).
-- Safe to run more than once. Changes no existing customer data.
BEGIN;

-- 1. unified_transactions becomes the ledger of every payment / booking / record we've processed,
--    so a webhook delivered twice is only counted once.
ALTER TABLE unified_transactions ADD COLUMN IF NOT EXISTS external_customer_id TEXT;
ALTER TABLE unified_transactions ADD COLUMN IF NOT EXISTS recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
CREATE INDEX IF NOT EXISTS idx_ut_cluster ON unified_transactions (cluster_id);
-- Square payments parked until we can tell who the Square customer is.
CREATE INDEX IF NOT EXISTS idx_ut_pending_square
    ON unified_transactions (source_system, external_customer_id) WHERE cluster_id IS NULL;

-- 1b. Seed the ledger with payments/bookings already processed (their Purchase events carry the IDs).
--     Amounts are what was counted at the time (Square amount_money, i.e. before tips).
INSERT INTO unified_transactions (transaction_id, cluster_id, source_system, transaction_date, total_amount)
SELECT event_id, cluster_id,
       CASE WHEN event_id LIKE 'sq_pay_%' THEN 'square' ELSE 'webres' END,
       sent_at, event_value
FROM capi_events_log
WHERE event_name = 'Purchase' AND (event_id LIKE 'sq_pay_%' OR event_id LIKE 'wr_%')
  AND cluster_id IN (SELECT cluster_id FROM master_customers)
ON CONFLICT (transaction_id) DO NOTHING;

-- 2. Customer numbers come from a sequence instead of MAX(cluster_id) + 1.
CREATE SEQUENCE IF NOT EXISTS master_cluster_id_seq;
SELECT setval('master_cluster_id_seq', GREATEST(
    (SELECT COALESCE(MAX(cluster_id), 24220) FROM master_customers),
    (SELECT last_value FROM master_cluster_id_seq)));
ALTER SEQUENCE master_cluster_id_seq OWNED BY master_customers.cluster_id;
ALTER TABLE master_customers ALTER COLUMN cluster_id SET DEFAULT nextval('master_cluster_id_seq');

COMMIT;
