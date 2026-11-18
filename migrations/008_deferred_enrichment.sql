-- Run BEFORE deploying the matching main.py. Safe to run more than once.
-- Deferred enrichment: webhooks save the customer and set enrich_after (first webhook + ENRICH_DELAY_MINUTES);
-- a background worker then runs Versium + Census + scoring + the held Meta events, once the WebRes /
-- Partywirks details that follow a Square payment have been merged.
ALTER TABLE master_customers ADD COLUMN IF NOT EXISTS enrich_after TIMESTAMPTZ;
ALTER TABLE master_customers ADD COLUMN IF NOT EXISTS enrich_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE master_customers ADD COLUMN IF NOT EXISTS enrich_inputs JSONB;
CREATE INDEX IF NOT EXISTS master_customers_enrich_after_idx ON master_customers (enrich_after)
    WHERE enrich_after IS NOT NULL;
CREATE INDEX IF NOT EXISTS capi_events_log_deferred_idx ON capi_events_log (cluster_id)
    WHERE NOT sent_to_meta AND meta_response_status = 'DEFERRED_ENRICHMENT';
