-- Per-customer enrichment outcome (fix #9): which providers answered or failed on the last enrichment run, after
-- which attempt, and how it ended (enriched / retry_later / gave_up_versium), so a customer's state can be read
-- from the database instead of inferred from logs. Apply BEFORE deploying the code that writes it. Safe to run
-- more than once.
ALTER TABLE master_customers ADD COLUMN IF NOT EXISTS enrich_outcome JSONB;
