-- Run BEFORE deploying the matching main.py. Safe to run more than once.
-- Manual Melissa.com uploads: customers Versium found nothing for (with a name and an email or phone, and no
-- Melissa demographics) are queued; every MELISSA_BATCH_SIZE (100) of them become a batch that the Gmail
-- Apps Script emails as a CSV (POST /cron/melissa-batch).
CREATE TABLE IF NOT EXISTS melissa_batches (
    batch_id             SERIAL PRIMARY KEY,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    customer_count       INTEGER NOT NULL,
    emailed_at           TIMESTAMPTZ,
    results_imported_at  TIMESTAMPTZ
);
-- NULL = not uploaded yet; 'prior' = already through Melissa before this queue existed; 'sent' = in a batch
-- (melissa_batch_id). The results import will add 'matched' / 'no_match'.
ALTER TABLE master_customers ADD COLUMN IF NOT EXISTS melissa_upload_status TEXT;
ALTER TABLE master_customers ADD COLUMN IF NOT EXISTS melissa_batch_id INTEGER REFERENCES melissa_batches (batch_id);
CREATE INDEX IF NOT EXISTS master_customers_melissa_batch_idx ON master_customers (melissa_batch_id)
    WHERE melissa_batch_id IS NOT NULL;

-- The current pool that Melissa already answered for (real result codes) is marked 'prior' so it isn't
-- uploaded again. To include them after all: UPDATE master_customers SET melissa_upload_status = NULL
-- WHERE melissa_upload_status = 'prior';
UPDATE master_customers SET melissa_upload_status = 'prior'
WHERE melissa_upload_status IS NULL
  AND md_results ~ '^[A-Z]{2}[0-9]{2}(,|$)'
  AND COALESCE(TRIM(first_name), '') <> '' AND COALESCE(TRIM(last_name), '') <> ''
  AND (COALESCE(TRIM(email), '') <> '' OR COALESCE(TRIM(phone), '') <> '')
  AND COALESCE(versium_lifestyle_flags ? '_versium_checked' OR versium_lifestyle_flags ? '_source', FALSE)
  AND NOT EXISTS (SELECT 1 FROM jsonb_object_keys(COALESCE(versium_lifestyle_flags, '{}'::jsonb)) AS k
                  WHERE left(k, 1) <> '_')  -- any Versium field counts as a match (see versium_matched)
  AND COALESCE(TRIM(md_household_income), '') = '' AND COALESCE(TRIM(md_date_of_birth), '') = ''
  AND COALESCE(TRIM(md_demographic_results), '') = '';
