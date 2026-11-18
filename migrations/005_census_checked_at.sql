-- Run BEFORE deploying the matching main.py. Safe to run more than once.
-- When the free US Census Geocoder last answered for this customer's address (match or no match),
-- so /cron/census-geocode doesn't ask again.
ALTER TABLE master_customers ADD COLUMN IF NOT EXISTS census_checked_at TIMESTAMPTZ;
