-- Run BEFORE deploying the matching main.py. Safe to run more than once.
-- Each Square payment row in the ledger carries what was bought (price keys) and the Square customer ID.
ALTER TABLE unified_transactions ADD COLUMN IF NOT EXISTS line_items JSONB;
ALTER TABLE unified_transactions ADD COLUMN IF NOT EXISTS square_customer_id TEXT;
CREATE INDEX IF NOT EXISTS idx_ut_square_customer ON unified_transactions (square_customer_id);
CREATE INDEX IF NOT EXISTS idx_ut_variation_ids ON unified_transactions USING GIN (variation_ids);
