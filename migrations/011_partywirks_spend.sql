-- Partywirks order totals as customer spend (the party money is often not linked to the booker via Square).
-- true_lifetime_spend = GREATEST(sq_spend, wr_spend, pw_spend): the same money can arrive through Square too,
-- so it's a max, never a sum. Apply BEFORE deploying the code that writes it.
ALTER TABLE master_customers ADD COLUMN IF NOT EXISTS pw_spend NUMERIC;
