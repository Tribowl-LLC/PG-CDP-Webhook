-- The party date of a Partywirks booking, on its ledger row (unified_transactions pw_<order>). NULL = not known.
-- transaction_date on these rows is the day they were first recorded, so it can't be used as the party date.
-- Used to match a Tripleseat re-entry of the same party (one Qualified_Bowler per party; main.party_qb_twin).
-- Apply BEFORE deploying the code that reads it. Safe to run more than once.
ALTER TABLE unified_transactions ADD COLUMN IF NOT EXISTS party_date DATE;
