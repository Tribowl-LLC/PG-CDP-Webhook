-- Run BEFORE deploying the matching main.py. Safe to run more than once.
-- Versium online audience append: {"facebook"|"google"|"linkedin"|"tiktok": {field: [values]}}.
-- Facebook/TikTok emails and phones arrive SHA-256 hashed; LinkedIn adds job title, company and device IDs.
ALTER TABLE master_customers ADD COLUMN IF NOT EXISTS versium_online_audience JSONB NOT NULL DEFAULT '{}'::jsonb;
