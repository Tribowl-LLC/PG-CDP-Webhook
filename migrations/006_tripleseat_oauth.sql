-- Run BEFORE using /oauth/tripleseat/login. Safe to run more than once.
-- One row per API provider: the OAuth tokens that keep the server logged in to Tripleseat.
-- Access tokens last 2 hours; the refresh token renews them without a new login.
CREATE TABLE IF NOT EXISTS oauth_tokens (
    provider      TEXT PRIMARY KEY,
    access_token  TEXT NOT NULL,
    refresh_token TEXT,
    expires_at    TIMESTAMPTZ NOT NULL,
    scope         TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
