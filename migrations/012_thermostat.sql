-- The Qualified_Bowler score thermostat, under version control (it used to exist only in Neon).
-- Safe to run more than once, and it never changes a live value: the table and its row are created only
-- if missing. The seed is the value in use when this was written (2026-11-19: target 50, threshold 55,
-- min 45, max 90), so a fresh database starts where production is, never back at 80.
--
-- /cron/adjust-thermostat (nightly) calls adjust_meta_capi_thermostat(). The function below is copied
-- exactly from production (pg_get_functiondef, 2026-11-19): it counts Qualified_Bowler events Meta accepted
-- in the last 7 days; under 40 the threshold steps down 5 (not below 45), over 65 it steps up 5 (not above
-- 90), otherwise it holds. The 40/65/45/90 numbers are written into the function; it doesn't read
-- target_weekly_conversions, min_threshold or max_threshold.

CREATE TABLE IF NOT EXISTS cdp_thermostat_config (
    id                        INTEGER PRIMARY KEY DEFAULT 1,
    target_weekly_conversions INTEGER DEFAULT 50,
    current_score_threshold   INTEGER DEFAULT 70,
    min_threshold             INTEGER DEFAULT 45,
    max_threshold             INTEGER DEFAULT 90,
    last_adjusted_at          TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO cdp_thermostat_config (id, target_weekly_conversions, current_score_threshold, min_threshold,
                                   max_threshold)
VALUES (1, 50, 55, 45, 90)
ON CONFLICT (id) DO NOTHING;

CREATE OR REPLACE FUNCTION public.adjust_meta_capi_thermostat()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
    weekly_count INT;
    curr_thresh INT;
    new_thresh INT;
BEGIN
    SELECT COUNT(*) INTO weekly_count
    FROM capi_events_log
    WHERE sent_to_meta = TRUE
      AND event_name = 'Qualified_Bowler'
      AND sent_at >= NOW() - INTERVAL '7 days';

    SELECT current_score_threshold INTO curr_thresh
    FROM cdp_thermostat_config WHERE id = 1;

    IF weekly_count < 40 THEN
        new_thresh := GREATEST(45, curr_thresh - 5);
    ELSIF weekly_count > 65 THEN
        new_thresh := LEAST(90, curr_thresh + 5);
    ELSE
        new_thresh := curr_thresh;
    END IF;

    UPDATE cdp_thermostat_config
    SET current_score_threshold = new_thresh,
        last_adjusted_at = NOW()
    WHERE id = 1;

    RETURN new_thresh;
END;
$function$;
