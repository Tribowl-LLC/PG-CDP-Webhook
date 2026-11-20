"""Audits every event dropped after enrichment (DROPPED_AFTER_ENRICHMENT) against the fix #9 rule, and with
--apply re-releases the ones that were dropped before enrichment had finished.

A drop was correct when the customer's enrichment had finished (main.enrichment_complete: Versium answered,
match or no match, or Versium doesn't apply) or the customer is in an excluded area. A drop on a customer
Versium never answered for (e.g. while VERSIUM_API_KEY was missing) is re-released: the event goes back to
DEFERRED_ENRICHMENT and the customer is queued for enrichment, so the deferred worker enriches them and then
sends or drops the event under the current rules. Only events Meta still accepts (under 6 days old) are
re-released; older ones are listed as too_old. Nothing is sent by this script itself.

Run where DATABASE_URL is set (e.g. `render ssh <service> --ephemeral`), AFTER migrations/015:
    python scripts/reevaluate_dropped_events.py            # dry run: the audit table, writes nothing
    python scripts/reevaluate_dropped_events.py --apply    # re-releases
Prints event IDs (internal keys) with scores; no contact details.
"""
import argparse
import collections
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import main  # noqa: E402


def run(apply: bool) -> collections.Counter:
    stats = collections.Counter()
    conn = main.get_conn()
    try:
        with conn, conn.cursor() as cur:
            thresh = main.get_threshold(cur)
            cur.execute("""SELECT l.event_id, l.event_name, l.meta_response_body AS reason, l.bowler_value_score
                                  AS score_at_drop, l.threshold_at_send AS thresh_at_drop,
                                  COALESCE(l.event_time, l.sent_at) > NOW() - make_interval(days => %s) AS sendable,
                                  m.*
                           FROM capi_events_log l JOIN master_customers m ON m.cluster_id = l.cluster_id
                           WHERE l.meta_response_status = 'DROPPED_AFTER_ENRICHMENT'
                           ORDER BY l.sent_at;""", (main.RETRY_MAX_AGE_DAYS,))
            rows = cur.fetchall()
            requeue = set()
            print(f"threshold now: {thresh}")
            print("event_id | event | reason | score at drop / threshold then | score now | verdict")
            for r in rows:
                complete = main.enrichment_complete(r)
                excluded = main.is_detroit_excluded(r.get("md_city"), r.get("zip5"))
                if r["reason"] == "excluded_area" or excluded:
                    verdict = "correct: excluded area"
                elif complete:
                    verdict = "correct: enrichment had finished, score below threshold then"
                elif not r["sendable"]:
                    verdict = "too_old: enrichment had not finished, but Meta no longer accepts it"
                else:
                    verdict = "re-release: dropped before enrichment finished"
                    requeue.add(r["cluster_id"])
                    if apply:
                        cur.execute("UPDATE capi_events_log SET meta_response_status = 'DEFERRED_ENRICHMENT', "
                                    "meta_response_body = NULL WHERE event_id = %s "
                                    "AND meta_response_status = 'DROPPED_AFTER_ENRICHMENT';", (r["event_id"],))
                stats[verdict.split(":")[0]] += 1
                print(f"{r['event_id']} | {r['event_name']} | {r['reason']} | {r['score_at_drop']} / "
                      f"{r['thresh_at_drop']} | {r['bowler_value_score']} | {verdict}")
            if apply and requeue:
                cur.execute("UPDATE master_customers SET enrich_after = NOW(), enrich_attempts = 0 "
                            "WHERE cluster_id = ANY(%s) AND enrich_after IS NULL;", (sorted(requeue),))
            stats["customers_requeued"] = len(requeue)
            if not apply:
                conn.rollback()
    finally:
        conn.close()
    return stats


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true")
    a = p.parse_args()
    for k, v in sorted(run(a.apply).items()):
        print(f"  {k}: {v}")
    print("Applied: the deferred worker enriches the requeued customers within minutes." if a.apply
          else "Dry run: nothing written. Re-run with --apply to re-release.")
