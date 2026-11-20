"""Audits every event dropped after enrichment (DROPPED_AFTER_ENRICHMENT) against the fix #9 rule, and with
--apply re-releases the ones that were dropped before enrichment had finished.

A drop was correct when the customer's enrichment had finished AT THE TIME OF THE DROP (Versium's answer is dated
on or before the drop day, or came from the CSV import, or Versium doesn't apply to them) or the customer is in
an excluded area. Current completeness alone proves nothing: Versium may have answered after a premature drop.
Re-released (back to DEFERRED_ENRICHMENT): a drop made before enrichment finished, and any enrichment_exhausted
drop. A customer still not enriched is queued for the deferred worker; an already-enriched one is picked up by the
worker's release sweep within a minute. Either way the event is then sent or dropped under the current rules.
_versium_checked is a date, so an answer later on the drop day counts as before it (the worker enriches and
releases in one transaction, so same-day answers normally are). Only events Meta still accepts (under 6 days old) are
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


def complete_at_drop(r: dict) -> bool:
    """Whether enrichment had finished when the event was dropped (see the module docstring)."""
    flags = r.get("versium_lifestyle_flags") or {}
    if flags.get("_source"):
        return True  # the CSV import, before any of these events existed
    checked = flags.get("_versium_checked")
    if checked:
        return str(checked)[:10] <= r["dropped_at"].date().isoformat()
    # Never answered: complete only if Versium doesn't apply to this customer at all.
    return not main.VERSIUM_REQUIRED or not main.versium_queryable(r)


def run(apply: bool) -> collections.Counter:
    stats = collections.Counter()
    conn = main.get_conn()
    try:
        with conn, conn.cursor() as cur:
            thresh = main.get_threshold(cur)
            cur.execute("""SELECT l.event_id, l.event_name, l.meta_response_body AS reason, l.sent_at AS dropped_at,
                                  l.bowler_value_score
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
                excluded = main.is_detroit_excluded(r.get("md_city"), r.get("zip5"))
                if r["reason"] == "excluded_area" or excluded:
                    verdict = "correct: excluded area"
                elif r["reason"] != "enrichment_exhausted" and complete_at_drop(r):
                    verdict = "correct: enrichment had finished at the drop, score below threshold then"
                elif not r["sendable"]:
                    verdict = "too_old: enrichment had not finished, but Meta no longer accepts it"
                else:
                    verdict = "re-release: " + ("enrichment_exhausted drop" if r["reason"] == "enrichment_exhausted"
                                                else "dropped before enrichment finished")
                    if not main.enrichment_complete(r):
                        requeue.add(r["cluster_id"])   # enrich first; complete ones go to the release sweep
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
