"""One-time Partywirks backfill from the REST API.

Records every Partywirks reservation in the ledger and refreshes each parent's details (real package,
party date, street address for Melissa). Bookings created before --count-since were already counted
by the original import, so they're recorded without being counted again and send no Meta events.
Bookings created after it (missed before the webhook existed) are counted normally.

Run where PARTYWIRKS_API_KEY is set (e.g. `render ssh <service> --ephemeral`):
    python scripts/backfill_partywirks.py --count-since "2026-11-15 00:00:00"            # dry run
    python scripts/backfill_partywirks.py --count-since "2026-11-15 00:00:00" --apply    # writes

Before the API existed, a webhook without a usable order ID was recorded under a placeholder key
(pw_c<customer>_...) with a guessed package/date. Those rows are undone first (booking count -1,
last package/date cleared) so the real reservation data replaces them.

Paces itself under the API limit (two calls per new reservation), so a full run takes a few minutes.
Only aggregate counts are printed.
"""
import argparse
import collections
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import main  # noqa: E402


def undo_placeholder_rows(apply: bool) -> int:
    conn = main.get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT transaction_id, cluster_id FROM unified_transactions "
                        "WHERE source_system = 'partywirks' AND transaction_id LIKE 'pw\\_c%%';")
            rows = cur.fetchall()
            if apply:
                for r in rows:
                    if r["cluster_id"]:
                        cur.execute("UPDATE master_customers SET pw_bookings = GREATEST(0, COALESCE(pw_bookings, 0) - 1), "
                                    "pw_last_package = NULL, pw_last_party_date = NULL WHERE cluster_id = %s;",
                                    (r["cluster_id"],))
                    cur.execute("DELETE FROM unified_transactions WHERE transaction_id = %s;", (r["transaction_id"],))
            else:
                conn.rollback()
        return len(rows)
    finally:
        conn.close()


def created_per_day(days: int = 7):
    """How many reservations were created on each recent day, to sanity-check the --count-since cutoff."""
    since = date.fromordinal(date.today().toordinal() - 2000).isoformat()
    counts, page = collections.Counter(), 1
    while True:
        body = main.partywirks_api("/reservations", {"date_from": since, "per_page": 100, "page": page})
        if not body:
            break
        for r in body.get("data") or []:
            counts[str(r.get("created_at") or "")[:10]] += 1
        if not (body.get("pagination") or {}).get("has_more"):
            break
        page += 1
    recent = sorted(counts.items())[-days:]
    print("Reservations created per day (most recent days):", ", ".join(f"{d}: {n}" for d, n in recent))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--count-since", required=True,
                   help="Partywirks created_at ('YYYY-MM-DD HH:MM:SS') from which bookings are counted")
    p.add_argument("--apply", action="store_true")
    a = p.parse_args()
    if not main.PARTYWIRKS_API_KEY:
        sys.exit("PARTYWIRKS_API_KEY is not set")
    print(f"Placeholder rows from pre-API webhooks {'undone' if a.apply else 'to undo'}: {undo_placeholder_rows(a.apply)}")
    if not a.apply:
        created_per_day()
    out = main.run_partywirks_sync(days_back=2000, count_bookings_created_since=a.count_since, apply=a.apply)
    for k, v in out.items():
        print(f"  {k}: {v}")
    if not a.apply:
        print("Dry run: nothing written. Re-run with --apply to write.")
