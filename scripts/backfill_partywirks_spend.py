"""Puts the real order total of every recorded Partywirks booking on its ledger row and its customer's pw_spend.

Before migration 011 order totals weren't stored, so every Partywirks ledger row is $0 and most bookers had no
spend. Totals come from the Partywirks API's reservation list (`totals.total`). An order the API gives no total
for is skipped: no total is ever invented. Each order is applied once (main.record_partywirks_spend only
changes a $0 row), so re-running is harmless. true_lifetime_spend becomes the max of the customer's Square,
WebRes and Partywirks totals, never the sum. Customers who changed are rescored. No Meta events are sent.

Run where PARTYWIRKS_API_KEY and DATABASE_URL are set (e.g. `render ssh <service> --ephemeral`), AFTER
migrations/011 and the code that reads pw_spend are live:
    python scripts/backfill_partywirks_spend.py            # dry run: counts only, writes nothing
    python scripts/backfill_partywirks_spend.py --apply    # writes

Only aggregate counts are printed.
"""
import argparse
import collections
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import main  # noqa: E402


def list_reservations(days_back: int = 4000):
    """Every reservation with a party date from `days_back` days ago on. None if the API is unavailable."""
    since = date.fromordinal(date.today().toordinal() - days_back).isoformat()
    out, page = [], 1
    while True:
        body = main.partywirks_api("/reservations", {"date_from": since, "per_page": 100, "page": page})
        if not body:
            return None
        out += body.get("data") or []
        if not (body.get("pagination") or {}).get("has_more"):
            return out
        page += 1


def run(apply: bool, reservations: list) -> collections.Counter:
    stats = collections.Counter()
    totals = {}
    for r in reservations:
        oid = str(r.get("order_id") or "").strip()
        if not oid or str(r.get("status") or "").lower() == "cancelled":
            stats["listed_cancelled_or_no_id"] += 1
            continue
        total = main._partywirks_total(r)
        if total > 0:
            totals[f"pw_{oid}"] = total
        else:
            stats["listed_without_total"] += 1
    conn = main.get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT transaction_id, cluster_id, total_amount FROM unified_transactions "
                        "WHERE source_system = 'partywirks' AND cluster_id IS NOT NULL;")
            ledger = cur.fetchall()
            touched = set()
            for row in ledger:
                total = totals.get(row["transaction_id"])
                if float(row["total_amount"] or 0) > 0:
                    stats["ledger_already_has_total"] += 1
                elif total is None:
                    stats["ledger_no_total_available"] += 1  # not listed, cancelled, or no total from the API
                elif main.record_partywirks_spend(cur, row["cluster_id"], row["transaction_id"], total):
                    stats["ledger_totals_recorded"] += 1
                    touched.add(row["cluster_id"])
            for cid in sorted(touched):
                cur.execute("SELECT * FROM master_customers WHERE cluster_id = %s;", (cid,))
                main.rescore_customer(cur, cur.fetchone())
            stats["customers_updated"] = len(touched)
            cur.execute("""SELECT COUNT(*) AS bookers,
                                  COUNT(*) FILTER (WHERE COALESCE(pw_spend, 0) > 0) AS with_pw_spend,
                                  COUNT(*) FILTER (WHERE COALESCE(true_lifetime_spend, 0) = 0) AS zero_lifetime_spend
                           FROM master_customers WHERE COALESCE(pw_bookings, 0) > 0;""")
            stats.update({f"bookers_{k}": v for k, v in cur.fetchone().items()})
            if not apply:
                conn.rollback()
    finally:
        conn.close()
    return stats


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true")
    a = p.parse_args()
    if not main.PARTYWIRKS_API_KEY:
        sys.exit("PARTYWIRKS_API_KEY is not set")
    res = list_reservations()
    if res is None:
        sys.exit("Partywirks reservation list unavailable; nothing changed")
    print(f"Reservations listed: {len(res)}")
    for k, v in sorted(run(a.apply, res).items()):
        print(f"  {k}: {v}")
    print("Applied." if a.apply else "Dry run: nothing written. Re-run with --apply to write.")
