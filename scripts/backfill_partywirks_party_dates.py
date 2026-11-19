"""Puts each recorded Partywirks booking's party date on its ledger row (unified_transactions pw_<order>).

Rows recorded before this carry the date they were first seen, not the party date, so a Tripleseat re-entry of
the same party can't be matched to them (main.party_qb_twin). Dates come from the Partywirks API's reservation
list (`event_date`). An order the API doesn't list is left alone. Re-running is harmless. Nothing is rescored
(scoring reads only Square rows' dates) and no Meta events are sent.

Run where PARTYWIRKS_API_KEY and DATABASE_URL are set (e.g. `render ssh <service> --ephemeral`):
    python scripts/backfill_partywirks_party_dates.py            # dry run: counts only, writes nothing
    python scripts/backfill_partywirks_party_dates.py --apply    # writes

Only aggregate counts are printed.
"""
import argparse
import collections
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import main  # noqa: E402
from backfill_partywirks_spend import list_reservations  # noqa: E402


def run(apply: bool, reservations: list) -> collections.Counter:
    stats = collections.Counter()
    dates = {}
    for r in reservations:
        oid = str(r.get("order_id") or "").strip()
        ts = main.pw_party_timestamp(main.date_only(r.get("event_date")))
        if oid and ts:
            dates[f"pw_{oid}"] = ts
        else:
            stats["listed_without_id_or_date"] += 1
    conn = main.get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT transaction_id FROM unified_transactions WHERE source_system = 'partywirks';")
            for row in cur.fetchall():
                ts = dates.get(row["transaction_id"])
                if not ts:
                    stats["ledger_not_listed"] += 1
                    continue
                cur.execute("UPDATE unified_transactions SET transaction_date = %s::timestamptz "
                            "WHERE transaction_id = %s AND transaction_date IS DISTINCT FROM %s::timestamptz "
                            "RETURNING 1;", (ts, row["transaction_id"], ts))
                stats["ledger_dates_set" if cur.fetchone() else "ledger_already_dated"] += 1
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
