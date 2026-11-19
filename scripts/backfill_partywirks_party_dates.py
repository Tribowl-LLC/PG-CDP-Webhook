"""Puts each recorded Partywirks booking's party date on its ledger row (unified_transactions pw_<order>).

Rows recorded before migrations/013 have no party_date (their transaction_date is the day they were first
recorded), so a Tripleseat re-entry of the same party can't be matched to them (main.party_qb_twin). Dates
come from the Partywirks API's reservation list (`event_date`). An order the API doesn't list is left alone. Re-running is harmless. Nothing is rescored
(scoring reads only Square rows' dates) and no Meta events are sent.

Run where PARTYWIRKS_API_KEY and DATABASE_URL are set (e.g. `render ssh <service> --ephemeral`), after
migrations/013 is applied:
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
        d = main.pw_party_date(main.date_only(r.get("event_date")))
        if oid and d:
            dates[f"pw_{oid}"] = d
        else:
            stats["listed_without_id_or_date"] += 1
    conn = main.get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT transaction_id FROM unified_transactions WHERE source_system = 'partywirks';")
            for row in cur.fetchall():
                d = dates.get(row["transaction_id"])
                if not d:
                    stats["ledger_not_listed"] += 1  # its party_date stays unknown (NULL), which never matches
                    continue
                changed = main.set_pw_party_date(cur, row["transaction_id"], d)
                stats["ledger_dates_set" if changed else "ledger_already_dated"] += 1
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
