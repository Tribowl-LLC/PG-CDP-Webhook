"""Rescores every customer with the current model (main.rescore_customer): persona and bowler_value_score are
recomputed from current facts, so scores can go down. No Meta events are sent and no audiences are uploaded.

Run it where DATABASE_URL is available (e.g. `render ssh <service> --ephemeral`):
    python scripts/rescore_all.py            # dry run: prints the new distribution, changes nothing
    python scripts/rescore_all.py --apply    # writes the new personas and scores

Only aggregate counts are printed.
"""
import argparse
import collections
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import main  # noqa: E402

BATCH = 500
THRESHOLDS = (80, 70, 60, 55, 50, 45)


def run(apply: bool):
    conn = main.get_conn()
    personas, bands = collections.Counter(), collections.Counter()
    changed_score = changed_persona = done = 0
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT cluster_id FROM master_customers ORDER BY cluster_id;")
            ids = [r["cluster_id"] for r in cur.fetchall()]
        for i in range(0, len(ids), BATCH):
            with conn, conn.cursor() as cur:
                cur.execute("SELECT * FROM master_customers WHERE cluster_id = ANY(%s) ORDER BY cluster_id;",
                            (ids[i:i + BATCH],))
                for before in cur.fetchall():
                    after = main.rescore_customer(cur, before)
                    changed_score += before["bowler_value_score"] != after["bowler_value_score"]
                    changed_persona += before["pg_mosaic_persona"] != after["pg_mosaic_persona"]
                    personas[after["pg_mosaic_persona"]] += 1
                    s = after["bowler_value_score"]
                    for t in THRESHOLDS:
                        bands[t] += s >= t
                    done += 1
                if not apply:
                    conn.rollback()
            print(f"  {done}/{len(ids)}", flush=True)
    finally:
        conn.close()

    print(f"Rescored {done} customers: {changed_score} scores and {changed_persona} personas changed")
    for p, n in sorted(personas.items()):
        print(f"  {p}: {n}")
    for t in THRESHOLDS:
        print(f"  score >= {t}: {bands[t]}")
    print("Applied." if apply else "Dry run: nothing written. Re-run with --apply to write.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true")
    run(p.parse_args().apply)
