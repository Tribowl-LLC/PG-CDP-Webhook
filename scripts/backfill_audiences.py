"""Pushes every qualifying customer into the Meta Custom Audiences they belong in.

Qualifying = the same rule the webhooks use: bowler_value_score at or above the current thermostat
threshold, not in an excluded area, and with an email or phone Meta can match on. Re-uploading a
customer who is already in an audience is harmless (Meta de-duplicates).

Run it where META_ACCESS_TOKEN is available (e.g. `render ssh <service> --ephemeral`):
    python scripts/backfill_audiences.py            # dry run: counts per audience, uploads nothing
    python scripts/backfill_audiences.py --apply    # uploads

Only aggregate counts are printed.
"""
import argparse
import collections
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import main  # noqa: E402

BATCH = 5000  # Meta allows up to 10,000 rows per request


def run(apply: bool):
    conn = main.get_conn()
    try:
        with conn, conn.cursor() as cur:
            thresh = main.get_threshold(cur)
            cur.execute(
                """
                SELECT * FROM master_customers
                WHERE bowler_value_score >= %s
                  AND (COALESCE(email, '') <> '' OR COALESCE(phone, '') <> '');
                """,
                (thresh,),
            )
            customers = cur.fetchall()
    finally:
        conn.close()

    rows_by_audience = collections.defaultdict(list)
    excluded = 0
    for c in customers:
        targets, ltv = main.audience_targets(c)
        if not targets:
            excluded += 1
            continue
        row = main.audience_row(c, ltv)
        for aud in targets:
            rows_by_audience[aud].append(row)

    names = main.audience_names()
    print(f"Threshold {thresh}: {len(customers)} customers at or above it with an email/phone "
          f"({excluded} skipped as excluded area)")
    for aud, rows in rows_by_audience.items():
        print(f"  {names.get(aud, aud)}: {len(rows)} customers")

    if not apply:
        print("Dry run: nothing uploaded. Re-run with --apply to upload.")
        return
    if not main.META_ACCESS_TOKEN:
        sys.exit("META_ACCESS_TOKEN is not set")

    failures = 0
    for aud, rows in rows_by_audience.items():
        received = invalid = 0
        for i in range(0, len(rows), BATCH):
            res = main.upload_audience_rows(aud, rows[i:i + BATCH])
            if not res["ok"]:
                failures += 1
                print(f"  {names.get(aud, aud)}: batch {i // BATCH + 1} REJECTED: {res.get('error')}")
                continue
            received += res.get("received") or 0
            invalid += res.get("invalid") or 0
        print(f"  {names.get(aud, aud)}: Meta received {received}, invalid {invalid}")
    print("Done." if not failures else f"Done with {failures} rejected batch(es).")
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true")
    run(p.parse_args().apply)
