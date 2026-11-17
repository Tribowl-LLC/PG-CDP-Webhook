"""One-time data repair. Run AFTER migration 001 and AFTER the new main.py is live.

    DATABASE_URL=... python scripts/repair_after_deploy.py            # dry run: prints totals, changes nothing
    DATABASE_URL=... python scripts/repair_after_deploy.py --apply    # makes the changes

What it does:
  1. Square spend/visits: for customers created from Square payments, recomputes sq_spend and sq_visits
     from the payment ledger, undoing the double counting from repeated Square notices.
  2. Rescores (exactly, not ratcheting) those customers plus every Grosse Pointe customer, who were
     previously excluded and pinned at a score of 25.
  3. Optional (--delete-blank-rows): removes customer rows with no name, email, phone or Square ID
     that nothing else references.

Only aggregate counts are printed.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import main  # noqa: E402


def run(apply: bool, delete_blank: bool):
    conn = main.get_conn()
    try:
        with conn.cursor() as cur:
            # --- 1. Square spend / visits from the ledger ---
            cur.execute(
                """
                WITH ledger AS (
                    SELECT cluster_id, COUNT(*) AS visits, ROUND(SUM(total_amount)::numeric, 2) AS spend
                    FROM unified_transactions
                    WHERE source_system = 'square' AND cluster_id IS NOT NULL
                    GROUP BY cluster_id
                )
                SELECT mc.cluster_id, mc.sq_visits, mc.sq_spend, mc.md_city, mc.zip5,
                       l.visits AS ledger_visits, l.spend AS ledger_spend
                FROM master_customers mc JOIN ledger l USING (cluster_id)
                WHERE mc.source = 'Square_POS'
                  AND (mc.sq_visits > l.visits OR mc.sq_spend > l.spend + 0.01);
                """
            )
            # Skip anyone in an excluded area: their payments never produced Purchase events,
            # so the ledger can't fully describe their history.
            to_fix = [r for r in cur.fetchall() if not main.is_detroit_excluded(r["md_city"], r["zip5"])]
            spend_removed = sum(float(r["sq_spend"]) - float(r["ledger_spend"]) for r in to_fix)
            visits_removed = sum(int(r["sq_visits"]) - int(r["ledger_visits"]) for r in to_fix)
            print(f"[1] Square customers to correct: {len(to_fix)}  "
                  f"(removes ${spend_removed:,.2f} of double-counted spend, {visits_removed} visits)")

            for r in to_fix:
                cur.execute(
                    """
                    UPDATE master_customers SET
                        sq_spend = %(spend)s,
                        sq_visits = %(visits)s,
                        true_lifetime_spend = GREATEST(%(spend)s, COALESCE(wr_spend, 0)),
                        instore_pos_spend = GREATEST(0, %(spend)s - COALESCE(wr_spend, 0)),
                        updated_at = NOW()
                    WHERE cluster_id = %(cid)s;
                    """,
                    {"spend": r["ledger_spend"], "visits": r["ledger_visits"], "cid": r["cluster_id"]},
                )

            # --- 2. Exact rescore ---
            cur.execute("SELECT * FROM master_customers WHERE zip5 = ANY(%s);", (list(main.ALLOWED_482_ZIPS),))
            gp_rows = [r for r in cur.fetchall() if not main.is_detroit_excluded(r["md_city"], r["zip5"])]
            ids = {r["cluster_id"] for r in to_fix} | {r["cluster_id"] for r in gp_rows}
            changed_score = changed_persona = 0
            for cid in sorted(ids):
                cur.execute("SELECT * FROM master_customers WHERE cluster_id = %s;", (cid,))
                before = cur.fetchone()
                after = main.rescore_customer(cur, before, exact=True)
                changed_score += before["bowler_value_score"] != after["bowler_value_score"]
                changed_persona += before["pg_mosaic_persona"] != after["pg_mosaic_persona"]
            print(f"[2] Rescored {len(ids)} customers ({len(gp_rows)} in Grosse Pointe): "
                  f"{changed_score} scores and {changed_persona} personas changed")

            # --- 3. Optional blank-row cleanup ---
            cur.execute(
                """
                SELECT COUNT(*) AS n FROM master_customers mc
                WHERE COALESCE(TRIM(first_name), '') = '' AND COALESCE(TRIM(last_name), '') = ''
                  AND COALESCE(TRIM(email), '') = '' AND COALESCE(TRIM(phone), '') = ''
                  AND COALESCE(TRIM(sq_id), '') = ''
                  AND NOT EXISTS (SELECT 1 FROM customer_identities ci WHERE ci.cluster_id = mc.cluster_id)
                  AND NOT EXISTS (SELECT 1 FROM unified_transactions ut WHERE ut.cluster_id = mc.cluster_id);
                """
            )
            blank = cur.fetchone()["n"]
            if delete_blank:
                cur.execute(
                    """
                    DELETE FROM master_customers mc
                    WHERE COALESCE(TRIM(first_name), '') = '' AND COALESCE(TRIM(last_name), '') = ''
                      AND COALESCE(TRIM(email), '') = '' AND COALESCE(TRIM(phone), '') = ''
                      AND COALESCE(TRIM(sq_id), '') = ''
                      AND NOT EXISTS (SELECT 1 FROM customer_identities ci WHERE ci.cluster_id = mc.cluster_id)
                      AND NOT EXISTS (SELECT 1 FROM unified_transactions ut WHERE ut.cluster_id = mc.cluster_id);
                    """
                )
                print(f"[3] Deleted {cur.rowcount} completely blank customer rows")
            else:
                print(f"[3] {blank} completely blank customer rows found (pass --delete-blank-rows to remove)")

        if apply:
            conn.commit()
            print("Changes committed.")
        else:
            conn.rollback()
            print("Dry run: nothing was changed. Re-run with --apply to commit.")
    finally:
        conn.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true")
    p.add_argument("--delete-blank-rows", action="store_true")
    args = p.parse_args()
    run(args.apply, args.delete_blank_rows)
