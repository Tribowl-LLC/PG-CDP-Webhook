"""Square payment + item history backfill.

Pulls every Square payment since --since, plus each payment's order (what was bought: price keys,
quantities, amounts), and records them in the unified_transactions ledger.

* Nothing is added to anyone's spend or visits: the original import already holds Square's lifetime
  totals. Historical rows are recorded for item-level history and audit only, and are never
  auto-credited later (they don't use external_customer_id).
* Optional: --credit-between START END credits payments in that window that the system never counted
  (e.g. between the Square export and the moment the live webhooks went live). Use the dry-run report
  to choose the window.

Run where SQUARE_ACCESS_TOKEN is set (e.g. `render ssh <service> --ephemeral`):
    python scripts/backfill_square_history.py --since 2023-12-01              # dry run: report only
    python scripts/backfill_square_history.py --since 2023-12-01 --apply      # record history + items

Only aggregate counts are printed.
"""
import argparse
import collections
import json
import os
import sys
import time
from datetime import datetime

from psycopg2.extras import execute_values

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import main  # noqa: E402


def iter_payment_pages(since: str):
    cursor = None
    while True:
        params = {"begin_time": since, "sort_order": "ASC", "limit": 100}
        if cursor:
            params["cursor"] = cursor
        body = main.square_api("GET", "/v2/payments", params=params, timeout=30)
        if body is None:
            raise RuntimeError("Square payments list unavailable; re-run to continue")
        yield body.get("payments") or []
        cursor = body.get("cursor")
        if not cursor:
            return


def fetch_orders(order_ids: list) -> dict:
    out = {}
    for i in range(0, len(order_ids), 100):
        chunk = order_ids[i:i + 100]
        body = main.square_api("POST", "/v2/orders/batch-retrieve", json_body={"order_ids": chunk}, timeout=30)
        for o in (body or {}).get("orders") or []:
            out[o["id"]] = o
    return out


def parse_ts(v: str):
    v = (v or "").strip().replace("Z", "+00:00")
    if len(v) == 10:
        v += "T00:00:00+00:00"
    return datetime.fromisoformat(v)


def net_dollars(p: dict) -> float:
    total = main.to_int((p.get("total_money") or p.get("amount_money") or {}).get("amount"))
    refunded = main.to_int((p.get("refunded_money") or {}).get("amount"))
    return round((total - refunded) / 100.0, 2)


def run(since: str, apply: bool, credit_between=None):
    conn = main.get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT id_value, cluster_id FROM customer_identities WHERE id_type = 'sq_id';")
        sq_to_cluster = {r["id_value"]: r["cluster_id"] for r in cur.fetchall()}
        cur.execute("SELECT 1 FROM information_schema.columns WHERE table_name = 'unified_transactions' "
                    "AND column_name = 'line_items';")
        if not cur.fetchone():
            if apply:
                sys.exit("Run migrations/003_square_line_items.sql first")
            print("(migration 003 not applied yet: dry run treats every payment as lacking items)")
            cur.execute("SELECT transaction_id, FALSE AS has_items FROM unified_transactions WHERE source_system = 'square';")
        else:
            cur.execute("SELECT transaction_id, (line_items IS NOT NULL) AS has_items FROM unified_transactions "
                        "WHERE source_system = 'square';")
        ledger = {r["transaction_id"]: r["has_items"] for r in cur.fetchall()}
        cur.execute("SELECT cluster_id, COALESCE(sq_spend, 0) AS sq_spend, COALESCE(sq_visits, 0) AS sq_visits, "
                    "source FROM master_customers WHERE COALESCE(sq_spend, 0) > 0 OR COALESCE(sq_visits, 0) > 0;")
        db_totals = {r["cluster_id"]: r for r in cur.fetchall()}
    conn.commit()

    st = collections.Counter()
    money = collections.Counter()
    hist = collections.defaultdict(lambda: [0.0, 0])  # cluster -> [spend, payments]
    unknown_sq = set()
    gap = collections.Counter()      # hour bucket -> payments not in the ledger, Nov 14-16
    gap_money = collections.Counter()
    to_credit = []                   # (txn_id, cluster_id or None, sq_id, net)

    for page in iter_payment_pages(since):
        rows, need_orders, page_credit = [], [], []
        for p in page:
            st["payments_listed"] += 1
            if p.get("status") != "COMPLETED":
                st["not_completed"] += 1
                continue
            st["completed"] += 1
            net = net_dollars(p)
            money["completed_net"] += net
            txn = f"sq_pay_{p['id']}"
            sq = (p.get("customer_id") or "").strip()
            cid = sq_to_cluster.get(sq) if sq else None
            if not sq:
                st["no_square_customer"] += 1
            elif cid is None:
                st["square_customer_not_in_db"] += 1
                unknown_sq.add(sq)
            else:
                st["known_customer"] += 1
                hist[cid][0] += net
                hist[cid][1] += 1
            created = p.get("created_at") or ""
            in_ledger = txn in ledger
            if not in_ledger and "2026-11-14" <= created[:10] <= "2026-11-16":
                gap[created[:13]] += 1
                gap_money[created[:13]] += net
            if credit_between and not in_ledger and created and credit_between[0] <= parse_ts(created) < credit_between[1]:
                to_credit.append((txn, cid, sq, net))
                page_credit.append((txn, cid, sq, net))
            if not in_ledger or not ledger.get(txn):
                if p.get("order_id"):
                    need_orders.append(p["order_id"])
                rows.append((txn, p, cid, sq, net, in_ledger))
        if not apply or not rows:
            continue
        orders = fetch_orders(list(dict.fromkeys(need_orders)))
        values = []
        for txn, p, cid, sq, net, in_ledger in rows:
            items = main.order_line_items(orders.get(p.get("order_id"))) if p.get("order_id") in orders else None
            if items is not None:
                st["orders_with_items"] += 1
            values.append((txn, cid, "square", p.get("created_at"), net, p.get("updated_at"), sq or None,
                           json.dumps(items) if items is not None else None,
                           [i["variation_id"] for i in (items or []) if i["variation_id"]] or None))
        with conn, conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO unified_transactions (transaction_id, cluster_id, source_system, transaction_date,
                    total_amount, source_updated_at, square_customer_id, line_items, variation_ids)
                VALUES %s
                ON CONFLICT (transaction_id) DO UPDATE SET
                    line_items = COALESCE(unified_transactions.line_items, EXCLUDED.line_items),
                    variation_ids = COALESCE(unified_transactions.variation_ids, EXCLUDED.variation_ids),
                    square_customer_id = COALESCE(unified_transactions.square_customer_id, EXCLUDED.square_customer_id),
                    source_updated_at = COALESCE(unified_transactions.source_updated_at, EXCLUDED.source_updated_at)
            """, values, template="(%s, %s, %s, %s::timestamptz, %s, %s::timestamptz, %s, %s::jsonb, %s)")
            # Credit the window in the SAME transaction that first records these payments, so a run that
            # stops part-way never leaves a payment recorded but uncredited (a re-run would skip it).
            for txn, cid, sq, net in page_credit:
                if cid:
                    main.adjust_spend(cur, cid, sq_diff=net, sq_visits=1)
                    st["credited_to_known_customers"] += 1
                elif sq:
                    cur.execute("UPDATE unified_transactions SET external_customer_id = %s WHERE transaction_id = %s "
                                "AND cluster_id IS NULL;", (sq, txn))
                    st["parked_for_later_identification"] += 1
        for txn, *_ in rows:
            ledger[txn] = True
        st["ledger_rows_written"] += len(values)
        time.sleep(0.05)

    if not apply and to_credit:
        st["would_credit_or_park"] = len(to_credit)
        money["would_credit_net"] = round(sum(x[3] for x in to_credit), 2)
    conn.close()

    # --- Report: how well do the imported totals match the real payment history? ---
    cmp = collections.Counter()
    for cid, (spend, n) in hist.items():
        db = db_totals.get(cid)
        if not db:
            cmp["history_but_no_db_spend"] += 1
            continue
        d = float(db["sq_spend"]) - spend
        if abs(d) <= 1:
            cmp["spend_matches_within_$1"] += 1
        elif abs(d) <= max(5.0, 0.10 * spend):
            cmp["spend_within_10pct"] += 1
        elif d > 0:
            cmp["db_spend_higher_than_history"] += 1
        else:
            cmp["db_spend_lower_than_history"] += 1
        cmp["visits_match"] += int(int(db["sq_visits"]) == n)
        if db["source"] == "Square_Instant_Profile":
            cmp["instant_profiles_checked"] += 1
            cmp["instant_profile_spend_mismatch"] += int(abs(d) > max(5.0, 0.10 * spend))
    print("Payments:", dict(st))
    print("Money:", {k: round(v, 2) for k, v in money.items()})
    print("Distinct Square customer IDs not in the database:", len(unknown_sq))
    print("Customers with payment history:", len(hist), "| comparison with imported totals:", dict(cmp))
    print("Payments NOT already in the ledger, Nov 14-16 (UTC hour: count / $):")
    for h in sorted(gap):
        print(f"   {h}:00  {gap[h]:4d}  ${gap_money[h]:,.2f}")
    print("Applied." if apply else "Dry run: nothing written. Re-run with --apply to write.")


if __name__ == "__main__":
    a = argparse.ArgumentParser()
    a.add_argument("--since", required=True, help="ISO date/time, e.g. 2023-12-01")
    a.add_argument("--apply", action="store_true")
    a.add_argument("--credit-between", nargs=2, metavar=("START", "END"),
                   help="credit uncounted payments created in [START, END), ISO UTC, e.g. 2026-11-14T18:00Z 2026-11-15T02:12Z")
    args = a.parse_args()
    since = args.since if "T" in args.since else args.since + "T00:00:00Z"
    window = (parse_ts(args.credit_between[0]), parse_ts(args.credit_between[1])) if args.credit_between else None
    run(since, args.apply, window)
