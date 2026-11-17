"""One-time identity recovery from data Square already has.

1. Payment history: for each Square customer ID that isn't in the database, look at its payments.
   - A receipt email (buyer_email_address) matching a known customer        -> link the ID to them.
   - Otherwise every card fingerprint pointing to the same known customer   -> link the ID to them.
   - Otherwise a receipt email of a new person                              -> create the customer.
   Ambiguous cases (emails or cards pointing at different customers) are skipped.
2. Square customer directory: profiles with an email or phone whose Square ID isn't linked yet
   are processed like a live customer.updated webhook (links existing customers or creates new ones).

Linked/created customers get their uncounted historical payments under that Square ID credited
(spend, visits, first visit) plus any parked payments, are rescored, and are pushed to the Meta
audiences when they qualify. Unambiguous card fingerprints are also stored as identities.

The dry run (default) is read-only: no Melissa calls, no Meta uploads, no database writes. It also
flags links to existing customers whose imported Square total already looks large enough to include
the credit (Square may have merged those profiles before the export).

Run where SQUARE_ACCESS_TOKEN is set (e.g. `render ssh <service> --ephemeral`):
    python scripts/recover_identities.py --since 2023-12-01            # dry run
    python scripts/recover_identities.py --since 2023-12-01 --apply    # do it
Only aggregate counts are printed.
"""
import argparse
import collections
import os
import sys

from fastapi import BackgroundTasks

try:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
except NameError:  # piped over stdin
    pass
sys.path.insert(0, "/opt/render/project/src")
import main  # noqa: E402


def identity_maps(cur):
    cur.execute("SELECT id_type, id_value, cluster_id FROM customer_identities "
                "WHERE id_type IN ('sq_id', 'email', 'phone', 'card_fingerprint');")
    m = collections.defaultdict(dict)
    for r in cur.fetchall():
        m[r["id_type"]][r["id_value"]] = r["cluster_id"]
    return m


def ledger_maps(cur):
    """Uncounted history and parked payments per Square ID, counted Square spend per customer,
    imported spend and first Square visit per customer."""
    cur.execute("""SELECT square_customer_id AS sq, count(*) AS n, COALESCE(sum(total_amount), 0) AS amt
                   FROM unified_transactions
                   WHERE source_system = 'square' AND cluster_id IS NULL AND external_customer_id IS NULL
                     AND square_customer_id IS NOT NULL GROUP BY 1;""")
    history = {r["sq"]: (int(r["n"]), float(r["amt"])) for r in cur.fetchall()}
    cur.execute("""SELECT external_customer_id AS sq, count(*) AS n, COALESCE(sum(total_amount), 0) AS amt
                   FROM unified_transactions
                   WHERE source_system = 'square' AND cluster_id IS NULL AND external_customer_id IS NOT NULL
                   GROUP BY 1;""")
    parked = {r["sq"]: (int(r["n"]), float(r["amt"])) for r in cur.fetchall()}
    cur.execute("""SELECT cluster_id, COALESCE(sum(total_amount), 0) AS amt FROM unified_transactions
                   WHERE source_system = 'square' AND cluster_id IS NOT NULL GROUP BY 1;""")
    linked = {r["cluster_id"]: float(r["amt"]) for r in cur.fetchall()}
    cur.execute("SELECT cluster_id, COALESCE(sq_spend, 0) AS sq_spend, sq_first_visit_date FROM master_customers;")
    custs = {r["cluster_id"]: (float(r["sq_spend"]), r["sq_first_visit_date"]) for r in cur.fetchall()}
    return history, parked, linked, custs


def scan_payments(since: str, known_sq: dict):
    fp_clusters = collections.defaultdict(set)
    unknown = collections.defaultdict(lambda: {"fps": set(), "emails": collections.Counter(),
                                               "names": collections.Counter(), "zips": collections.Counter()})
    cursor = None
    while True:
        params = {"begin_time": since, "sort_order": "ASC", "limit": 100}
        if cursor:
            params["cursor"] = cursor
        body = main.square_api("GET", "/v2/payments", params=params, timeout=30)
        if body is None:
            raise RuntimeError("Square payments list unavailable; nothing was changed")
        for p in body.get("payments") or []:
            if p.get("status") != "COMPLETED":
                continue
            sq = (p.get("customer_id") or "").strip()
            card = (p.get("card_details") or {}).get("card") or {}
            fp = card.get("fingerprint")
            if not sq:
                continue
            if sq in known_sq:
                if fp:
                    fp_clusters[fp].add(known_sq[sq])
                continue
            u = unknown[sq]
            if fp:
                u["fps"].add(fp)
            em = (p.get("buyer_email_address") or "").strip().lower()
            if em:
                u["emails"][em] += 1
            fn, ln = main.parse_cardholder_name(card.get("cardholder_name") or "")
            if fn and ln:
                u["names"][(fn, ln)] += 1
            z = ((card.get("billing_address") or {}).get("postal_code") or "")[:5]
            if len(z) == 5 and z.isdigit():
                u["zips"][z] += 1
        cursor = body.get("cursor")
        if not cursor:
            return fp_clusters, unknown


def iter_directory():
    cursor = None
    while True:
        params = {"limit": 100}
        if cursor:
            params["cursor"] = cursor
        body = main.square_api("GET", "/v2/customers", params=params, timeout=30)
        if body is None:
            print("Square customer list unavailable part-way; directory pass is partial")
            return
        yield from body.get("customers") or []
        cursor = body.get("cursor")
        if not cursor:
            return


def classify(u, ids, fp_clusters):
    email_clusters = {ids["email"][e] for e in u["emails"] if e in ids["email"]}
    fp_hits, ambiguous_fp = set(), False
    for fp in u["fps"]:
        c = fp_clusters.get(fp) or set()
        ambiguous_fp |= len(c) > 1
        fp_hits |= c
    if len(email_clusters) == 1:
        return "linked_by_receipt_email", next(iter(email_clusters))
    if len(email_clusters) > 1:
        return "skipped_emails_point_to_different_customers", None
    if len(fp_hits) == 1 and not ambiguous_fp:
        return "linked_by_card_fingerprint", next(iter(fp_hits))
    if fp_hits:
        return "skipped_card_shared_by_several_customers", None
    if u["emails"]:
        return "new_customer_from_receipt_email", None
    return "no_usable_signal", None


def finish(cur, cust: dict, sq: str, credit_history: bool, touched: set) -> dict:
    """Credit parked payments (never counted anywhere) and, unless held back, the uncounted
    history under this Square ID; then rescore."""
    cust = main.claim_pending_square_payments(cur, cust, sq)
    if credit_history:
        cust = main.attach_square_history(cur, cust, sq)
    cust = main.rescore_customer(cur, cust)
    touched.add(cust["cluster_id"])
    return cust


def add_identity(cur, cluster_id, id_type, value, source="Square_Recovery"):
    cur.execute("INSERT INTO customer_identities (cluster_id, id_type, id_value, source_system) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING 1;", (cluster_id, id_type, value, source))
    return bool(cur.fetchone())


OUTCOMES = ("linked_by_receipt_email", "linked_by_card_fingerprint", "new_customer_from_receipt_email",
            "directory_linked_existing", "directory_new_customer",
            "skipped_emails_point_to_different_customers", "skipped_card_shared_by_several_customers",
            "no_usable_signal")


def run(since: str, apply: bool):
    st, money, pay, actual = (collections.Counter() for _ in range(4))
    touched = set()
    conn = main.get_conn()
    with conn.cursor() as cur:
        ids = identity_maps(cur)
        history, parked, linked_spend, custs = ledger_maps(cur)
    conn.commit()

    # ---------- Plan (read-only) ----------
    fp_clusters, unknown = scan_payments(since, ids["sq_id"])
    st["unknown_square_ids_with_payments"] = len(unknown)
    new_fps = [(fp, next(iter(c))) for fp, c in fp_clusters.items()
               if len(c) == 1 and fp not in ids["card_fingerprint"]]

    plan = []                                 # (how, sq, existing cluster or None, payload)
    planned_emails = {}
    for sq, u in unknown.items():
        how, target = classify(u, ids, fp_clusters)
        if how == "new_customer_from_receipt_email":
            top = u["emails"].most_common(1)[0][0]
            if top in planned_emails:          # same new person seen under another Square ID
                how, target = "linked_to_planned_new_customer", None
            planned_emails.setdefault(top, sq)
        plan.append((how, sq, target, u))
    in_plan = {sq for how, sq, _, _ in plan if not how.startswith("skipped") and how != "no_usable_signal"}
    for cu in iter_directory():
        sid = cu.get("id")
        em = (cu.get("email_address") or "").strip().lower()
        ph = main.clean_phone(cu.get("phone_number"))
        if not sid or sid in ids["sq_id"] or sid in in_plan or not (em or ph):
            continue
        existing = ids["email"].get(em) or ids["phone"].get(ph)
        plan.append(("directory_linked_existing" if existing else "directory_new_customer", sid, existing, cu))
        in_plan.add(sid)

    # History credit per existing customer, and the double-count check.
    planned_credit = collections.Counter()
    for how, sq, target, _ in plan:
        if target is not None:
            planned_credit[target] += history.get(sq, (0, 0.0))[1]
    held_back = set()
    for cid, amt in planned_credit.items():
        spend, _ = custs.get(cid, (0.0, None))
        if amt > 0 and spend - linked_spend.get(cid, 0.0) >= amt - 1:
            held_back.add(cid)

    for how, sq, target, _ in plan:
        n, amt = history.get(sq, (0, 0.0))
        pn, pamt = parked.get(sq, (0, 0.0))
        st[how] += 1
        pay[how] += n + pn
        money[how] += amt + pamt
        if target in held_back:
            st["held_back_ids"] += 1
            money["held_back"] += amt

    # Tighter check: customers whose first Square visit is inside the payment history, so their
    # imported total is fully covered by the ledger and the comparison is exact.
    exact = [cid for cid, amt in planned_credit.items()
             if amt > 0 and custs.get(cid, (0, None))[1] and str(custs[cid][1]) >= since[:10]]
    exact_flagged = [cid for cid in exact if cid in held_back]

    # ---------- Apply ----------
    if apply:
        with conn, conn.cursor() as cur:
            for fp, cid in new_fps:
                add_identity(cur, cid, "card_fingerprint", fp, "Square")
        for how, sq, target, payload in plan:
            if how.startswith("skipped") or how == "no_usable_signal":
                continue
            try:
                with conn, conn.cursor() as cur:
                    if how.startswith("directory"):
                        res = main.handle_square_customer(cur, "customer.updated", payload, BackgroundTasks())
                        cid = res.get("cluster_id")
                        if not cid:
                            continue
                        cur.execute("SELECT * FROM master_customers WHERE cluster_id = %s;", (cid,))
                        before = cur.fetchone()
                    elif target is not None:
                        if not add_identity(cur, target, "sq_id", sq):
                            st["skipped_already_linked_meanwhile"] += 1
                            continue
                        for e in payload["emails"]:
                            if e not in ids["email"]:
                                add_identity(cur, target, "email", e)
                        cur.execute("SELECT * FROM master_customers WHERE cluster_id = %s;", (target,))
                        before = cur.fetchone()
                        if not before:
                            continue
                    else:                      # new person from receipt email(s)
                        u = payload
                        email = u["emails"].most_common(1)[0][0]
                        fn, ln = u["names"].most_common(1)[0][0] if u["names"] else ("", "")
                        zip5 = u["zips"].most_common(1)[0][0] if u["zips"] else ""
                        cur.execute("SELECT * FROM master_customers WHERE FALSE;")
                        before = None
                        cust, _ = main.upsert_and_enrich_customer(cur, {
                            "sq_id": sq, "email": email, "first_name": fn, "last_name": ln,
                            "zip": zip5, "avs_postal_code": zip5, "in_square": 1, "source": "Square_Recovery",
                        })
                        for e in u["emails"]:
                            add_identity(cur, cust["cluster_id"], "email", e)
                        ids["email"][email] = cust["cluster_id"]
                        before = cust
                    cid = before["cluster_id"]
                    spend0 = float(before.get("sq_spend") or 0) if how != "new_customer_from_receipt_email" else 0.0
                    if how == "new_customer_from_receipt_email" or (how == "directory_new_customer"):
                        cur.execute("SELECT COALESCE(sum(total_amount),0) AS a FROM unified_transactions "
                                    "WHERE cluster_id = %s AND square_customer_id = %s;", (cid, sq))
                        spend0 = float(before.get("sq_spend") or 0) - float(cur.fetchone()["a"])
                    cust = finish(cur, before, sq, cid not in held_back, touched)
                    actual[how] += float(cust.get("sq_spend") or 0) - spend0
            except Exception as e:
                st["errors"] += 1
                print("  error on one Square ID:", type(e).__name__, str(e)[:120])

        if touched:
            with conn.cursor() as cur:
                thresh = main.get_threshold(cur)
                cur.execute("SELECT * FROM master_customers WHERE cluster_id = ANY(%s);", (list(touched),))
                rows = cur.fetchall()
            conn.commit()
            for c in rows:
                if (c.get("bowler_value_score") or 0) >= thresh and (c.get("email") or c.get("phone")) \
                        and not main.is_detroit_excluded(c.get("md_city"), c.get("zip5")):
                    main.sync_customer_to_meta_custom_audiences(dict(c))
                    st["pushed_to_meta_audiences"] += 1
    conn.close()

    print("APPLIED" if apply else "DRY RUN (nothing written, no Melissa/Meta calls)")
    print(f"Unknown Square IDs with payments: {st['unknown_square_ids_with_payments']:,} | "
          f"card fingerprints {'stored' if apply else 'to store'}: {len(new_fps):,}")
    print(f"{'outcome':48s} {'Square IDs':>10s} {'payments':>9s} {'available':>13s}" + ("   credited" if apply else ""))
    for k in OUTCOMES + ("linked_to_planned_new_customer",):
        if st[k] or k in OUTCOMES:
            print(f"{k:48s} {st[k]:>10,} {pay[k]:>9,} ${money[k]:>12,.2f}" + (f"  ${actual[k]:>12,.2f}" if apply else ""))
    print(f"Existing customers with history to credit: {sum(1 for v in planned_credit.values() if v > 0):,} | "
          f"held back (credit may already be in imported total): {len(held_back):,} customers, "
          f"{st['held_back_ids']:,} Square IDs, ${money['held_back']:,.2f} (linked, not credited)")
    print(f"Tighter check (first Square visit on/after {since[:10]}): {len(exact):,} customers, "
          f"{len(exact_flagged):,} look already counted")
    for k in ("skipped_already_linked_meanwhile", "pushed_to_meta_audiences", "errors"):
        if st[k]:
            print(f"{k}: {st[k]:,}")
    if apply:
        print("Customers touched:", len(touched))


if __name__ == "__main__":
    a = argparse.ArgumentParser()
    a.add_argument("--since", required=True)
    a.add_argument("--apply", action="store_true")
    args = a.parse_args()
    run(args.since if "T" in args.since else args.since + "T00:00:00Z", args.apply)
