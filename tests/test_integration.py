"""End-to-end webhook tests against a disposable database copy (see conftest.py)."""
import json
import threading

from fastapi import BackgroundTasks

import main

WH = {"X-CDP-Key": "test-webhook-secret"}


def square_post(client, body: dict):
    raw = json.dumps(body).encode()
    sig = main.square_signature(raw, main.SQUARE_WEBHOOK_SIGNATURE_KEY, main.SQUARE_WEBHOOK_URL)
    return client.post("/webhooks/square", content=raw,
                       headers={"x-square-hmacsha256-signature": sig, "content-type": "application/json"})


def payment(pid, cents=5000, refund=0, sq_id=None, email=None, name=None, zip5="48335", etype="payment.updated"):
    p = {"id": pid, "status": "COMPLETED", "total_money": {"amount": cents}, "amount_money": {"amount": cents},
         "card_details": {"card": {"billing_address": {"postal_code": zip5}}}}
    if refund:
        p["refunded_money"] = {"amount": refund}
    if sq_id:
        p["customer_id"] = sq_id
    if email:
        p["buyer_email_address"] = email
    if name:
        p["card_details"]["card"]["cardholder_name"] = name
    return {"type": etype, "data": {"object": {"payment": p}}}


def customer_row(db, cluster_id):
    db.execute("SELECT * FROM master_customers WHERE cluster_id = %s", (cluster_id,))
    return db.fetchone()


def capi_rows(db, like):
    db.execute("SELECT * FROM capi_events_log WHERE event_id LIKE %s", (like,))
    return db.fetchall()


# --- Square payments -------------------------------------------------------------------------

def test_repeated_square_notices_count_once(client, db, fake, uid):
    email = f"pay-{uid}@example.test"
    r1 = square_post(client, payment(f"P{uid}", 5000, email=email, etype="payment.created")).json()
    r2 = square_post(client, payment(f"P{uid}", 5000, email=email)).json()
    r3 = square_post(client, payment(f"P{uid}", 5000, email=email)).json()
    assert r1["status"] == "payment_attributed"
    assert r2["status"] == r3["status"] == "ignored_duplicate"
    c = customer_row(db, r1["cluster_id"])
    assert float(c["sq_spend"]) == 50.0 and c["sq_visits"] == 1
    assert len(capi_rows(db, f"sq_pay_P{uid}")) == 1
    assert fake.count("/events") == 1
    assert fake.meta_events()[0]["action_source"] == "physical_store"


def test_refund_reduces_spend(client, db, fake, uid):
    email = f"refund-{uid}@example.test"
    r = square_post(client, payment(f"R{uid}", 8000, email=email)).json()
    adj = square_post(client, payment(f"R{uid}", 8000, refund=3000, email=email)).json()
    assert adj == {"status": "amount_adjusted", "transaction_id": f"sq_pay_R{uid}", "delta": -30.0}
    c = customer_row(db, r["cluster_id"])
    assert float(c["sq_spend"]) == 50.0 and float(c["true_lifetime_spend"]) == 50.0 and c["sq_visits"] == 1


def test_simultaneous_duplicates_count_once(db, fake, uid):
    email = f"race-{uid}@example.test"
    body = payment(f"C{uid}", 4000, email=email)
    results = []

    def go():
        results.append(main.handle_square_event(json.loads(json.dumps(body)), BackgroundTasks()))

    threads = [threading.Thread(target=go) for _ in range(6)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    statuses = sorted(r["status"] for r in results)
    assert statuses.count("payment_attributed") == 1, statuses
    cid = next(r["cluster_id"] for r in results if r["status"] == "payment_attributed")
    c = customer_row(db, cid)
    assert float(c["sq_spend"]) == 40.0 and c["sq_visits"] == 1


def test_anonymous_payment_creates_no_customer(client, db, fake, uid):
    db.execute("SELECT COUNT(*) AS n FROM master_customers")
    before = db.fetchone()["n"]
    r = square_post(client, payment(f"A{uid}", 1200)).json()
    assert r["status"] == "skipped_anonymous_payment"
    db.execute("SELECT COUNT(*) AS n FROM master_customers")
    assert db.fetchone()["n"] == before
    assert fake.count("/events") == 0
    # A repeat of it is still recognised as already handled
    assert square_post(client, payment(f"A{uid}", 1200)).json()["status"] == "ignored_duplicate"


def test_blank_square_profile_is_parked_then_credited(client, db, fake, uid):
    sq = f"SQ{uid}"
    fake.square_customers[sq] = {"id": sq}  # Square knows the ID but has no contact details yet
    r1 = square_post(client, payment(f"B1{uid}", 3000, sq_id=sq)).json()
    r2 = square_post(client, payment(f"B2{uid}", 2000, sq_id=sq)).json()
    assert r1["status"] == r2["status"] == "parked_unidentified_square_customer"

    # Later the front desk adds the customer's email in Square
    r3 = square_post(client, {"type": "customer.updated", "data": {"object": {"customer": {
        "id": sq, "given_name": "Parker", "family_name": f"Tester{uid}", "email_address": f"park-{uid}@example.test",
    }}}}).json()
    c = customer_row(db, r3["cluster_id"])
    assert float(c["sq_spend"]) == 50.0 and c["sq_visits"] == 2

    # And the next payment goes straight to them without asking Square again
    lookups = fake.count("/v2/customers/")
    r4 = square_post(client, payment(f"B3{uid}", 1000, sq_id=sq)).json()
    assert r4["cluster_id"] == r3["cluster_id"]
    assert fake.count("/v2/customers/") == lookups
    assert float(customer_row(db, r3["cluster_id"])["sq_spend"]) == 60.0


def test_square_lookup_failure_asks_square_to_retry(client, db, fake, uid):
    sq = f"SQE{uid}"
    fake.square_customers[sq] = "error"
    resp = square_post(client, payment(f"E{uid}", 2500, sq_id=sq))
    assert resp.status_code == 503
    db.execute("SELECT 1 FROM unified_transactions WHERE transaction_id = %s", (f"sq_pay_E{uid}",))
    assert db.fetchone() is None  # rolled back, so Square's retry is processed normally

    fake.square_customers[sq] = {"id": sq, "email_address": f"retry-{uid}@example.test"}
    r = square_post(client, payment(f"E{uid}", 2500, sq_id=sq)).json()
    assert r["status"] == "payment_attributed"


# --- Enrichment ------------------------------------------------------------------------------

def test_unmatchable_customer_is_not_looked_up_twice(client, db, fake, uid):
    email = f"nomatch-{uid}@example.test"
    square_post(client, payment(f"N1{uid}", 1000, email=email))
    square_post(client, payment(f"N2{uid}", 1000, email=email))
    square_post(client, payment(f"N3{uid}", 1000, email=email))
    assert fake.count("melissadata") == 1


def test_melissa_outage_is_retried_later(client, db, fake, uid):
    email = f"timeout-{uid}@example.test"
    r = square_post(client, payment(f"T1{uid}", 1000, email=email)).json()
    assert customer_row(db, r["cluster_id"])["md_results"] is None
    square_post(client, payment(f"T2{uid}", 1000, email=email))
    assert fake.count("melissadata") == 2


def test_existing_customer_enrichment_saves_all_fields(client, db, fake, uid):
    email = f"later-{uid}@example.test"
    # First seen with no usable input for Melissa, then again with an email
    r = client.post("/webhooks/webres", headers=WH, json={"reservation_id": f"W0{uid}", "first_name": "Late",
                                                           "last_name": f"Tester{uid}", "amount": 0}).json()
    client.post("/webhooks/webres", headers=WH, json={"reservation_id": f"W1{uid}", "first_name": "Late",
                                                       "last_name": f"Tester{uid}", "email": email, "amount": 0})
    c = customer_row(db, r["cluster_id"])
    assert c["md_address1"] == "100 Test St" and c["md_latitude"] is not None and c["md_results"] == "AS01"


# --- Meta ------------------------------------------------------------------------------------

def test_meta_outage_keeps_customer_and_retries(client, db, fake, uid):
    email = f"metadown-{uid}@example.test"
    fake.meta_mode = "timeout"
    r = square_post(client, payment(f"M{uid}", 2000, email=email)).json()
    assert r["status"] == "payment_attributed"
    assert float(customer_row(db, r["cluster_id"])["sq_spend"]) == 20.0
    row = capi_rows(db, f"sq_pay_M{uid}")[0]
    assert row["sent_to_meta"] is False and row["meta_response_status"] == "NETWORK_ERROR"

    # Meta recovers; the same event is re-sent (spend is not counted again)
    fake.meta_mode = "ok"
    cust = customer_row(db, r["cluster_id"])
    db.connection.autocommit = False
    main.fire_meta_capi_event(db, cust, "Purchase", 20.0, 55, event_id=f"sq_pay_M{uid}")
    db.connection.commit()
    db.connection.autocommit = True
    assert capi_rows(db, f"sq_pay_M{uid}")[0]["sent_to_meta"] is True


def test_meta_gets_country_code_phone_and_compact_city(client, db, fake, uid):
    r = client.post("/webhooks/webres", headers=WH, json={
        "reservation_id": f"MP{uid}", "first_name": "Phone", "last_name": f"Tester{uid}",
        "email": f"phone-{uid}@example.test", "phone": "(248) 555-0142", "amount": 40}).json()
    ev = fake.meta_events()[0]
    assert ev["event_id"] == f"wr_MP{uid}"
    assert main.sha256_hash("12485550142") in ev["user_data"]["ph"]
    assert ev["user_data"]["ct"] == [main.sha256_hash("farmingtonhills")]
    assert r["status"] == "webres_synced"


def test_grosse_pointe_is_marketed_detroit_is_not(client, db, fake, uid):
    gp = square_post(client, payment(f"GP{uid}", 3000, email=f"grossepointe-{uid}@example.test")).json()
    dt = square_post(client, payment(f"DT{uid}", 3000, email=f"detroit-{uid}@example.test")).json()
    assert len(capi_rows(db, f"sq_pay_GP{uid}")) == 1
    assert len(capi_rows(db, f"sq_pay_DT{uid}")) == 0
    assert customer_row(db, gp["cluster_id"])["bowler_value_score"] > 25
    assert customer_row(db, dt["cluster_id"])["bowler_value_score"] == 25


# --- WebRes / Tripleseat / Partywirks ----------------------------------------------------------

def test_webres_repeat_counted_once(client, db, fake, uid):
    body = {"reservation_id": f"WR{uid}", "first_name": "Web", "last_name": f"Tester{uid}",
            "email": f"web-{uid}@example.test", "amount": 60, "reservation_date": "2026-11-20T18:00:00"}
    r = client.post("/webhooks/webres", headers=WH, json=body).json()
    client.post("/webhooks/webres", headers=WH, json=body)
    c = customer_row(db, r["cluster_id"])
    assert float(c["wr_spend"]) == 60.0 and c["wr_bookings"] == 1


def test_webres_without_id_does_not_put_email_in_event_id(client, db, fake, uid):
    email = f"noid-{uid}@example.test"
    client.post("/webhooks/webres", headers=WH, json={"first_name": "No", "last_name": f"Id{uid}",
                                                       "email": email, "amount": 25, "date": "2026-11-21"})
    ev = fake.meta_events()[0]
    assert email not in ev["event_id"] and "noid" not in ev["event_id"]


def test_tripleseat_resends_give_one_qualified_bowler(client, db, fake, uid):
    lead = {"lead": {"id": f"L{uid}", "first_name": "Corp", "last_name": f"Planner{uid}",
                     "email_address": f"corp-{uid}@example.test", "company": "Test Co", "guest_count": "40",
                     "event_date": "2026-12-12", "status": "prospect"}}
    for _ in range(4):
        r = client.post("/webhooks/tripleseat", headers=WH, json=lead).json()
    # The lead becomes an event on the same date
    client.post("/webhooks/tripleseat", headers=WH, json={"event": {**lead["lead"], "id": f"E{uid}", "status": "definite"}})
    qb = capi_rows(db, f"qb_ts_{r['cluster_id']}_%")
    assert len(qb) == 1
    assert customer_row(db, r["cluster_id"])["ts_events"] == 2  # one lead + one event, not 5


def test_partywirks_resends_give_one_qualified_bowler(client, db, fake, uid):
    body = {"reservation": {"id": f"PW{uid}", "customer": {"first_name": "Party", "last_name": f"Parent{uid}",
            "email": f"party-{uid}@example.test"}, "package_name": "BOWLING PARTY", "party_date": "2026-12-05"}}
    for _ in range(3):
        r = client.post("/webhooks/partywirks", headers=WH, json=body).json()
    assert len(capi_rows(db, f"qb_pw_PW{uid}")) == 1
    assert customer_row(db, r["cluster_id"])["pw_bookings"] == 1


# --- Authentication ----------------------------------------------------------------------------

def test_endpoints_reject_missing_or_wrong_credentials(client, fake):
    raw = json.dumps(payment("X", 100)).encode()
    assert client.post("/webhooks/square", content=raw).status_code == 401
    assert client.post("/webhooks/square", content=raw,
                       headers={"x-square-hmacsha256-signature": "bad"}).status_code == 401
    for path in ("/webhooks/webres", "/webhooks/tripleseat", "/webhooks/partywirks"):
        assert client.post(path, json={}).status_code == 401
        assert client.post(path, json={}, headers={"X-CDP-Key": "wrong"}).status_code == 401
    for path in ("/cron/enrich-partywirks", "/cron/recover-square-avs-zips", "/cron/adjust-thermostat",
                 "/cron/sync-square-catalog"):
        assert client.post(path).status_code == 401
        assert client.post(path, headers=WH).status_code == 401  # webhook key doesn't open cron endpoints
    assert fake.calls == []


def test_query_param_key_works_for_vendor_webhooks(client, fake):
    assert client.post("/webhooks/webres?key=test-webhook-secret", json={}).status_code == 200
