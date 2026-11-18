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


def test_partywirks_webhook_uses_api_and_counts_once(client, db, fake, uid):
    oid = f"26{int(uid, 16)}"
    body = fake.add_partywirks(uid, oid, package="BIRTHDAY EXTRAVAGANZA", event_date="2026-12-05")
    body["data"] = {"order_id": oid}  # even a bare notification works: details come from the API
    for _ in range(3):
        r = client.post("/webhooks/partywirks", headers=WH, json=body).json()
    c = customer_row(db, r["cluster_id"])
    assert c["pw_last_package"] == "BIRTHDAY EXTRAVAGANZA"
    assert str(c["pw_last_party_date"]) == "2026-12-05"
    assert c["pw_bookings"] == 1
    assert c["md_address1"] == "100 Test St"   # Melissa was given the parent's street address
    melissa = [kw["params"] for m, url, kw in fake.calls if "melissadata" in url]
    assert melissa[0]["a1"] == "200 Party Ln" and melissa[0]["postal"] == "48335"
    assert len(capi_rows(db, f"qb_pw_{oid}")) == 1
    # Children's details from the reservation are never stored
    assert "Kid" not in json.dumps(c, default=str) and "8th Birthday" not in json.dumps(c, default=str)


def test_partywirks_unknown_order_is_ignored(client, db, fake, uid):
    r = client.post("/webhooks/partywirks", headers=WH, json={"data": {
        "order_id": f"FAKE{uid}", "customer": {"name": "Forged Person", "email": f"forged-{uid}@example.test"}}}).json()
    assert r["status"] == "ignored_unknown_partywirks_reservation"
    db.execute("SELECT 1 FROM master_customers WHERE email = %s", (f"forged-{uid}@example.test",))
    assert db.fetchone() is None


def test_partywirks_api_down_falls_back_to_webhook_body(client, db, fake, uid):
    oid = f"27{int(uid, 16)}"
    body = fake.add_partywirks(uid, oid, package="TEEN PARTY")
    fake.pw_down = True
    r = client.post("/webhooks/partywirks", headers=WH, json=body).json()
    c = customer_row(db, r["cluster_id"])
    assert c["pw_last_package"] == "TEEN PARTY" and c["pw_bookings"] == 1


def test_partywirks_cancelled_is_not_counted(client, db, fake, uid):
    oid = f"28{int(uid, 16)}"
    body = fake.add_partywirks(uid, oid, status="cancelled")
    r = client.post("/webhooks/partywirks", headers=WH, json=body).json()
    assert r["status"] == "partywirks_cancelled_reservation"
    assert customer_row(db, r["cluster_id"])["pw_bookings"] in (0, None)
    assert capi_rows(db, f"qb_pw_{oid}") == []


def test_partywirks_catch_up_sync(db, fake, uid, monkeypatch):
    monkeypatch.setattr(main.time, "sleep", lambda s: None)
    fake.add_partywirks(uid, f"30{int(uid, 16)}", created_at="2026-11-01 09:00:00", event_date="2099-01-01")       # old: imported
    fake.add_partywirks(uid[::-1], f"31{int(uid, 16)}", created_at="2026-11-16 09:00:00", event_date="2099-01-02")  # missed webhook
    first = main.run_partywirks_sync(days_back=1, count_bookings_created_since="2026-11-15 00:00:00")
    assert first["new_recorded_without_counting"] >= 1 and first["new_counted"] >= 1
    db.execute("SELECT pw_bookings FROM master_customers WHERE email = %s", (f"party-{uid}@example.test",))
    assert db.fetchone()["pw_bookings"] in (0, None)   # old booking recorded but not counted again
    db.execute("SELECT pw_bookings FROM master_customers WHERE email = %s", (f"party-{uid[::-1]}@example.test",))
    assert db.fetchone()["pw_bookings"] == 1
    assert capi_rows(db, f"qb_pw_30{int(uid, 16)}") == []         # no conversion for an old booking
    again = main.run_partywirks_sync(days_back=1, count_bookings_created_since="2026-11-15 00:00:00")
    assert again.get("new_counted", 0) == 0 and again.get("new_recorded_without_counting", 0) == 0


def test_partywirks_sync_dry_run_writes_nothing(db, fake, uid):
    fake.add_partywirks(uid, f"32{int(uid, 16)}", event_date="2099-02-01")
    before = len(fake.calls)
    out = main.run_partywirks_sync(days_back=1, apply=False)
    assert out["status"] == "partywirks_sync_dry_run" and out["new_counted"] >= 1
    assert all("/reservations" == url.split("/api/v1", 1)[1] for m, url, kw in fake.calls[before:])
    db.execute("SELECT 1 FROM master_customers WHERE email = %s", (f"party-{uid}@example.test",))
    assert db.fetchone() is None


# --- Review fixes ---------------------------------------------------------------------------

def test_simultaneous_webhooks_for_new_customer_make_one_record(db, fake, uid):
    email = f"newrace-{uid}@example.test"
    bodies = [payment(f"NR{i}{uid}", 1000, email=email) for i in range(5)]
    results = []

    def go(b):
        results.append(main.handle_square_event(json.loads(json.dumps(b)), BackgroundTasks()))

    threads = [threading.Thread(target=go, args=(b,)) for b in bodies]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len({r["cluster_id"] for r in results}) == 1, results
    c = customer_row(db, results[0]["cluster_id"])
    assert float(c["sq_spend"]) == 50.0 and c["sq_visits"] == 5


def test_stale_square_notice_does_not_undo_refund(client, db, fake, uid):
    email = f"stale-{uid}@example.test"
    first = payment(f"S{uid}", 8000, email=email)
    first["data"]["object"]["payment"]["updated_at"] = "2026-11-17T10:00:00Z"
    refund = payment(f"S{uid}", 8000, refund=3000, email=email)
    refund["data"]["object"]["payment"]["updated_at"] = "2026-11-17T11:00:00Z"
    r = square_post(client, first).json()
    square_post(client, refund)
    assert square_post(client, first).json()["status"] == "ignored_stale_notice"  # old snapshot arrives late
    assert float(customer_row(db, r["cluster_id"])["sq_spend"]) == 50.0


def test_failed_meta_event_is_resent_on_redelivery(client, db, fake, uid):
    email = f"resend-{uid}@example.test"
    fake.meta_mode = "500"
    r = square_post(client, payment(f"RS{uid}", 2000, email=email)).json()
    assert capi_rows(db, f"sq_pay_RS{uid}")[0]["sent_to_meta"] is False
    fake.meta_mode = "ok"
    square_post(client, payment(f"RS{uid}", 2000, email=email))   # Square re-delivers
    row = capi_rows(db, f"sq_pay_RS{uid}")[0]
    assert row["sent_to_meta"] is True
    assert float(customer_row(db, r["cluster_id"])["sq_spend"]) == 20.0   # spend not counted twice


def test_nightly_retry_resends_unsent_events(client, db, fake, uid):
    email = f"nightly-{uid}@example.test"
    fake.meta_mode = "timeout"
    square_post(client, payment(f"NT{uid}", 1500, email=email))
    fake.meta_mode = "ok"
    db.execute("SELECT current_score_threshold AS t FROM cdp_thermostat_config WHERE id = 1")
    before = db.fetchone()["t"]
    out = client.post("/cron/adjust-thermostat", headers={"X-CDP-Key": "test-cron-secret"}).json()
    db.execute("UPDATE cdp_thermostat_config SET current_score_threshold = %s WHERE id = 1", (before,))
    assert out["meta_retry"]["retried"] >= 1
    assert capi_rows(db, f"sq_pay_NT{uid}")[0]["sent_to_meta"] is True


def test_webres_without_id_keeps_different_people_apart(client, db, fake, uid):
    a = client.post("/webhooks/webres", headers=WH, json={"first_name": "Alice", "last_name": f"Smith{uid}",
                                                           "amount": 60, "date": "2026-11-22"}).json()
    b = client.post("/webhooks/webres", headers=WH, json={"first_name": "Bob", "last_name": f"Jones{uid}",
                                                           "amount": 60, "date": "2026-11-22"}).json()
    assert a["status"] == b["status"] == "webres_synced" and a["cluster_id"] != b["cluster_id"]


def test_older_partywirks_booking_does_not_replace_newer(client, db, fake, uid):
    newer = fake.add_partywirks(uid, f"40{int(uid, 16)}", event_date="2026-12-20", package="TEEN PARTY")
    r = client.post("/webhooks/partywirks", headers=WH, json=newer).json()
    older = fake.add_partywirks(uid, f"41{int(uid, 16)}", event_date="2025-06-01", package="BOWLING PARTY")
    client.post("/webhooks/partywirks", headers=WH, json=older)
    c = customer_row(db, r["cluster_id"])
    assert str(c["pw_last_party_date"]) == "2026-12-20" and c["pw_last_package"] == "TEEN PARTY"
    assert c["pw_bookings"] == 2


def test_catch_up_retries_when_customer_lookup_fails(db, fake, uid, monkeypatch):
    monkeypatch.setattr(main.time, "sleep", lambda s: None)
    fake.add_partywirks(uid, f"50{int(uid, 16)}", event_date="2099-03-01", created_at="2026-11-16 09:00:00")
    real_get = fake.get

    def customers_down(url, **kw):
        if "/customers/" in url:
            return __import__("conftest").FakeResponse(503, {"message": "down"})
        return real_get(url, **kw)

    monkeypatch.setattr(main.requests, "get", customers_down)
    first = main.run_partywirks_sync(days_back=1, count_bookings_created_since="2026-11-15 00:00:00")
    assert first.get("api_lookup_failed_retry_next_run") == 1
    monkeypatch.setattr(main.requests, "get", real_get)
    second = main.run_partywirks_sync(days_back=1, count_bookings_created_since="2026-11-15 00:00:00")
    assert second.get("new_counted") == 1
    db.execute("SELECT md_address1 FROM master_customers WHERE email = %s", (f"party-{uid}@example.test",))
    assert db.fetchone()["md_address1"]


def test_catch_up_pushes_recovered_customers_to_audiences(db, fake, uid, monkeypatch):
    monkeypatch.setattr(main.time, "sleep", lambda s: None)
    fake.add_partywirks(uid, f"60{int(uid, 16)}", event_date="2099-04-01", created_at="2026-11-16 09:00:00")
    main.run_partywirks_sync(days_back=1, count_bookings_created_since="2026-11-15 00:00:00")
    assert len(capi_rows(db, f"qb_pw_60{int(uid, 16)}")) == 1
    assert fake.count("/users") >= 1


# --- Partywirks webhook hardening -------------------------------------------------------------

PW_SAMPLE = {"event": "reservation.create", "vendor_id": 618, "data": {
    "id": "PW-202610-8472", "order_id": "PW-202610-8472", "status": "Confirmed", "event_date": "2026-10-15",
    "total_amount": 250, "activity": {"id": 14, "name": "Laser Tag Birthday Party"},
    "customer_responses": [{"question": "Celebrant Name?", "answer": "Alex Doe"}],
    "customer": {"name": "John Doe", "first_name": "John", "last_name": "Doe", "email": "john.doe@example.com",
                 "phone": "+1 (555) 234-5678", "address": "742 Evergreen Terrace", "city": "Springfield",
                 "state": "OR", "zip_code": "97477"}}}


def test_partywirks_sample_payload_is_never_saved(client, db, fake):
    for down in (False, True):
        fake.pw_down = down
        r = client.post("/webhooks/partywirks", headers=WH, json=PW_SAMPLE).json()
        assert r["status"].startswith("ignored"), r
    db.execute("SELECT 1 FROM customer_identities WHERE id_value = 'john.doe@example.com'")
    assert db.fetchone() is None


def test_partywirks_api_down_uses_flat_webhook_address(client, db, fake, uid):
    oid = f"70{int(uid, 16)}"
    body = json.loads(json.dumps(PW_SAMPLE))
    body["data"].update({"id": oid, "order_id": oid, "event_date": "2026-12-12"})
    body["data"]["customer"].update({"email": f"flat-{uid}@example.test", "phone": "", "name": f"Flat Parent{uid}",
                                     "first_name": "Flat", "last_name": f"Parent{uid}",
                                     "address": "300 Flat St", "city": "Novi", "state": "MI", "zip_code": "48375"})
    fake.pw_down = True
    r = client.post("/webhooks/partywirks", headers=WH, json=body).json()
    assert r["status"] == "partywirks_synced"
    c = customer_row(db, r["cluster_id"])
    assert c["pw_last_package"] == "Laser Tag Birthday Party" and str(c["pw_last_party_date"]) == "2026-12-12"
    melissa = [kw["params"] for m, url, kw in fake.calls if "melissadata" in url][-1]
    assert melissa["a1"] == "300 Flat St" and melissa["postal"] == "48375" and melissa["city"] == "Novi"
    assert "Alex Doe" not in json.dumps(c, default=str)


def _pw_signed(body: bytes, secret: str, style: str) -> str:
    import base64, hashlib, hmac
    d = hmac.new(secret.encode(), body, hashlib.sha256).digest()
    return {"hex": d.hex(), "HEX": d.hex().upper(), "prefixed": "sha256=" + d.hex(), "b64": base64.b64encode(d).decode()}[style]


def test_partywirks_signature_logging_then_enforced(client, db, fake, monkeypatch, caplog):
    raw = json.dumps(PW_SAMPLE).encode()
    headers = {**WH, "content-type": "application/json"}
    monkeypatch.setattr(main, "PARTYWIRKS_WEBHOOK_SECRET", "pw-sig-key")
    monkeypatch.setattr(main, "PARTYWIRKS_SIGNATURE_ENFORCE", False)
    with caplog.at_level("WARNING", logger="pg_cdp"):
        r = client.post("/webhooks/partywirks", content=raw, headers={**headers, "X-Partywirks-Signature": "bad"})
    assert r.status_code == 200 and "MISMATCH" in caplog.text          # logging only: not rejected
    monkeypatch.setattr(main, "PARTYWIRKS_SIGNATURE_ENFORCE", True)
    assert client.post("/webhooks/partywirks", content=raw, headers={**headers, "X-Partywirks-Signature": "bad"}).status_code == 401
    assert client.post("/webhooks/partywirks", content=raw, headers=headers).status_code == 401
    for style in ("hex", "HEX", "prefixed", "b64"):
        sig = _pw_signed(raw, "pw-sig-key", style)
        assert client.post("/webhooks/partywirks", content=raw,
                           headers={**headers, "X-Partywirks-Signature": sig}).status_code == 200, style


# --- Square line items & history backfill ------------------------------------------------------

def _order(oid, *items):
    return {"id": oid, "line_items": [
        {"catalog_object_id": vid, "name": name, "variation_name": "Regular", "quantity": str(q),
         "gross_sales_money": {"amount": cents}, "total_money": {"amount": cents}} for vid, name, q, cents in items]}


def test_live_payment_records_what_was_bought(client, db, fake, uid):
    fake.square_orders[f"O{uid}"] = _order(f"O{uid}", ("VAR_PIZZA", "Large Pizza", 1, 2400), ("VAR_SHOES", "Kids Shoes", 3, 1500))
    body = payment(f"LI{uid}", 3900, email=f"items-{uid}@example.test")
    body["data"]["object"]["payment"]["order_id"] = f"O{uid}"
    r = square_post(client, body).json()
    assert r["status"] == "payment_attributed"
    db.execute("SELECT line_items, variation_ids FROM unified_transactions WHERE transaction_id = %s", (f"sq_pay_LI{uid}",))
    row = db.fetchone()
    assert row["variation_ids"] == ["VAR_PIZZA", "VAR_SHOES"]
    assert row["line_items"][1] == {"variation_id": "VAR_SHOES", "item": "Kids Shoes", "variation": "Regular",
                                    "qty": 3.0, "gross": 15.0, "total": 15.0}


def test_order_lookup_failure_does_not_block_payment(client, db, fake, uid):
    fake.square_orders_down = True
    body = payment(f"OF{uid}", 1000, email=f"ordfail-{uid}@example.test", sq_id=f"SQOF{uid}")
    body["data"]["object"]["payment"]["order_id"] = f"OX{uid}"
    fake.square_customers[f"SQOF{uid}"] = {"id": f"SQOF{uid}", "email_address": f"ordfail-{uid}@example.test"}
    assert square_post(client, body).json()["status"] == "payment_attributed"
    db.execute("SELECT square_customer_id, line_items FROM unified_transactions WHERE transaction_id = %s", (f"sq_pay_OF{uid}",))
    row = db.fetchone()
    assert row["square_customer_id"] == f"SQOF{uid}" and row["line_items"] is None


def _history_payment(pid, cents, sq=None, created="2025-06-01T18:00:00Z", order=None):
    p = {"id": pid, "status": "COMPLETED", "total_money": {"amount": cents}, "created_at": created,
         "updated_at": created}
    if sq:
        p["customer_id"] = sq
    if order:
        p["order_id"] = order
    return p


def _load_backfill():
    import importlib.util, pathlib
    path = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "backfill_square_history.py"
    spec = importlib.util.spec_from_file_location("backfill_square_history", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_square_history_backfill_records_without_changing_spend(client, db, fake, uid, monkeypatch, capsys):
    bf = _load_backfill()
    monkeypatch.setattr(bf.time, "sleep", lambda s: None)
    sq = f"SQH{uid}"
    fake.square_customers[sq] = {"id": sq, "email_address": f"hist-{uid}@example.test"}
    live = square_post(client, payment(f"H0{uid}", 2000, sq_id=sq)).json()      # counted live: $20
    cid = live["cluster_id"]
    fake.square_orders[f"HO1{uid}"] = _order(f"HO1{uid}", ("VAR_LANE", "Lane Hour", 1, 4000))
    fake.square_payments = [
        _history_payment(f"H1{uid}", 4000, sq=sq, order=f"HO1{uid}"),            # older, from the import era
        _history_payment(f"H0{uid}", 2000, sq=sq),                                # already in the ledger
        _history_payment(f"H2{uid}", 1500, sq=f"UNKNOWN{uid}"),                   # Square customer we don't have
        _history_payment(f"H3{uid}", 900),                                        # anonymous
    ]
    bf.run("2025-01-01T00:00:00Z", apply=False)
    db.execute("SELECT 1 FROM unified_transactions WHERE transaction_id = %s", (f"sq_pay_H1{uid}",))
    assert db.fetchone() is None                                                  # dry run wrote nothing
    assert "Dry run" in capsys.readouterr().out

    bf.run("2025-01-01T00:00:00Z", apply=True)
    bf.run("2025-01-01T00:00:00Z", apply=True)                                     # idempotent
    c = customer_row(db, cid)
    assert float(c["sq_spend"]) == 20.0 and c["sq_visits"] == 1                   # spend untouched
    db.execute("SELECT transaction_id, cluster_id, square_customer_id, external_customer_id, total_amount, variation_ids "
               "FROM unified_transactions WHERE transaction_id = ANY(%s) ORDER BY transaction_id",
               ([f"sq_pay_H0{uid}", f"sq_pay_H1{uid}", f"sq_pay_H2{uid}", f"sq_pay_H3{uid}"],))
    rows = {r["transaction_id"]: r for r in db.fetchall()}
    assert rows[f"sq_pay_H1{uid}"]["cluster_id"] == cid and rows[f"sq_pay_H1{uid}"]["variation_ids"] == ["VAR_LANE"]
    assert rows[f"sq_pay_H2{uid}"]["cluster_id"] is None
    assert rows[f"sq_pay_H2{uid}"]["square_customer_id"] == f"UNKNOWN{uid}"
    assert rows[f"sq_pay_H2{uid}"]["external_customer_id"] is None                 # never auto-credited later
    assert rows[f"sq_pay_H3{uid}"]["cluster_id"] is None
    assert float(rows[f"sq_pay_H0{uid}"]["total_amount"]) == 20.0


def test_square_history_credit_window(client, db, fake, uid, monkeypatch):
    bf = _load_backfill()
    monkeypatch.setattr(bf.time, "sleep", lambda s: None)
    sq = f"SQW{uid}"
    fake.square_customers[sq] = {"id": sq, "email_address": f"window-{uid}@example.test"}
    cid = square_post(client, payment(f"W0{uid}", 1000, sq_id=sq)).json()["cluster_id"]
    fake.square_payments = [
        _history_payment(f"W1{uid}", 3000, sq=sq, created="2026-11-14T22:30:00Z"),         # in the gap: credit
        _history_payment(f"W2{uid}", 5000, sq=sq, created="2026-11-13T22:30:00Z"),         # before: import had it
        _history_payment(f"W3{uid}", 700, sq=f"NEW{uid}", created="2026-11-14T23:00:00Z"),  # gap, unknown: park
    ]
    window = (bf.parse_ts("2026-11-14T18:00:00Z"), bf.parse_ts("2026-11-15T02:12:00Z"))
    bf.run("2026-11-01", apply=True, credit_between=window)
    c = customer_row(db, cid)
    assert float(c["sq_spend"]) == 40.0 and c["sq_visits"] == 2
    db.execute("SELECT external_customer_id FROM unified_transactions WHERE transaction_id = %s", (f"sq_pay_W3{uid}",))
    assert db.fetchone()["external_customer_id"] == f"NEW{uid}"


def test_credit_window_survives_a_run_that_stops_part_way(client, db, fake, uid, monkeypatch):
    bf = _load_backfill()
    monkeypatch.setattr(bf.time, "sleep", lambda s: None)
    sq = f"SQX{uid}"
    fake.square_customers[sq] = {"id": sq, "email_address": f"crash-{uid}@example.test"}
    cid = square_post(client, payment(f"X0{uid}", 1000, sq_id=sq)).json()["cluster_id"]
    page1 = [_history_payment(f"X1{uid}", 3000, sq=sq, created="2026-11-14T22:30:00Z")]
    page2 = [_history_payment(f"X2{uid}", 2000, sq=sq, created="2026-11-14T23:30:00Z")]
    window = (bf.parse_ts("2026-11-14T18:00:00Z"), bf.parse_ts("2026-11-15T02:12:00Z"))

    def crashing_pages(since):
        yield page1
        raise RuntimeError("Square payments list unavailable")

    monkeypatch.setattr(bf, "iter_payment_pages", crashing_pages)
    try:
        bf.run("2026-11-01", apply=True, credit_between=window)
    except RuntimeError:
        pass
    c = customer_row(db, cid)
    assert float(c["sq_spend"]) == 40.0 and c["sq_visits"] == 2      # page 1 recorded AND credited together

    monkeypatch.setattr(bf, "iter_payment_pages", lambda since: iter([page1, page2]))
    bf.run("2026-11-01", apply=True, credit_between=window)          # re-run after the failure
    c = customer_row(db, cid)
    assert float(c["sq_spend"]) == 60.0 and c["sq_visits"] == 3      # page 2 credited, page 1 not twice


def _history_rows(db, sq, *rows):
    for pid, amount, when in rows:
        db.execute("INSERT INTO unified_transactions (transaction_id, source_system, total_amount, transaction_date, "
                   "square_customer_id) VALUES (%s, 'square', %s, %s, %s)", (f"sq_pay_{pid}", amount, when, sq))


def test_new_customer_gets_their_earlier_square_history(client, db, fake, uid):
    sq = f"SQN{uid}"
    _history_rows(db, sq, (f"N1{uid}", 30.0, "2025-02-01"), (f"N2{uid}", 12.5, "2025-08-15"))
    r = square_post(client, {"type": "customer.updated", "data": {"object": {"customer": {
        "id": sq, "given_name": "Back", "family_name": f"Again{uid}", "email_address": f"back-{uid}@example.test"}}}}).json()
    c = customer_row(db, r["cluster_id"])
    assert float(c["sq_spend"]) == 42.5 and c["sq_visits"] == 2
    assert str(c["sq_first_visit_date"]) == "2025-02-01"
    db.execute("SELECT count(*) AS n FROM unified_transactions WHERE square_customer_id = %s AND cluster_id = %s",
               (sq, r["cluster_id"]))
    assert db.fetchone()["n"] == 2


def test_returning_name_only_guest_payment_counts_once_plus_history(client, db, fake, uid):
    sq = f"SQR{uid}"
    _history_rows(db, sq, (f"R1{uid}", 40.0, "2025-03-01"))
    fake.square_customers[sq] = {"id": sq, "given_name": "Card", "family_name": f"Name{uid}"}
    r = square_post(client, payment(f"R2{uid}", 2500, sq_id=sq)).json()      # their next visit
    assert r["status"] == "payment_attributed"
    c = customer_row(db, r["cluster_id"])
    assert float(c["sq_spend"]) == 65.0 and c["sq_visits"] == 2               # $25 today + $40 history, once each


def test_existing_customer_gaining_a_square_id_gets_no_history_credit(client, db, fake, uid):
    email = f"merge-{uid}@example.test"
    first = square_post(client, payment(f"M1{uid}", 1000, email=email)).json()
    sq = f"SQM{uid}"
    _history_rows(db, sq, (f"M0{uid}", 99.0, "2025-01-01"))                  # may already be in their import total
    square_post(client, {"type": "customer.updated", "data": {"object": {"customer": {
        "id": sq, "given_name": "Merged", "family_name": f"Profile{uid}", "email_address": email}}}})
    c = customer_row(db, first["cluster_id"])
    assert float(c["sq_spend"]) == 10.0 and c["sq_visits"] == 1
    db.execute("SELECT cluster_id FROM unified_transactions WHERE transaction_id = %s", (f"sq_pay_M0{uid}",))
    assert db.fetchone()["cluster_id"] is None


def test_docs_shows_key_box_on_cron_endpoints(client):
    spec = client.get("/openapi.json").json()
    for path in ("/cron/adjust-thermostat", "/cron/sync-square-catalog", "/cron/enrich-partywirks",
                 "/cron/recover-square-avs-zips", "/cron/sync-partywirks", "/cron/versium-online-audience",
                 "/cron/melissa-geocode"):
        params = spec["paths"][path]["post"].get("parameters", [])
        assert any(p["name"] == "key" and p["in"] == "query" for p in params), path


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
                 "/cron/sync-square-catalog", "/cron/sync-partywirks", "/cron/versium-online-audience",
                 "/cron/melissa-geocode"):
        assert client.post(path).status_code == 401
        assert client.post(path, headers=WH).status_code == 401  # webhook key doesn't open cron endpoints
    assert fake.calls == []


def test_query_param_key_works_for_vendor_webhooks(client, fake):
    assert client.post("/webhooks/webres?key=test-webhook-secret", json={}).status_code == 200


# --- Versium online audience catch-up -------------------------------------------------------------

def test_online_audience_catch_up_buys_once_and_marks_no_match(client, db, monkeypatch, uid):
    from conftest import FakeResponse
    h = "e" * 64
    ids = {}
    for tag, state, score in (("hit", "MI", 10**6 + 2), ("miss", None, 10**6 + 1), ("ohio", "OH", 10**6 + 3)):
        db.execute("INSERT INTO master_customers (cluster_id, email, md_state, bowler_value_score) "
                   "VALUES (nextval('master_cluster_id_seq'), %s, %s, %s) RETURNING cluster_id",
                   (f"{tag}-{uid}@example.test", state, score))
        ids[tag] = db.fetchone()["cluster_id"]
    asked = []

    def get(url, **kw):
        asked.append(kw["params"].get("email"))
        hit = kw["params"].get("email", "").startswith("hit-")
        return FakeResponse(200, {"versium": {"results": [{"facebook": [{"email": [h]}]}] if hit else []}})
    monkeypatch.setattr(main, "VERSIUM_API_KEY", "test-versium-key")
    monkeypatch.setattr(main.requests, "get", get)
    try:
        out = client.post("/cron/versium-online-audience?limit=2", headers={"X-CDP-Key": "test-cron-secret"}).json()
        assert (out["matched"], out["no_match"], out["failed_retry_later"]) == (1, 1, 0)
        assert not any(e and e.startswith("ohio-") for e in asked)          # out of state: never bought
        db.execute("SELECT cluster_id, versium_online_audience v FROM master_customers WHERE cluster_id = ANY(%s)",
                   (list(ids.values()),))
        v = {r["cluster_id"]: r["v"] for r in db.fetchall()}
        assert v[ids["hit"]]["facebook"]["email"] == [h] and v[ids["hit"]]["_checked"]
        assert set(v[ids["miss"]]) == {"_checked"} and v[ids["ohio"]] == {}
        assert main.build_meta_user_identifiers({"cluster_id": 1, "versium_online_audience": v[ids["hit"]]})["em"] == [h]
        asked.clear()
        client.post("/cron/versium-online-audience?limit=2", headers={"X-CDP-Key": "test-cron-secret"})
        assert not any(e and (e.startswith("hit-") or e.startswith("miss-")) for e in asked)  # not bought twice
    finally:
        db.execute("DELETE FROM master_customers WHERE cluster_id = ANY(%s)", (list(ids.values()),))


def test_online_audience_catch_up_respects_claims(client, db, monkeypatch, uid):
    from conftest import FakeResponse
    ids = {}
    for tag, stored, score in (("fresh", {"_pending": 9e12}, 10**6 + 2), ("stale", {"_pending": 1}, 10**6 + 1)):
        db.execute("INSERT INTO master_customers (cluster_id, email, md_state, bowler_value_score, versium_online_audience) "
                   "VALUES (nextval('master_cluster_id_seq'), %s, 'MI', %s, %s::jsonb) RETURNING cluster_id",
                   (f"{tag}-{uid}@example.test", score, json.dumps(stored)))
        ids[tag] = db.fetchone()["cluster_id"]
    asked = []

    def get(url, **kw):
        asked.append(kw["params"].get("email"))
        return FakeResponse(200, {"versium": {"results": []}})
    monkeypatch.setattr(main, "VERSIUM_API_KEY", "test-versium-key")
    monkeypatch.setattr(main.requests, "get", get)
    try:
        client.post("/cron/versium-online-audience?limit=1", headers={"X-CDP-Key": "test-cron-secret"})
        assert f"stale-{uid}@example.test" in asked and f"fresh-{uid}@example.test" not in asked
        db.execute("SELECT versium_online_audience v FROM master_customers WHERE cluster_id = %s", (ids["stale"],))
        assert set(db.fetchone()["v"]) == {"_checked"}                      # claim released, result stored
    finally:
        db.execute("DELETE FROM master_customers WHERE cluster_id = ANY(%s)", (list(ids.values()),))


def test_paid_online_audience_kept_when_melissa_fails(client, db, fake, monkeypatch, uid):
    from conftest import FakeResponse
    h = "f" * 64
    db.execute("INSERT INTO master_customers (cluster_id, email, md_state, in_partywirks) "
               "VALUES (nextval('master_cluster_id_seq'), %s, 'MI', 1) RETURNING cluster_id",
               (f"timeout-{uid}@example.test",))
    cid = db.fetchone()["cluster_id"]
    real_get = fake.get

    def get(url, **kw):
        if "api.versium.com" in url:
            if "onlineAudience" in url:
                return FakeResponse(200, {"versium": {"results": [{"facebook": [{"email": [h]}]}]}})
            return FakeResponse(200, {"versium": {"results": [{}]}})
        return real_get(url, **kw)
    monkeypatch.setattr(main, "VERSIUM_API_KEY", "test-versium-key")
    monkeypatch.setattr(main.requests, "get", get)
    try:
        client.post("/cron/enrich-partywirks", headers={"X-CDP-Key": "test-cron-secret"})
        db.execute("SELECT md_results, versium_online_audience v FROM master_customers WHERE cluster_id = %s", (cid,))
        row = db.fetchone()
        assert row["md_results"] is None and row["v"]["facebook"]["email"] == [h]
    finally:
        db.execute("DELETE FROM master_customers WHERE cluster_id = %s", (cid,))


# --- Melissa geocode catch-up -----------------------------------------------------------------------

def test_melissa_geocode_fills_distance_and_census_once(client, db, fake, uid):
    ids = {}
    for tag, email, score in (("ok", f"geo-{uid}@example.test", 10**6 + 2),
                              ("det", f"detroit-{uid}@example.test", 10**6 + 1)):
        db.execute("INSERT INTO master_customers (cluster_id, email, md_address1, md_city, md_state, zip5, "
                   "bowler_value_score, md_results) VALUES (nextval('master_cluster_id_seq'), %s, '1 Versium Rd', "
                   "'Farmingtn Hls', 'MI', '48335', %s, 'ES01,VS40') RETURNING cluster_id", (email, score))
        ids[tag] = db.fetchone()["cluster_id"]
    try:
        out = client.post("/cron/melissa-geocode?limit=2", headers={"X-CDP-Key": "test-cron-secret"}).json()
        assert (out["geocoded"], out["now_in_excluded_area"], out["failed_retry_later"]) == (2, 1, 0)
        db.execute("SELECT * FROM master_customers WHERE cluster_id = ANY(%s)", (list(ids.values()),))
        rows = {r["cluster_id"]: r for r in db.fetchall()}
        ok, det = rows[ids["ok"]], rows[ids["det"]]
        assert ok["md_city"] == "Farmington Hills" and float(ok["distance_miles"]) < 2
        assert ok["md_census_tract"] == "152300" and ok["md_unified_school_district_name"] == "Farmington Public School District"
        assert ok["md_results"].endswith(",GEO_TRIED") and "VS40" not in ok["md_results"]
        assert det["zip5"] == "48226" and det["bowler_value_score"] == 25      # verified into an excluded area
        before = fake.count("personator.melissadata.net")
        client.post("/cron/melissa-geocode?limit=2", headers={"X-CDP-Key": "test-cron-secret"})
        assert fake.count("personator.melissadata.net") - before <= 2            # ours are never re-sent
        db.execute("SELECT count(*) n FROM master_customers WHERE cluster_id = ANY(%s) AND md_results LIKE '%%GEO_TRIED'",
                   (list(ids.values()),))
        assert db.fetchone()["n"] == 2
    finally:
        db.execute("DELETE FROM customer_identities WHERE cluster_id = ANY(%s)", (list(ids.values()),))
        db.execute("DELETE FROM master_customers WHERE cluster_id = ANY(%s)", (list(ids.values()),))


def test_melissa_geocode_skips_a_row_another_run_holds(client, db, fake, uid):
    import os
    import psycopg2
    db.execute("INSERT INTO master_customers (cluster_id, email, md_address1, zip5, bowler_value_score) "
               "VALUES (nextval('master_cluster_id_seq'), %s, '1 Versium Rd', '48335', %s) RETURNING cluster_id",
               (f"locked-{uid}@example.test", 10**6 + 5))
    cid = db.fetchone()["cluster_id"]
    other = psycopg2.connect(os.environ["TEST_DATABASE_URL"])
    try:
        oc = other.cursor()
        oc.execute("SELECT 1 FROM master_customers WHERE cluster_id = %s FOR UPDATE", (cid,))   # a run mid-lookup
        before = fake.count("personator.melissadata.net")
        out = client.post("/cron/melissa-geocode?limit=1", headers={"X-CDP-Key": "test-cron-secret"}).json()
        assert out["skipped_already_handled"] == 1 and fake.count("personator.melissadata.net") == before
    finally:
        other.rollback(); other.close()
        db.execute("DELETE FROM master_customers WHERE cluster_id = %s", (cid,))
