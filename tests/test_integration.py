"""End-to-end webhook tests against a disposable database copy (see conftest.py)."""
import csv
import io
import json
import threading

import pytest

from fastapi import BackgroundTasks

import main

WH = {"X-CDP-Key": "test-webhook-secret"}


@pytest.fixture(autouse=True)
def melissa_append_enabled(monkeypatch):
    """These flows exercise Melissa finding an address from an email/phone (the Append action), which needs a
    subscription license (set MELISSA_ACT). The production default is the credit-license request (Check only),
    covered by test_credit_license_skips_melissa_without_a_street."""
    monkeypatch.setattr(main, "MELISSA_ACT", "Check,Verify,Append")


@pytest.fixture(autouse=True)
def fixed_threshold(monkeypatch):
    """The test database is a copy of production, whose hourly thermostat moves the score threshold (45-90).
    Pin it so these flows don't pass or fail with the live value."""
    monkeypatch.setattr(main, "get_threshold", lambda cur: 55)


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
                 "/cron/melissa-geocode", "/cron/census-geocode"):
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
                 "/cron/melissa-geocode", "/cron/census-geocode"):
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


# --- Census geocode catch-up ------------------------------------------------------------------------

def test_census_geocode_fills_census_fields_once(client, db, fake, uid):
    ids = {}
    for tag, street, lat, score in (("new", "1 Versium Rd", None, 10**6 + 3),
                                    ("hasgeo", "2 Melissa Ln", 42.5, 10**6 + 2),
                                    ("miss", "3 Nomatch Ct", None, 10**6 + 1)):
        db.execute("INSERT INTO master_customers (cluster_id, email, md_address1, md_city, md_state, zip5, "
                   "md_latitude, distance_miles, bowler_value_score) VALUES (nextval('master_cluster_id_seq'), %s, %s, "
                   "'Farmington Hills', 'MI', '48335', %s, %s, %s) RETURNING cluster_id",
                   (f"census-{tag}-{uid}@example.test", street, lat, 9.9 if lat else None, score))
        ids[tag] = db.fetchone()["cluster_id"]
    try:
        out = client.post("/cron/census-geocode?limit=3", headers={"X-CDP-Key": "test-cron-secret"}).json()
        assert (out["matched"], out["coordinates_added"], out["no_match"], out["failed_retry_later"]) == (2, 1, 1, 0)
        db.execute("SELECT * FROM master_customers WHERE cluster_id = ANY(%s)", (list(ids.values()),))
        rows = {r["cluster_id"]: r for r in db.fetchall()}
        new, hasgeo, miss = rows[ids["new"]], rows[ids["hasgeo"]], rows[ids["miss"]]
        assert new["md_census_tract"] == "167000" and float(new["distance_miles"]) < 1 and new["census_checked_at"]
        assert hasgeo["md_census_tract"] == "167000" and float(hasgeo["distance_miles"]) == 9.9   # Melissa's coords kept
        assert miss["md_census_tract"] is None and miss["census_checked_at"]                    # asked once
        before = fake.count("geocoding.geo.census.gov")
        client.post("/cron/census-geocode?limit=3", headers={"X-CDP-Key": "test-cron-secret"})
        db.execute("SELECT count(*) n FROM master_customers WHERE cluster_id = ANY(%s) AND census_checked_at IS NOT NULL",
                   (list(ids.values()),))
        assert db.fetchone()["n"] == 3
    finally:
        db.execute("DELETE FROM master_customers WHERE cluster_id = ANY(%s)", (list(ids.values()),))


def test_credit_license_skips_melissa_without_a_street(client, db, fake, monkeypatch, uid):
    monkeypatch.setattr(main, "MELISSA_ACT", "Check")
    email = f"creditonly-{uid}@example.test"
    before = fake.count("personator.melissadata.net")
    r = client.post("/webhooks/webres", headers=WH, json={"reservation_id": f"CR{uid}", "first_name": "Cee",
                                                          "last_name": "Dee", "email": email, "amount": 30})
    assert r.status_code == 200 and r.json()["status"] == "webres_synced"
    assert fake.count("personator.melissadata.net") == before            # no street: no credit spent
    db.execute("SELECT cluster_id, md_results FROM master_customers WHERE email = %s", (email,))
    row = db.fetchone()
    try:
        assert row["md_results"] == main.MELISSA_NO_STREET                 # answered: no retry until a street arrives
        client.post("/webhooks/webres", headers=WH, json={"reservation_id": f"CS{uid}", "first_name": "Cee",
                                                          "last_name": "Dee", "email": email, "amount": 30})
        assert fake.count("personator.melissadata.net") == before
    finally:
        db.execute("DELETE FROM unified_transactions WHERE cluster_id = %s", (row["cluster_id"],))
        db.execute("DELETE FROM customer_identities WHERE cluster_id = %s", (row["cluster_id"],))
        db.execute("DELETE FROM master_customers WHERE cluster_id = %s", (row["cluster_id"],))


def ts_booking(uid, bid, eid, status="DEFINITE", created="2026-11-01T10:00:00Z", payments=None):
    return {
        "id": bid, "name": "Holiday Party", "status": status, "start_date": "2026-12-12",
        "created_at": created, "updated_at": "2026-11-10T10:00:00Z",
        "contact": {"first_name": "Corp", "last_name": f"Booker{uid}",
                    "email_addresses": [{"address": f"tsbook-{uid}@example.test"}]},
        "account": {"name": "Booking Co"},
        "events": [{"id": eid, "status": status, "event_start_iso8601": "2026-12-12T18:00:00-05:00",
                    "guest_count": 60, "grand_total": "2,400.00", "amount_due": "0.00"}],
        "payment_sets": payments if payments is not None else [{"amount_due": 0, "payments": [
            {"id": 1, "amount": 2000, "state": "paid", "payment_method": "Credit Card", "credit_card_type": "Visa"},
            {"id": 2, "amount": 400, "state": "paid", "payment_method": "Check"}]}],
    }


def test_tripleseat_sync_booking_counts_once_and_keeps_money_out_of_spend(db, fake, uid):
    bid, eid = int(uid[:8], 16), int(uid[4:12], 16)
    with main.get_conn() as conn, conn.cursor() as cur:
        res = main.ingest_tripleseat_booking(cur, ts_booking(uid, bid, eid), count_it=True)
    assert res["status"] == "created"
    db.execute("SELECT * FROM tripleseat_bookings WHERE booking_id = %s", (bid,))
    b = db.fetchone()
    assert float(b["paid_total"]) == 2400.0 and float(b["paid_not_card"]) == 400.0
    db.execute("SELECT * FROM tripleseat_events WHERE event_id = %s", (eid,))
    e = db.fetchone()
    assert float(e["grand_total"]) == 2400.0 and e["cluster_id"] == b["cluster_id"] and e["guest_count"] == 60
    c = customer_row(db, b["cluster_id"])
    assert c["in_tripleseat"] == 1 and c["ts_events"] == 1 and c["ts_account"] == "Booking Co"
    assert c["ts_max_guests"] == 60 and str(c["ts_last_event_date"])[:10] == "2026-12-12"
    assert float(c["true_lifetime_spend"] or 0) == 0.0
    # Re-reading the same booking (nightly overlap) changes nothing and never fires Meta.
    with main.get_conn() as conn, conn.cursor() as cur:
        assert main.ingest_tripleseat_booking(cur, ts_booking(uid, bid, eid), count_it=True)["status"] == "matched"
    assert customer_row(db, b["cluster_id"])["ts_events"] == 1
    assert fake.meta_events() == []


def test_tripleseat_sync_skips_count_when_webhook_saw_the_event(client, db, fake, uid):
    bid, eid = int(uid[:8], 16) + 1, int(uid[4:12], 16) + 1
    hook = {"event": {"id": eid, "first_name": "Corp", "last_name": f"Booker{uid}", "status": "definite",
                      "email_address": f"tsbook-{uid}@example.test", "event_date": "2026-12-12"}}
    cid = client.post("/webhooks/tripleseat", headers=WH, json=hook).json()["cluster_id"]
    assert customer_row(db, cid)["ts_events"] == 1
    with main.get_conn() as conn, conn.cursor() as cur:
        main.ingest_tripleseat_booking(cur, ts_booking(uid, bid, eid), count_it=True)
    assert customer_row(db, cid)["ts_events"] == 1


def test_tripleseat_history_is_recorded_without_counting(db, fake, uid):
    bid, eid = int(uid[:8], 16) + 2, int(uid[4:12], 16) + 2
    with main.get_conn() as conn, conn.cursor() as cur:
        main.ingest_tripleseat_booking(cur, ts_booking(uid, bid, eid, created="2021-03-01T00:00:00Z",
                                                       payments=[]), count_it=False)
    db.execute("SELECT cluster_id, payments, paid_total FROM tripleseat_bookings WHERE booking_id = %s", (bid,))
    b = db.fetchone()
    assert b["payments"] == [] and float(b["paid_total"]) == 0.0
    assert (customer_row(db, b["cluster_id"])["ts_events"] or 0) == 0


def test_tripleseat_lead_stored_with_campaign_and_dry_run_writes_nothing(db, fake, uid):
    lid = int(uid[:8], 16) + 3
    lead = {"id": lid, "first_name": "Lead", "last_name": f"Person{uid}", "email_address": f"tslead-{uid}@example.test",
            "company": "Lead Co", "guest_count": 25, "event_date": "12/20/2026", "campaign_source": "google",
            "campaign_medium": "cpc", "selected_lead_sources": [{"lead_source": {"name": "Website"}}],
            "created_at": "2026-11-15T12:00:00Z"}
    with main.get_conn() as conn, conn.cursor() as cur:
        assert main.ingest_tripleseat_lead(cur, lead, apply=False)["status"] == "would_created"
    db.execute("SELECT 1 FROM tripleseat_leads WHERE lead_id = %s", (lid,))
    assert db.fetchone() is None
    with main.get_conn() as conn, conn.cursor() as cur:
        assert main.ingest_tripleseat_lead(cur, lead)["status"] == "created"
    db.execute("SELECT * FROM tripleseat_leads WHERE lead_id = %s", (lid,))
    row = db.fetchone()
    assert row["campaign"] == {"source": "google", "medium": "cpc"} and row["lead_sources"] == ["Website"]
    assert str(row["event_date"]) == "2026-12-20"
    c = customer_row(db, row["cluster_id"])
    assert c["ts_lead_status"] == "PROSPECT" and (c["ts_events"] or 0) == 0


def test_tripleseat_sync_keeps_latest_event_date(db, fake, uid):
    bid, eid = int(uid[:8], 16) + 4, int(uid[4:12], 16) + 4
    newer = ts_booking(uid, bid, eid)  # 2026-12-12
    older = ts_booking(uid, bid + 1, eid + 1)
    older["events"][0]["event_start_iso8601"] = "2024-05-01T18:00:00-04:00"
    with main.get_conn() as conn, conn.cursor() as cur:
        main.ingest_tripleseat_booking(cur, newer, count_it=True)
        main.ingest_tripleseat_booking(cur, older, count_it=True)
    db.execute("SELECT cluster_id FROM tripleseat_bookings WHERE booking_id = %s", (bid,))
    c = customer_row(db, db.fetchone()["cluster_id"])
    assert c["ts_last_event_date"] == "2026-12-12" and c["ts_events"] == 2


def test_tripleseat_new_event_on_synced_booking_not_counted_again(client, db, fake, uid):
    bid, eid = int(uid[:8], 16) + 6, int(uid[4:12], 16) + 6
    with main.get_conn() as conn, conn.cursor() as cur:
        main.ingest_tripleseat_booking(cur, ts_booking(uid, bid, eid), count_it=True)
    hook = {"event": {"id": eid + 100, "booking_id": bid, "first_name": "Corp", "last_name": f"Booker{uid}",
                      "status": "definite", "email_address": f"tsbook-{uid}@example.test", "event_date": "2026-12-13"}}
    cid = client.post("/webhooks/tripleseat", headers=WH, json=hook).json()["cluster_id"]
    assert customer_row(db, cid)["ts_events"] == 1


CRON = {"X-CDP-Key": "test-cron-secret"}


def _new_customer(db, uid, email, flags=None, address=None):
    db.execute("SELECT nextval('master_cluster_id_seq') AS id")
    cid = db.fetchone()["id"]
    db.execute("""INSERT INTO master_customers (cluster_id, first_name, last_name, email, md_address1, md_city,
                  md_state, zip5, versium_lifestyle_flags, bowler_value_score, updated_at)
                  VALUES (%s, 'Vee', %s, %s, %s, %s, 'MI', %s, %s::jsonb, 100000, NOW())""",
               (cid, f"Catch{uid}", email, address, "Novi" if address else None, "48375" if address else None,
                json.dumps(flags) if flags is not None else None))
    return cid


def test_versium_catch_up_enriches_unchecked_and_marks_them(client, db, fake, uid, monkeypatch):
    monkeypatch.setattr(main, "VERSIUM_API_KEY", "test-versium-key")
    fresh = _new_customer(db, uid, f"v-{uid}@example.test")
    done = _new_customer(db, uid, f"vcsv-{uid}@example.test", flags={"_source": "versium_reach_csv", "pets": "Y"})
    down = _new_customer(db, uid, f"vdown-{uid}@example.test")
    # The test database keeps earlier runs' customers; rank this run's first.
    db.execute("UPDATE master_customers SET bowler_value_score = 0 WHERE bowler_value_score >= 100000 "
               "AND cluster_id NOT IN (%s, %s, %s)", (fresh, done, down))
    r = client.post("/cron/versium-enrich?limit=500", headers=CRON).json()
    assert r["status"] == "versium_enrich"
    c = customer_row(db, fresh)
    assert c["md_address1"] == "500 Versium Way" and c["zip5"] == "48375" and c["md_census_tract"]
    assert c["distance_miles"] is not None and c["versium_net_worth"] == "$100,000 - $249,999"
    f = c["versium_lifestyle_flags"]
    assert f["presence_of_children"] == "Yes" and f["_versium_checked"] and "religion" not in f
    assert "_versium_pending" not in f and c["versium_alt_phones"]
    assert customer_row(db, done)["versium_lifestyle_flags"] == {"_source": "versium_reach_csv", "pets": "Y"}
    dflags = customer_row(db, down)["versium_lifestyle_flags"] or {}
    assert "_versium_checked" not in dflags and "_versium_pending" not in dflags  # retried next run
    # A second run never pays for the finished customer again.
    mine = lambda: sum(1 for m, u, kw in fake.calls
                       if "versium" in u and (kw.get("params") or {}).get("email") == f"v-{uid}@example.test")
    before = mine()
    client.post("/cron/versium-enrich?limit=500", headers=CRON)
    assert before > 0 and mine() == before


def test_webhook_versium_merges_with_csv_flags_and_melissa_off_marks_done(client, db, fake, uid, monkeypatch):
    monkeypatch.setattr(main, "VERSIUM_API_KEY", "test-versium-key")
    monkeypatch.setattr(main, "MELISSA_LIVE", False)
    email = f"vweb-{uid}@example.test"
    r = client.post("/webhooks/webres", headers=WH, json={"reservation_id": f"VW{uid}", "first_name": "Web",
                                                           "last_name": f"Vee{uid}", "email": email, "amount": 30}).json()
    c = customer_row(db, r["cluster_id"])
    assert c["md_results"] == main.MELISSA_OFF and c["md_address1"] == "500 Versium Way"
    assert c["versium_lifestyle_flags"]["presence_of_children"] == "Yes"
    assert fake.count("melissadata") == 0
    db.execute("UPDATE master_customers SET versium_lifestyle_flags = versium_lifestyle_flags || "
               "'{\"pets\": \"Y\"}'::jsonb WHERE cluster_id = %s", (r["cluster_id"],))
    n = sum(1 for m, u, kw in fake.calls if "versium" in u)
    client.post("/webhooks/webres", headers=WH, json={"reservation_id": f"VW2{uid}", "first_name": "Web",
                                                      "last_name": f"Vee{uid}", "email": email, "amount": 30})
    assert sum(1 for m, u, kw in fake.calls if "versium" in u) == n  # finished: not looked up again
    assert customer_row(db, r["cluster_id"])["versium_lifestyle_flags"]["pets"] == "Y"


# --- Deferred enrichment ---------------------------------------------------------------------

@pytest.fixture
def deferred(monkeypatch):
    monkeypatch.setattr(main, "ENRICH_DELAY_MINUTES", 15)
    monkeypatch.setattr(main, "MELISSA_LIVE", False)
    monkeypatch.setattr(main, "VERSIUM_API_KEY", "test-versium-key")


def run_worker_now(db, cluster_id):
    db.execute("UPDATE master_customers SET enrich_after = NOW() - INTERVAL '1 second' "
               "WHERE cluster_id = %s AND enrich_after IS NOT NULL", (cluster_id,))
    return main.process_pending_enrichment(limit=500)


def versium_emails(fake):
    return [(kw.get("params") or {}).get("email") for m, u, kw in fake.calls if "api.versium.com" in u]


def test_square_then_webres_enriches_once_on_merged_details(client, db, fake, uid, deferred):
    email = f"sqwr-{uid}@example.test"
    phone = "248" + str(int(uid[:6], 16) % 10_000_000).zfill(7)
    r = square_post(client, payment(f"DP{uid}", 6800, email=email, name=f"DEFER/SAM{uid}", zip5="48375",
                                    etype="payment.created")).json()
    cid = r["cluster_id"]
    c = customer_row(db, cid)
    assert c["enrich_after"] is not None and fake.meta_events() == [] and versium_emails(fake) == []
    assert capi_rows(db, f"sq_pay_DP{uid}")[0]["meta_response_status"] == "DEFERRED_ENRICHMENT"
    # Five minutes later the WebRes booking arrives with a phone; it merges and the window doesn't restart.
    w = client.post("/webhooks/webres", headers=WH, json={
        "reservation_id": f"DW{uid}", "first_name": "Sam", "last_name": f"Defer{uid}", "email": email,
        "phone": phone, "amount": 68}).json()
    assert w["cluster_id"] == cid
    c2 = customer_row(db, cid)
    assert c2["enrich_after"] == c["enrich_after"] and c2["enrich_inputs"]["phone"] == phone
    assert fake.meta_events() == []
    out = run_worker_now(db, cid)
    assert out["enriched"] >= 1
    c3 = customer_row(db, cid)
    assert c3["enrich_after"] is None and c3["md_address1"] == "500 Versium Way" and c3["md_census_tract"]
    assert c3["versium_lifestyle_flags"]["_versium_checked"] and c3["md_results"] == main.MELISSA_OFF
    contact = [kw["params"] for m, u, kw in fake.calls if "/v2/contact" in u and kw["params"].get("email") == email]
    assert len(contact) == 1 and contact[0]["phone"] == phone   # one lookup, with the WebRes phone
    sent = {e["event_id"] for e in fake.meta_events()}
    assert f"sq_pay_DP{uid}" in sent and f"wr_DW{uid}" in sent
    assert capi_rows(db, f"sq_pay_DP{uid}")[0]["sent_to_meta"] is True


def test_deferred_events_dropped_when_versium_address_is_excluded(client, db, fake, uid, deferred, monkeypatch):
    orig = fake._versium

    def detroit(url, params):
        r = orig(url, params)
        if "/v2/contact" in url and r.status_code == 200:
            body = r.json()
            body["versium"]["results"][0].update({"City": "Detroit", "Zip": "48226", "Zip+4": ""})
        return r
    monkeypatch.setattr(fake, "_versium", detroit)
    email = f"sqdet-{uid}@example.test"
    # No card ZIP, so nothing contradicts the address Versium finds.
    cid = square_post(client, payment(f"DD{uid}", 4000, email=email, zip5=None,
                                      etype="payment.created")).json()["cluster_id"]
    run_worker_now(db, cid)
    row = capi_rows(db, f"sq_pay_DD{uid}")[0]
    assert row["sent_to_meta"] is False and row["meta_response_status"] == "DROPPED_AFTER_ENRICHMENT"
    assert row["meta_response_body"] == "excluded_area"
    assert not any("/v2/demographic" in u for m, u, kw in fake.calls if (kw.get("params") or {}).get("email") == email)
    assert main.retry_unsent_events(db)["still_unsent"] >= 0
    assert capi_rows(db, f"sq_pay_DD{uid}")[0]["meta_response_status"] == "DROPPED_AFTER_ENRICHMENT"


def test_versium_failure_retries_later_then_releases(client, db, fake, uid, deferred):
    email = f"vdown-{uid}@example.test"
    cid = client.post("/webhooks/webres", headers=WH, json={
        "reservation_id": f"VD{uid}", "first_name": "Down", "last_name": f"V{uid}", "email": email,
        "amount": 40}).json()["cluster_id"]
    assert run_worker_now(db, cid).get("retry_later") == 1
    c = customer_row(db, cid)
    assert c["enrich_after"] is not None and c["enrich_attempts"] == 1
    assert capi_rows(db, f"wr_VD{uid}")[0]["meta_response_status"] == "DEFERRED_ENRICHMENT"
    db.execute("UPDATE master_customers SET enrich_attempts = %s WHERE cluster_id = %s",
               (main.ENRICH_MAX_ATTEMPTS, cid))
    run_worker_now(db, cid)  # gives up on Versium, but the purchase still reaches Meta
    assert customer_row(db, cid)["enrich_after"] is None
    assert capi_rows(db, f"wr_VD{uid}")[0]["sent_to_meta"] is True


def test_unmatched_customer_retried_once_with_new_details(client, db, fake, uid, deferred):
    first = f"vnomatch-{uid}@example.test"
    cid = client.post("/webhooks/webres", headers=WH, json={
        "reservation_id": f"NM{uid}", "first_name": "No", "last_name": f"Match{uid}", "email": first,
        "amount": 30}).json()["cluster_id"]
    run_worker_now(db, cid)
    assert customer_row(db, cid)["versium_lifestyle_flags"].get("_versium_checked")
    assert customer_row(db, cid)["md_results"] == main.MELISSA_OFF
    # Weeks later a booking with the same name and a different email that Versium does know.
    second = f"newmail-{uid}@example.test"
    client.post("/webhooks/webres", headers=WH, json={
        "reservation_id": f"NM2{uid}", "first_name": "No", "last_name": f"Match{uid}", "email": second,
        "phone": "248" + str(int(uid[:6], 16) % 10_000_000).zfill(7), "amount": 30})
    c = customer_row(db, cid)
    assert c["enrich_after"] is not None and c["enrich_inputs"].get("_retry") is True
    run_worker_now(db, cid)
    c = customer_row(db, cid)
    assert second in versium_emails(fake) and c["versium_lifestyle_flags"]["_versium_retried"]
    assert c["versium_lifestyle_flags"]["presence_of_children"] == "Yes"
    # Matched now, so a third new detail never triggers another paid lookup.
    n = len(versium_emails(fake))
    client.post("/webhooks/webres", headers=WH, json={
        "reservation_id": f"NM3{uid}", "first_name": "No", "last_name": f"Match{uid}",
        "email": f"third-{uid}@example.test", "amount": 30})
    assert customer_row(db, cid)["enrich_after"] is None and len(versium_emails(fake)) == n


def test_catch_up_leaves_waiting_customers_to_the_worker(client, db, fake, uid, deferred):
    email = f"wait-{uid}@example.test"
    cid = client.post("/webhooks/webres", headers=WH, json={
        "reservation_id": f"WT{uid}", "first_name": "Wait", "last_name": f"Er{uid}", "email": email,
        "amount": 20}).json()["cluster_id"]
    db.execute("UPDATE master_customers SET bowler_value_score = 100000 WHERE cluster_id = %s", (cid,))
    client.post("/cron/versium-enrich?limit=500", headers=CRON)
    assert email not in versium_emails(fake) and customer_row(db, cid)["enrich_after"] is not None


def test_details_arriving_during_lookup_get_a_second_look(client, db, fake, uid, deferred, monkeypatch):
    email = f"vnomatch-race-{uid}@example.test"
    cid = client.post("/webhooks/webres", headers=WH, json={
        "reservation_id": f"RC{uid}", "first_name": "Race", "last_name": f"Y{uid}", "email": email,
        "amount": 20}).json()["cluster_id"]
    real = main.run_melissa_and_versium_waterfall
    late = f"late-{uid}@example.test"

    def waterfall_with_webhook_meanwhile(*a, **kw):
        out = real(*a, **kw)
        client.post("/webhooks/webres", headers=WH, json={
            "reservation_id": f"RC2{uid}", "first_name": "Race", "last_name": f"Y{uid}", "email": email,
            "phone": "248" + str(int(uid[6:12], 16) % 10_000_000).zfill(7), "amount": 20})
        db.execute("UPDATE master_customers SET enrich_inputs = enrich_inputs || %s::jsonb WHERE cluster_id = %s",
                   (json.dumps({"email": late}), cid))
        return out
    monkeypatch.setattr(main, "run_melissa_and_versium_waterfall", waterfall_with_webhook_meanwhile)
    assert run_worker_now(db, cid).get("rerun_new_details") == 1
    c = customer_row(db, cid)
    assert c["enrich_after"] is not None and c["enrich_inputs"].get("_retry") is True
    assert capi_rows(db, f"wr_RC{uid}")[0]["meta_response_status"] == "DEFERRED_ENRICHMENT"
    monkeypatch.setattr(main, "run_melissa_and_versium_waterfall", real)
    run_worker_now(db, cid)
    assert late in versium_emails(fake) and customer_row(db, cid)["enrich_after"] is None


def test_released_events_log_the_final_score_and_threshold(client, db, fake, uid, deferred):
    cid = client.post("/webhooks/webres", headers=WH, json={
        "reservation_id": f"SC{uid}", "first_name": "Score", "last_name": f"Z{uid}",
        "email": f"score-{uid}@example.test", "amount": 20}).json()["cluster_id"]
    db.execute("UPDATE capi_events_log SET bowler_value_score = -1, threshold_at_send = -1 WHERE event_id = %s",
               (f"wr_SC{uid}",))
    run_worker_now(db, cid)
    row = capi_rows(db, f"wr_SC{uid}")[0]
    assert row["threshold_at_send"] == 55 and row["bowler_value_score"] == customer_row(db, cid)["bowler_value_score"]


def test_melissa_household_key_still_merges_in_deferred_mode(client, db, fake, uid, deferred, monkeypatch):
    monkeypatch.setattr(main, "MELISSA_LIVE", True)
    mik = f"MIK-{uid}"
    owner = client.post("/webhooks/webres", headers=WH, json={
        "reservation_id": f"MK{uid}", "first_name": "House", "last_name": f"Hold{uid}",
        "email": f"house-{uid}@example.test", "amount": 20}).json()["cluster_id"]
    db.execute("INSERT INTO customer_identities (cluster_id, id_type, id_value, source_system) "
               "VALUES (%s, 'md_mik', %s, 'test')", (owner, mik))
    monkeypatch.setattr(main, "melissa_lookup", lambda *a, **kw: {"md_mik": mik, "md_address1": "1 Same St",
                                                                  "md_results": "AS01"})
    r = client.post("/webhooks/partywirks", headers=WH, json=fake.add_partywirks(uid, f"26{int(uid, 16)}"))
    assert r.json().get("cluster_id") == owner


# --- Manual Melissa.com upload batches ----------------------------------------------------------

def _melissa_ready(db, uid, n, **over):
    """n customers Versium found nothing for (name + email/phone, no demographics), ready for Melissa."""
    ids = []
    for i in range(n):
        cid = _new_customer(db, uid, f"mq{i}-{uid}@example.test",
                            flags=over.get("flags", {"_versium_checked": "2026-11-18"}))
        db.execute("UPDATE master_customers SET phone = %s, md_results = 'MELISSA_OFF' WHERE cluster_id = %s",
                   (f"+1 (248) 555-{i:04d}", cid))
        ids.append(cid)
    return ids


def _melissa_isolate(db):
    """The test database is a copy of production: park its real queue and batches so only this test's count."""
    db.execute(f"UPDATE master_customers SET melissa_upload_status = 'prior' WHERE {main.MELISSA_UPLOAD_ELIGIBLE}")
    db.execute("UPDATE melissa_batches SET emailed_at = NOW() WHERE emailed_at IS NULL")


def test_melissa_batch_waits_for_a_full_batch_then_emails_it_once(client, db, uid, monkeypatch):
    monkeypatch.setattr(main, "MELISSA_BATCH_SIZE", 3)
    _melissa_isolate(db)
    first = _melissa_ready(db, uid, 2)
    r = client.post("/cron/melissa-batch", headers=CRON).json()
    assert r == {"ready": False, "waiting": 2, "batch_size": 3}
    extra = _melissa_ready(db, uid + "b", 2)
    r = client.post("/cron/melissa-batch", headers=CRON).json()
    assert r["ready"] and r["customers"] == 3 and r["filename"].startswith(f"melissa-batch-{r['batch_id']:03d}-")
    rows = list(csv.DictReader(io.StringIO(r["csv"])))
    assert [int(x["RecordID"]) for x in rows] == sorted(first + extra)[:3]  # oldest first
    assert rows[0]["Phone"] == "2485550000" and rows[0]["Email"] == f"mq0-{uid}@example.test"
    assert rows[0]["First Name"] == "Vee" and rows[0]["Address"] == ""
    # Not confirmed as emailed: the same batch comes back, nothing new is made.
    again = client.post("/cron/melissa-batch", headers=CRON).json()
    assert again["batch_id"] == r["batch_id"] and again["csv"] == r["csv"]
    done = client.post(f"/cron/melissa-batch/{r['batch_id']}/emailed", headers=CRON).json()
    assert done["status"] == "emailed"
    r = client.post("/cron/melissa-batch", headers=CRON).json()
    assert r == {"ready": False, "waiting": 1, "batch_size": 3}  # the 4th waits for the next batch
    assert customer_row(db, sorted(first + extra)[0])["melissa_upload_status"] == "sent"


def test_melissa_batch_leaves_out_matched_unnamed_waiting_and_prior(client, db, uid, monkeypatch):
    monkeypatch.setattr(main, "MELISSA_BATCH_SIZE", 1)
    _melissa_isolate(db)
    matched = _melissa_ready(db, uid + "m", 1, flags={"_versium_checked": "x", "household_income": "$1"})[0]
    net_worth_only = _melissa_ready(db, uid + "w", 1, flags={"_versium_checked": "x",
                                                            "estimated_net_worth": "$1"})[0]
    unchecked = _melissa_ready(db, uid + "u", 1, flags={})[0]
    nameless, waiting, melissa_demo, prior = _melissa_ready(db, uid, 4)
    db.execute("UPDATE master_customers SET last_name = NULL WHERE cluster_id = %s", (nameless,))
    db.execute("UPDATE master_customers SET enrich_after = NOW() WHERE cluster_id = %s", (waiting,))
    db.execute("UPDATE master_customers SET md_household_income = '$60,001-$75,000' WHERE cluster_id = %s",
               (melissa_demo,))
    db.execute("UPDATE master_customers SET melissa_upload_status = 'prior' WHERE cluster_id = %s", (prior,))
    assert client.post("/cron/melissa-batch", headers=CRON).json() == {"ready": False, "waiting": 0,
                                                                       "batch_size": 1}
    for cid in (matched, net_worth_only, unchecked, nameless, waiting, melissa_demo):
        assert customer_row(db, cid)["melissa_upload_status"] is None
    db.execute("UPDATE master_customers SET enrich_after = NULL WHERE cluster_id = %s", (waiting,))


def test_melissa_batch_endpoints_need_the_cron_key(client):
    for path in ("/cron/melissa-batch", "/cron/melissa-batch/1/emailed"):
        assert client.post(path).status_code == 401
        assert client.post(path, headers=WH).status_code == 401
    assert client.post("/cron/melissa-batch/999999999/emailed", headers=CRON).status_code == 404


def ts_lost_event_webhook(uid):
    """The shape of the Event webhooks that crashed on Nov 18, 2026 (field names from the production log;
    values are made up). Only LOST events carry lost_reason, and it is an object, not text."""
    contact = {"id": int(uid[:6], 16), "account_id": 9001, "first_name": "Lost", "last_name": f"Event{uid}",
               "email_addresses": [{"id": 1, "address": f"lost-{uid}@example.test"}],
               "phone_numbers": [{"id": 2, "number": "(248) 555-0142", "extension": None,
                                  "phone_number_type": "Work"}],
               "websites": [{"id": 3, "url": "https://example.test"}]}
    account = {"id": 9001, "name": f"Lost Event Co {uid}"}
    creator = {"id": 5, "customer_id": 77, "first_name": "Sales", "last_name": "Person", "email": "sales@example.test",
               "title": "Event Manager", "login_count": 10, "phone_numbers": [], "created_at": "2025-01-01T00:00:00Z",
               "updated_at": "2026-11-01T00:00:00Z", "login_at": "2026-11-18T15:00:00Z"}
    return {"event": {
        "id": int(uid[:7], 16), "booking_id": int(uid[:8], 16), "status": "LOST",
        "lost_reason": {"id": 12, "name": "Went with another venue"}, "lost_reason_details": "Price",
        "event_date": "12/12/2026", "event_start_iso8601": "2026-12-12T18:00:00-05:00", "guest_count": 60,
        "account": account, "account_id": 9001, "contact": contact, "contact_id": contact["id"],
        "creator": creator, "created_by": 5, "created_at": "2026-10-01T12:00:00Z", "customer_id": 77,
        "deleted_at": None, "actual_amount": "2400.00", "amount_due": "2400.00", "deposit_amount": None,
        "grand_total": "2400.00", "price_per_person": None, "rental_fee": None, "payment_set": None,
        "billing_totals": [{"name": "Food", "total": "2000.00"}],
        "category_totals": [{"name": "Bowling", "total": "400.00"}],
        "custom_fields": [{"id": 1, "custom_field_id": 2, "custom_field_name": "Occasion",
                           "custom_field_slug": "occasion", "custom_field_required": False, "value": "Holiday party"}],
        "attachments": [], "delivery_instructions": None, "delivery_provider": None, "description": None,
        "documents": [{"financials": {"amount_due": "2400.00", "grand_total": "2400.00", "discounts_total": None},
                       "line_items": [{"price": "40.00", "total_price": "2400.00", "grand_total": "2400.00"}],
                       "billing_totals": [], "category_totals": [],
                       "payment_set": {"amount_due": "2400.00", "remaining_payment_amount": "2400.00",
                                       "running_balance": "0.00", "payments": []}}],
        "booking": {"id": int(uid[:8], 16), "account": account, "contact": contact, "status": "LOST",
                    "lost_reason": {"id": 12, "name": "Went with another venue"}, "created_at": "2026-10-01T12:00:00Z",
                    "created_by": 5, "creator": creator, "booking_forecast": None, "category_forecasts": [],
                    "payment_set": None, "total_actual_amount": "2400.00", "total_event_actual_amount": "2400.00",
                    "total_event_grand_total": "2400.00", "total_grand_total": "2400.00",
                    "total_food_and_beverage_min": None, "total_rental_fee": None},
    }}


def test_tripleseat_lost_event_with_lost_reason_object_is_saved(client, db, fake, uid):
    r = client.post("/webhooks/tripleseat", headers=WH, json=ts_lost_event_webhook(uid))
    assert r.status_code == 200, r.text
    c = customer_row(db, r.json()["cluster_id"])
    assert c["ts_turned_down_reason"] == "Went with another venue"
    assert c["ts_lead_status"] == "LOST" and c["ts_account"] == f"Lost Event Co {uid}"
    assert c["email"] == f"lost-{uid}@example.test" and c["in_tripleseat"] == 1
    assert capi_rows(db, f"qb_ts_{c['cluster_id']}_%") == []  # a lost event is not a Qualified_Bowler


def test_failed_tripleseat_webhook_is_kept_and_retried(client, db, fake, uid, monkeypatch):
    from fastapi.testclient import TestClient
    body = ts_lost_event_webhook(uid)
    real = main.handle_tripleseat

    def broken(raw, tasks):
        raise RuntimeError("simulated outage")
    monkeypatch.setattr(main, "handle_tripleseat", broken)
    failing = TestClient(main.app, raise_server_exceptions=False)
    for _ in range(2):  # Tripleseat re-sending the same payload is stored once
        assert failing.post("/webhooks/tripleseat", headers=WH, json=body).status_code == 500
    db.execute("SELECT id, attempts, resolved_at, error FROM webhook_dead_letters "
               "WHERE source = 'tripleseat' AND payload->'event'->'contact'->>'last_name' = %s",
               (f"Event{uid}",))
    rows = db.fetchall()
    assert len(rows) == 1 and rows[0]["resolved_at"] is None and "simulated outage" in rows[0]["error"]

    monkeypatch.setattr(main, "handle_tripleseat", real)
    res = client.post("/cron/retry-webhooks", headers={"X-CDP-Key": "test-cron-secret"}).json()
    assert rows[0]["id"] in res["resolved"]
    db.execute("SELECT attempts, resolved_at, result FROM webhook_dead_letters WHERE id = %s", (rows[0]["id"],))
    dl = db.fetchone()
    assert dl["resolved_at"] is not None and dl["attempts"] == 1 and dl["result"]["status"] == "tripleseat_synced"
    assert customer_row(db, dl["result"]["cluster_id"])["ts_turned_down_reason"] == "Went with another venue"
    again = client.post("/cron/retry-webhooks", headers={"X-CDP-Key": "test-cron-secret"}).json()
    assert rows[0]["id"] not in again["resolved"] + again["failed"]
