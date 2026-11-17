"""scripts/recover_identities.py against a disposable database copy (see conftest.py)."""
import importlib.util
import pathlib

from test_integration import customer_row, payment, square_post, _history_rows


def _load():
    path = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "recover_identities.py"
    spec = importlib.util.spec_from_file_location("recover_identities", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _pay(pid, sq, fp=None, email=None, name=None, zip5=None):
    card = {}
    if fp:
        card["fingerprint"] = fp
    if name:
        card["cardholder_name"] = name
    if zip5:
        card["billing_address"] = {"postal_code": zip5}
    p = {"id": pid, "status": "COMPLETED", "customer_id": sq, "total_money": {"amount": 100},
         "created_at": "2025-06-01T18:00:00Z", "card_details": {"card": card}}
    if email:
        p["buyer_email_address"] = email
    return p


def _identity(db, cid, id_type):
    db.execute("SELECT id_value FROM customer_identities WHERE cluster_id = %s AND id_type = %s", (cid, id_type))
    return {r["id_value"] for r in db.fetchall()}


def test_recovery_links_credits_holds_back_and_skips(client, db, fake, uid):
    ri = _load()
    # Known customers, each with a live $10 payment under a Square ID we know.
    def known(tag, fp):
        sq = f"K{tag}{uid}"
        fake.square_customers[sq] = {"id": sq, "email_address": f"{tag}-{uid}@example.test"}
        cid = square_post(client, payment(f"L{tag}{uid}", 1000, sq_id=sq)).json()["cluster_id"]
        return sq, cid
    sq_a, a = known("a", None)
    sq_b, b = known("b", None)
    sq_c, c = known("c", None)
    sq_d, d = known("d", None)
    sq_e, e = known("e", None)
    db.execute("UPDATE master_customers SET sq_spend = 500 WHERE cluster_id = %s", (c,))   # imported total

    u_email, u_fp, u_held, u_new, u_shared = (f"U{t}{uid}" for t in ("email", "fp", "held", "new", "shared"))
    _history_rows(db, u_email, (f"E1{uid}", 40.0, "2025-02-01"))
    _history_rows(db, u_fp, (f"F1{uid}", 25.0, "2025-03-01"))
    _history_rows(db, u_held, (f"HB1{uid}", 40.0, "2025-04-01"))
    _history_rows(db, u_new, (f"NW1{uid}", 30.0, "2025-05-01"), (f"NW2{uid}", 5.0, "2025-01-15"))
    _history_rows(db, u_shared, (f"SH1{uid}", 60.0, "2025-05-01"))

    fake.square_payments = [
        _pay(f"p1{uid}", sq_b, fp=f"FPB{uid}"),                        # B's card
        _pay(f"p2{uid}", sq_d, fp=f"FPS{uid}"), _pay(f"p3{uid}", sq_e, fp=f"FPS{uid}"),  # card shared by D and E
        _pay(f"p4{uid}", u_email, email=f"A-{uid}@example.test".lower()),
        _pay(f"p5{uid}", u_fp, fp=f"FPB{uid}"),
        _pay(f"p6{uid}", u_held, email=f"c-{uid}@example.test"),
        _pay(f"p7{uid}", u_new, email=f"brandnew-{uid}@example.test", name=f"NEW/PERSON{uid}", zip5="48335"),
        _pay(f"p8{uid}", u_shared, fp=f"FPS{uid}"),
    ]
    db.execute("SELECT count(*) AS n FROM customer_identities")
    before = db.fetchone()["n"]
    calls_before = len(fake.calls)
    ri.run("2023-12-01T00:00:00Z", apply=False)
    db.execute("SELECT count(*) AS n FROM customer_identities")
    assert db.fetchone()["n"] == before                                         # dry run wrote nothing
    assert float(customer_row(db, a)["sq_spend"]) == 10.0
    assert not [u for m, u, _ in fake.calls[calls_before:] if "squareup.com" not in u]   # no Melissa/Meta

    ri.run("2023-12-01T00:00:00Z", apply=True)
    ri.run("2023-12-01T00:00:00Z", apply=True)                                  # second run changes nothing

    ca = customer_row(db, a)
    assert float(ca["sq_spend"]) == 50.0 and ca["sq_visits"] == 2               # receipt email: linked + credited
    assert u_email in _identity(db, a, "sq_id")
    cb = customer_row(db, b)
    assert float(cb["sq_spend"]) == 35.0 and cb["sq_visits"] == 2               # card fingerprint
    assert u_fp in _identity(db, b, "sq_id") and f"FPB{uid}" in _identity(db, b, "card_fingerprint")
    cc = customer_row(db, c)
    assert float(cc["sq_spend"]) == 500.0                                       # held back: linked, not credited
    assert u_held in _identity(db, c, "sq_id")
    db.execute("SELECT cluster_id FROM unified_transactions WHERE transaction_id = %s", (f"sq_pay_HB1{uid}",))
    assert db.fetchone()["cluster_id"] is None
    for cid in (d, e):                                                          # shared card: nobody gets it
        assert u_shared not in _identity(db, cid, "sq_id")
        assert f"FPS{uid}" not in _identity(db, cid, "card_fingerprint")
    db.execute("SELECT cluster_id FROM customer_identities WHERE id_type = 'sq_id' AND id_value = %s", (u_new,))
    rows = db.fetchall()
    assert len(rows) == 1
    cn = customer_row(db, rows[0]["cluster_id"])
    assert float(cn["sq_spend"]) == 35.0 and cn["sq_visits"] == 2               # new person, history credited once
    assert str(cn["sq_first_visit_date"]) == "2025-01-15"
    assert cn["email"] == f"brandnew-{uid}@example.test"


def test_recovery_directory_links_and_creates(client, db, fake, uid):
    ri = _load()
    email = f"dir-{uid}@example.test"
    known_sq = f"KD{uid}"
    fake.square_customers[known_sq] = {"id": known_sq, "email_address": email}
    cid = square_post(client, payment(f"LD{uid}", 1000, sq_id=known_sq)).json()["cluster_id"]
    second, newbie = f"D2{uid}", f"DN{uid}"
    _history_rows(db, second, (f"DS1{uid}", 20.0, "2025-02-01"))
    _history_rows(db, newbie, (f"DN1{uid}", 15.0, "2025-02-01"))
    fake.square_directory = [
        {"id": second, "email_address": email},                                   # second profile, same person
        {"id": newbie, "given_name": "Dir", "family_name": f"New{uid}", "email_address": f"dirnew-{uid}@example.test"},
        {"id": f"DB{uid}", "given_name": "No", "family_name": "Contact"},          # no email/phone: left alone
    ]
    fake.square_payments = []
    ri.run("2023-12-01T00:00:00Z", apply=True)
    c = customer_row(db, cid)
    assert float(c["sq_spend"]) == 30.0 and second in _identity(db, cid, "sq_id")
    db.execute("SELECT cluster_id FROM customer_identities WHERE id_type = 'sq_id' AND id_value = %s", (newbie,))
    n = customer_row(db, db.fetchone()["cluster_id"])
    assert float(n["sq_spend"]) == 15.0
    db.execute("SELECT 1 FROM customer_identities WHERE id_value = %s", (f"DB{uid}",))
    assert db.fetchone() is None
