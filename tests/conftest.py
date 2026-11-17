"""Test setup.

Unit tests need nothing. Integration tests need TEST_DATABASE_URL pointing at a DISPOSABLE copy of the
database (e.g. a Neon branch) that already has migrations/001 applied. They write test customers into it.
All outside services (Square, Melissa, Versium, Meta) are faked; nothing leaves the machine except
database traffic.
"""
import os
import uuid

import pytest
import requests

os.environ["DATABASE_URL"] = os.environ.get("TEST_DATABASE_URL", "postgresql://unused/unused")
os.environ.update({
    "SQUARE_ACCESS_TOKEN": "test-square-token",
    "MELISSA_API_KEY": "test-melissa-key",
    "META_PIXEL_ID": "test-pixel",
    "META_ACCESS_TOKEN": "test-meta-token",
    "SQUARE_WEBHOOK_SIGNATURE_KEY": "test-square-signature-key",
    "SQUARE_WEBHOOK_URL": "https://example.test/webhooks/square",
    "WEBHOOK_SECRET": "test-webhook-secret",
    "CRON_SECRET": "test-cron-secret",
    "PARTYWIRKS_API_KEY": "test-partywirks-key",
})
os.environ.pop("VERSIUM_API_KEY", None)

import main  # noqa: E402


class FakeResponse:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


class FakeHTTP:
    """Routes every outbound call made by main.py."""

    def __init__(self):
        self.calls = []
        self.square_customers = {}   # sq_id -> profile dict | "error" | "missing"
        self.meta_mode = "ok"        # "ok" | "500" | "timeout"
        self.pw_reservations = {}    # order_id -> reservation (Partywirks API shape)
        self.pw_customers = {}       # customer id -> customer (Partywirks API shape)
        self.pw_down = False

    def count(self, fragment):
        return sum(1 for method, url, _ in self.calls if fragment in url)

    def meta_events(self):
        return [kw["json"]["data"][0] for m, url, kw in self.calls if url.endswith("/events")]

    def get(self, url, **kw):
        self.calls.append(("GET", url, kw))
        if "personator.melissadata.net" in url:
            return self._melissa(kw["params"])
        if "app.partywirks.com/api/v1" in url:
            return self._partywirks(url, kw)
        if "connect.squareup.com/v2/customers/" in url:
            sq_id = url.rsplit("/", 1)[1]
            prof = self.square_customers.get(sq_id, "missing")
            if prof == "error":
                raise requests.Timeout("square down")
            if prof == "missing":
                return FakeResponse(404, {})
            return FakeResponse(200, {"customer": prof})
        raise AssertionError(f"unexpected GET {url}")

    def post(self, url, **kw):
        self.calls.append(("POST", url, kw))
        if "graph.facebook.com" in url:
            if self.meta_mode == "timeout":
                raise requests.Timeout("meta down")
            if self.meta_mode == "500":
                return FakeResponse(500, {"error": "boom"})
            return FakeResponse(200, {"events_received": 1})
        raise AssertionError(f"unexpected POST {url}")

    def _partywirks(self, url, kw):
        assert kw["headers"]["Authorization"] == "Bearer test-partywirks-key"
        if self.pw_down:
            raise requests.ConnectionError("partywirks down")
        path = url.split("/api/v1", 1)[1]
        if path.startswith("/reservations/"):
            r = self.pw_reservations.get(path.rsplit("/", 1)[1])
            return FakeResponse(200, {"success": True, "data": r}) if r else FakeResponse(404, {"message": "Not found"})
        if path.startswith("/customers/"):
            c = self.pw_customers.get(path.rsplit("/", 1)[1])
            return FakeResponse(200, {"success": True, "data": c}) if c else FakeResponse(404, {"message": "Not found"})
        if path == "/reservations":
            params = kw.get("params") or {}
            rows = [r for r in self.pw_reservations.values() if r["event_date"] >= params.get("date_from", "")]
            per, page = params.get("per_page", 25), params.get("page", 1)
            chunk = rows[(page - 1) * per: page * per]
            return FakeResponse(200, {"success": True, "data": chunk,
                                      "pagination": {"has_more": page * per < len(rows), "total": len(rows)}})
        raise AssertionError(f"unexpected Partywirks call {url}")

    def add_partywirks(self, uid, order_id, status="confirmed", event_date="2026-12-05",
                       created_at="2026-11-16 10:00:00", package="BOWLING PARTY", with_address=True):
        cid = f"9{uid[:6]}"
        phone = "248" + str(int(uid[:8], 16) % 10_000_000).zfill(7)  # unique per test parent
        self.pw_customers[cid] = {
            "id": cid, "first_name": "Party", "last_name": f"Parent{uid}", "full_name": f"Party Parent{uid}",
            "email": f"party-{uid}@example.test", "phone": phone, "alternate_phone": None,
            "address": {"street_1": "200 Party Ln", "street_2": None, "city": "Farmington Hills",
                        "state": "MI", "zip_code": "48335"} if with_address else
                       {"street_1": None, "street_2": None, "city": None, "state": None, "zip_code": None},
        }
        self.pw_reservations[order_id] = {
            "order_id": order_id, "status": status, "event_date": event_date, "event_time_start": "12:30:00",
            "event_name": "Child's 8th Birthday", "guests_count": 14, "customer_id": cid, "activity_id": 1,
            "customer_responses": [{"question": "Child's name", "answer": "Kid"}],
            "totals": {"total": 728.7}, "customer": {"id": cid, "name": f"Party Parent{uid}",
                                                    "email": f"party-{uid}@example.test", "phone": phone},
            "activity": {"id": 1, "name": package}, "created_at": created_at, "updated_at": created_at,
        }
        return {"event": "reservation.created", "data": self.pw_reservations[order_id]}

    @staticmethod
    def _melissa(params):
        email = params.get("email") or ""
        if "timeout" in email:
            raise requests.Timeout("melissa down")
        if "nomatch" in email:
            return FakeResponse(200, {"Records": [{"Results": "AE01"}]})
        if "detroit" in email:
            city, zip5, lat, lon = "Detroit", "48226", 42.3314, -83.0458
        elif "grossepointe" in email:
            city, zip5, lat, lon = "Grosse Pointe Farms", "48236", 42.4092, -82.8919
        else:
            city, zip5, lat, lon = "Farmington Hills", "48335", 42.4800, -83.4000
        return FakeResponse(200, {"Records": [{
            "AddressLine1": "100 Test St", "City": city, "State": "MI", "PostalCode": zip5,
            "Latitude": str(lat), "Longitude": str(lon), "Results": "AS01",
            "MelissaIdentityKey": f"MIK-{uuid.uuid4().hex}", "MelissaAddressKey": f"MAK-{uuid.uuid4().hex}",
        }]})


@pytest.fixture
def fake(monkeypatch):
    f = FakeHTTP()
    monkeypatch.setattr(main.requests, "get", f.get)
    monkeypatch.setattr(main.requests, "post", f.post)
    return f


@pytest.fixture
def db():
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.skip("TEST_DATABASE_URL not set")
    conn = main.get_conn()
    conn.autocommit = True
    yield conn.cursor()
    conn.close()


@pytest.fixture
def client(db):
    from fastapi.testclient import TestClient
    return TestClient(main.app)


@pytest.fixture
def uid():
    return uuid.uuid4().hex[:12]
