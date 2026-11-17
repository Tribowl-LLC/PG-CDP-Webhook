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

    def count(self, fragment):
        return sum(1 for method, url, _ in self.calls if fragment in url)

    def meta_events(self):
        return [kw["json"]["data"][0] for m, url, kw in self.calls if url.endswith("/events")]

    def get(self, url, **kw):
        self.calls.append(("GET", url, kw))
        if "personator.melissadata.net" in url:
            return self._melissa(kw["params"])
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
