import os
import re
import io
import csv
import hmac
import math
import time
import json
import base64
import hashlib
import logging
import asyncio
import collections
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional

import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.concurrency import run_in_threadpool
from contextlib import asynccontextmanager

log = logging.getLogger("pg_cdp")
logging.basicConfig(level=logging.INFO)


class _RedactKeyInAccessLog(logging.Filter):
    """uvicorn's access log prints the full request line, query string included, so a sender using ?key= put the
    shared webhook (or cron) key into Render's logs. This replaces any key= value with <redacted>."""
    _KEY = re.compile(r"((?:^|[?&])key=)[^&\s\"]*", re.I)

    def filter(self, record):
        if isinstance(record.args, tuple) and record.args:
            record.args = tuple(self._KEY.sub(r"\1<redacted>", a) if isinstance(a, str) else a for a in record.args)
        elif isinstance(record.msg, str):
            record.msg = self._KEY.sub(r"\1<redacted>", record.msg)
        return True


logging.getLogger("uvicorn.access").addFilter(_RedactKeyInAccessLog())



async def _enrichment_worker():
    """Every minute: enrich customers whose waiting window has passed (see process_pending_enrichment)."""
    while True:
        try:
            await run_in_threadpool(process_pending_enrichment)
        except Exception as e:
            log.exception("Deferred enrichment run failed: %s", type(e).__name__)
        await asyncio.sleep(ENRICH_WORKER_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(_app):
    await run_in_threadpool(sync_cluster_sequence)
    # Secret-blind check that Render holds the value from Tripleseat: compare length and character class with the
    # secret shown in Tripleseat (Settings > API/Webhooks). Never the value itself. Always logged, "not set" included.
    log.info("TRIPLESEAT_WEBHOOK_SECRET: %s", tripleseat_secret_shape(TRIPLESEAT_WEBHOOK_SECRET, _TS_SECRET_AS_SET))
    # Always on: with ENRICH_DELAY_MINUTES=0 it just drains customers still waiting from before the switch.
    worker = asyncio.create_task(_enrichment_worker()) if DATABASE_URL else None
    yield
    if worker:
        worker.cancel()


app = FastAPI(title="Perfect Game CDP, Enrichment & Meta CAPI Engine", lifespan=lifespan)

# Environment Variables (Configure in Render Dashboard)
DATABASE_URL = os.getenv("DATABASE_URL")
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN")
MELISSA_API_KEY = os.getenv("MELISSA_API_KEY")
VERSIUM_API_KEY = os.getenv("VERSIUM_API_KEY")
META_PIXEL_ID = os.getenv("META_PIXEL_ID")
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")

# Webhook / cron authentication. Each check is enforced as soon as its variable is set.
# Square: signature key from the Square Developer Dashboard webhook subscription.
SQUARE_WEBHOOK_SIGNATURE_KEY = os.getenv("SQUARE_WEBHOOK_SIGNATURE_KEY")
# Must exactly match the notification URL registered with Square (Square signs URL + body).
SQUARE_WEBHOOK_URL = os.getenv("SQUARE_WEBHOOK_URL", "https://pg-cdp-webhook.onrender.com/webhooks/square")
# WebRes / Tripleseat / Partywirks: send as header `X-CDP-Key` or query param `?key=`. Unset = every webhook is
# refused (401): a missing secret is a misconfiguration (/health shows webhook_key_check), never an open door.
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
# Rotation window: while set, the previous shared key is accepted too, so senders can be moved to the new one one
# at a time without a 401 (README: "Rotating WEBHOOK_SECRET"). Remove it once every sender uses the new key.
WEBHOOK_SECRET_PREVIOUS = os.getenv("WEBHOOK_SECRET_PREVIOUS")
# /cron/* endpoints: send as header `X-CDP-Key` or query param `?key=`.
CRON_SECRET = os.getenv("CRON_SECRET")
# Partywirks REST API (read-only use): reservations + customers.
PARTYWIRKS_API_KEY = os.getenv("PARTYWIRKS_API_KEY")
PARTYWIRKS_API = os.getenv("PARTYWIRKS_API", "https://app.partywirks.com/api/v1")
# Signature key from the Partywirks webhook settings. Partywirks signs every event with HMAC-SHA256 in
# the X-Partywirks-Signature header. Checked and logged when set; only enforced (401) once
# PARTYWIRKS_SIGNATURE_ENFORCE=true, after a real delivery has been seen to match.
PARTYWIRKS_WEBHOOK_SECRET = os.getenv("PARTYWIRKS_WEBHOOK_SECRET")
PARTYWIRKS_SIGNATURE_ENFORCE = os.getenv("PARTYWIRKS_SIGNATURE_ENFORCE", "").strip().lower() in ("1", "true", "yes")
# Tripleseat signs every webhook (header X-Signature: t=<timestamp>,v1=<HMAC-SHA256>) with the signing secret of
# its webhook endpoint (Settings > API/Webhooks). Tripleseat can't send custom headers, so this signature is how
# the shared key leaves its URL. Checked and logged when TRIPLESEAT_WEBHOOK_SECRET is set (the shared key is still
# required); once TRIPLESEAT_SIGNATURE_ENFORCE=true AND TRIPLESEAT_SIGNATURE_SCHEME names the construction real
# deliveries were seen to verify with, a valid signature is required and replaces the shared key.
# Stripped like the settings below: a newline or space pasted into Render with the secret breaks every HMAC.
_TS_SECRET_AS_SET = os.getenv("TRIPLESEAT_WEBHOOK_SECRET") or ""
TRIPLESEAT_WEBHOOK_SECRET = _TS_SECRET_AS_SET.strip() or None
TRIPLESEAT_SIGNATURE_ENFORCE = os.getenv("TRIPLESEAT_SIGNATURE_ENFORCE", "").strip().lower() in ("1", "true", "yes")
TRIPLESEAT_SIGNATURE_SCHEME = os.getenv("TRIPLESEAT_SIGNATURE_SCHEME", "").strip()
TRIPLESEAT_SIGNATURE_MAX_SKEW_SECONDS = 300  # a signed timestamp more than 5 minutes off our clock is rejected
# Real Partywirks order IDs are 13-14 digits; their sample/test payload uses "PW-YYYYMM-NNNN".
REAL_PARTYWIRKS_ORDER_ID = re.compile(r"^\d{10,20}$")

# Tripleseat REST API (OAuth 2, read-only use). Off until TRIPLESEAT_CLIENT_SECRET is set (Tripleseat
# releases it once they approve the app). The redirect URI must match the one registered with Tripleseat.
TRIPLESEAT_CLIENT_ID = os.getenv("TRIPLESEAT_CLIENT_ID")
TRIPLESEAT_CLIENT_SECRET = os.getenv("TRIPLESEAT_CLIENT_SECRET")
TRIPLESEAT_REDIRECT_URI = os.getenv(
    "TRIPLESEAT_REDIRECT_URI", "https://pg-cdp-webhook.onrender.com/oauth/tripleseat/callback")
# Space-separated; blank asks for the scopes configured on the Tripleseat app.
TRIPLESEAT_SCOPES = os.getenv("TRIPLESEAT_SCOPES", "").strip()
TRIPLESEAT_AUTHORIZE_URL = "https://login.tripleseat.com/oauth2/authorize"
TRIPLESEAT_TOKEN_URL = "https://api.tripleseat.com/oauth2/token"
TRIPLESEAT_API = os.getenv("TRIPLESEAT_API", "https://api.tripleseat.com/v1")

# Deferred enrichment. A WebRes / Partywirks booking's Square payment arrives first and the booking details
# (email, phone, address) follow a few minutes later, so webhooks only save the customer and Versium, Census,
# scoring and the Meta events run ENRICH_DELAY_MINUTES after the first webhook, on the merged record.
# 0 = enrich inside the webhook, as before (also the switch-back if anything goes wrong).
ENRICH_DELAY_MINUTES = int(os.getenv("ENRICH_DELAY_MINUTES", "15"))
ENRICH_MAX_ATTEMPTS = 5            # Versium failures retried 15, 30, 45, 60 min later, then given up (nightly catch-up)
ENRICH_WORKER_INTERVAL_SECONDS = 60
ENRICH_BATCH = 20                  # customers per worker run

# The live Meta Custom Audiences (non-Detroit). The env var names are historical: AUDIENCE_PG1_CORP is the
# Corporate audience and AUDIENCE_PG2_FAMILIES the Birthday audience (both recreated 2026-11-20, replacing the old
# PG-1 / PG-2 audiences). Defaults are the current IDs; Render sets the same values.
AUDIENCE_PG1_CORP = os.getenv("AUDIENCE_PG1_CORP", "120246630337620358")       # Corporate
AUDIENCE_PG2_FAMILIES = os.getenv("AUDIENCE_PG2_FAMILIES", "120246630337610358")  # Birthday
AUDIENCE_PRIME_SUBURBS = os.getenv("AUDIENCE_PRIME_SUBURBS", "120246555019320358")
AUDIENCE_ALL_NONDETROIT = os.getenv("AUDIENCE_ALL_NONDETROIT", "120246555027130358")  # Master
# Broad Local (top-quarter spenders, frequent visitors, affluent 35-55). Empty = not uploaded.
AUDIENCE_BROAD_LOCAL = os.getenv("AUDIENCE_BROAD_LOCAL", "120246630337600358")

ALLEY_LAT, ALLEY_LON = 42.4705, -83.3932
EXCLUDED_CITIES = {
    "detroit", "pontiac", "redford", "redford charter township", "redford twp",
    "inkster", "highland park", "hamtramck", "southfield", "lathrup village",
    "oak park", "ferndale", "hazel park", "eastpointe", "harper woods",
    "ecorse", "river rouge"
}
EXCLUDED_ZIPS = {
    "48033", "48034", "48037", "48075", "48076",
    "48340", "48341", "48342", "48141"
}
# Every 482xx ZIP is excluded (Detroit and its enclaves) EXCEPT these, which are the Grosse Pointes:
#   48230 = Grosse Pointe, Grosse Pointe Farms, Grosse Pointe Park
#   48236 = Grosse Pointe Woods, Grosse Pointe Shores
# The city check still runs first, so a 48236 address whose city is Harper Woods or Detroit stays excluded.
ALLOWED_482_ZIPS = {"48230", "48236"}
PRIME_SUBURB_ZIPS = {
    "48331", "48334", "48335", "48336", "48374", "48375", "48377",
    "48150", "48152", "48154", "48167", "48168", "48322", "48323",
    "48324", "48301", "48302", "48304", "48009", "48025", "48170",
    "48187", "48188", "48382", "48390", "48393", "48178"
}
GENERIC_CARD_WORDS = {
    "visa", "mastercard", "amex", "discover", "cardholder", "valued",
    "customer", "guest", "user", "card", "debit", "credit"
}


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def sha256_hash(val) -> str:
    if not val:
        return ""
    return hashlib.sha256(str(val).strip().lower().encode("utf-8")).hexdigest()


def clean_phone(phone) -> str:
    """10-digit US phone used for matching inside Neon."""
    digits = "".join(c for c in str(phone or "") if c.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) == 10 else ""


def meta_phone(phone) -> str:
    """Meta requires the country code: 1XXXXXXXXXX."""
    p10 = clean_phone(phone)
    return f"1{p10}" if p10 else ""


def meta_city(city) -> str:
    """Meta wants lowercase letters only: 'Farmington Hills' -> 'farmingtonhills'."""
    return re.sub(r"[^a-z]", "", str(city or "").lower())


def meta_state(state) -> str:
    """Meta wants the 2-letter state code, lowercase. Defaults to Michigan when unknown."""
    s = re.sub(r"[^a-z]", "", str(state or "").lower())
    if s == "michigan" or not s:
        return "mi"
    return s if len(s) == 2 else ""


def to_int(val, default: int = 0) -> int:
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return default


def to_float(val, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def date_only(val):
    if isinstance(val, str) and "T" in val:
        return val.split("T")[0]
    return val or None


def as_text(val) -> Optional[str]:
    """A webhook value for a text column. Tripleseat sends some fields as objects: `lost_reason` on a LOST
    event or booking is {"id", "name"}, so an object becomes its name and a list its names joined."""
    if isinstance(val, dict):
        return as_text(val.get("name") or val.get("value") or val.get("label"))
    if isinstance(val, (list, tuple)):
        return ", ".join(t for t in (as_text(v) for v in val) if t) or None
    if val is None:
        return None
    return str(val).strip() or None


def parse_cardholder_name(raw_name: str) -> tuple:
    """Parses EMV chip cardholder_name (handles 'LAST/FIRST' and 'FIRST LAST' formats)."""
    if not raw_name or not isinstance(raw_name, str):
        return "", ""
    cleaned = raw_name.strip()
    if "/" in cleaned:
        parts = [p.strip() for p in cleaned.split("/", 1)]
        last_n = parts[0].title()
        first_n = (parts[1].split()[0] if parts[1] else "").title()
    else:
        parts = cleaned.split()
        first_n = parts[0].title() if parts else ""
        last_n = parts[-1].title() if len(parts) > 1 else ""
    if len(first_n) < 2 or len(last_n) < 2:
        return "", ""
    if first_n.lower() in GENERIC_CARD_WORDS or last_n.lower() in GENERIC_CARD_WORDS:
        return "", ""
    return first_n, last_n


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return round(2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a)), 2)


def is_detroit_excluded(city, zip5) -> bool:
    c = " ".join(str(city or "").strip().lower().split())
    z = str(zip5 or "").strip()[:5]
    if c in EXCLUDED_CITIES or z in EXCLUDED_ZIPS:
        return True
    if z in ALLOWED_482_ZIPS:
        return False
    return z.startswith("482")


# Versium online audience append (Meta / Google / LinkedIn / TikTok IDs) is only for customers we can advertise to
# locally: never excluded areas, never out of state. Unknown state is allowed so Versium can help locate them.
def versium_online_append_eligible(city, state, zip5) -> bool:
    if is_detroit_excluded(city, zip5):
        return False
    st = str(state or "").strip().upper()
    return st in ("", "MI", "MICHIGAN")


# Versium fields we never store or use: credit data (owner rule: no credit scores) and sensitive
# categories (owner decision Nov 18, 2026). Matched by substring of the normalized field name.
VERSIUM_BLOCKED_FIELD_PARTS = ("credit_rating", "credit_lines", "ethnic", "religion", "party_affiliation",
                               "politic")


def strip_blocked_versium_fields(fields: dict) -> dict:
    def norm(k):
        return re.sub(r"[^a-z0-9]+", "_", str(k).lower()).strip("_")
    return {k: v for k, v in (fields or {}).items()
            if not any(part in norm(k) for part in VERSIUM_BLOCKED_FIELD_PARTS)}


VERSIUM_ONLINE_AUDIENCE_SERVICES = ("facebook", "google", "linkedin", "tiktok")
VERSIUM_ONLINE_BUDGET_SECONDS = 6
_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")


def parse_versium_online_audience(body: dict) -> dict:
    """Versium /v2/onlineAudience answer -> {service: {field: [values]}} for the services we use.
    Each service holds a list of objects whose fields are lists; they are merged and de-duplicated."""
    res = ((body or {}).get("versium") or {}).get("results") or []
    out = {}
    for rec in res if isinstance(res, list) else []:
        for svc in VERSIUM_ONLINE_AUDIENCE_SERVICES:
            for obj in (rec or {}).get(svc) or []:
                for field, vals in (obj or {}).items():
                    vals = vals if isinstance(vals, list) else [vals]
                    bucket = out.setdefault(svc, {}).setdefault(str(field).lower(), [])
                    for v in vals:
                        v = str(v).strip()
                        if v and v not in bucket:
                            bucket.append(v)
    return out


# --- Provider HTTP calls (Melissa, Versium) ---------------------------------------------------------------
# A transient failure (timeout, connection error, HTTP 5xx, 429) is retried inside the call, with a short backoff
# that honours Retry-After on a 429; a permanent one (any other 4xx) returns at once, so a request that can't
# succeed never spends another credit. Every failure is logged with the provider, endpoint, HTTP status,
# response time and a redacted, truncated response body, so a burst can be diagnosed from the logs alone.
PROVIDER_ATTEMPTS = int(os.getenv("PROVIDER_ATTEMPTS", "3"))   # 1 call + 2 retries
PROVIDER_BACKOFF_SECONDS = (1.0, 3.0)
PROVIDER_RETRY_AFTER_MAX = 10.0
_TRANSIENT_STATUSES = {429, 500, 502, 503, 504}
_REDACT_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(\.[\w-]+)+")
_REDACT_DIGITS = re.compile(r"\d[\d\s().-]{6,}\d")


def _redact(text) -> str:
    """A response body for the logs: no email addresses or phone-like numbers, at most 300 characters."""
    t = _REDACT_EMAIL.sub("<email>", str(text or ""))
    return _REDACT_DIGITS.sub("<number>", t)[:300]


def _retry_after(resp) -> Optional[float]:
    try:
        return min(max(float(resp.headers.get("Retry-After")), 0.0), PROVIDER_RETRY_AFTER_MAX)
    except (TypeError, ValueError, AttributeError):
        return None


_provider_sleep = time.sleep  # tests replace it, so retry backoff doesn't slow the suite


def provider_get(provider: str, endpoint: str, url: str, *, params=None, headers=None, timeout: float,
                 attempts: int = None, outcome: Optional[dict] = None, sleep=None):
    """GET with retries for transient failures. Returns the last response (any status: the caller reads the
    body), or None when every attempt failed without a response (timeouts, connection errors). `outcome`, when
    given, gets {"status", "attempts", "seconds", "error"} for the per-customer enrichment record. The URL and
    parameters are never logged: they carry the API key and the customer's details."""
    attempts = max(1, attempts or PROVIDER_ATTEMPTS)
    sleep = sleep or _provider_sleep
    resp, err = None, None
    for n in range(1, attempts + 1):
        t0 = time.monotonic()
        try:
            resp, err = requests.get(url, params=params, headers=headers, timeout=timeout), None
        except Exception as e:  # Timeout, ConnectionError, ...
            resp, err = None, type(e).__name__
        took = round(time.monotonic() - t0, 2)
        status = getattr(resp, "status_code", None)
        transient = err is not None or status in _TRANSIENT_STATUSES
        if outcome is not None:
            outcome.update(status=status, attempts=n, seconds=took, error=err)
        if not transient:
            if n > 1:
                log.info("%s %s answered (HTTP %s) on attempt %d of %d", provider, endpoint, status, n, attempts)
            return resp
        body = _redact(getattr(resp, "text", "")) if resp is not None else ""
        last = n == attempts
        log.warning("%s %s failed (attempt %d of %d): %s after %.2fs%s%s", provider, endpoint, n, attempts,
                    f"HTTP {status}" if status else err, took, f"; body: {body}" if body else "",
                    "; giving up" if last else "; retrying")
        if last:
            return resp
        wait = (_retry_after(resp) if status == 429 else None)
        sleep(wait if wait is not None else PROVIDER_BACKOFF_SECONDS[min(n - 1, len(PROVIDER_BACKOFF_SECONDS) - 1)])
    return resp


VERSIUM_ONLINE_TIMEOUT = 8


def fetch_versium_online_audience(v_headers: dict, v_input: dict, timeout: float = VERSIUM_ONLINE_TIMEOUT,
                                  attempts: int = None, outcome: Optional[dict] = None):
    """Returns the parsed IDs plus `_checked` (today), `{"_checked": today}` when Versium has no match,
    or None when the call failed or was rejected (bad parameter, no credits, auth), which is logged
    with Versium's reason and left for /cron/versium-online-audience to retry."""
    resp = provider_get("Versium", "onlineAudience", "https://api.versium.com/v2/onlineAudience",
                        headers=v_headers,
                        params={**v_input, "rcfg_oa_services[]": list(VERSIUM_ONLINE_AUDIENCE_SERVICES)},
                        timeout=timeout, attempts=attempts, outcome=outcome)
    if resp is None:
        return None  # every attempt timed out or failed to connect (logged); stays retryable
    try:
        body = resp.json()
    except ValueError:
        body = None
    if resp.status_code == 200 and not isinstance(body, dict):
        log.error("Versium online audience returned an unreadable answer (HTTP 200): %s", _redact(resp.text))
        return None  # stays retryable; never recorded as a no-match
    if resp.status_code != 200:
        log.error("Versium online audience rejected (HTTP %s): %s", resp.status_code, _redact(resp.text))
        return None
    return {**parse_versium_online_audience(body), "_checked": date.today().isoformat()}


def save_online_audience(cur, cluster_id: int, oa: dict):
    """Stores a paid online-audience answer on its own, so it is never lost when another step fails."""
    if oa:
        cur.execute("UPDATE master_customers SET versium_online_audience = "
                    "(COALESCE(versium_online_audience, '{}'::jsonb) - '_pending') || %s::jsonb, updated_at = NOW() "
                    "WHERE cluster_id = %s;", (json.dumps(oa), cluster_id))


def versium_meta_hashes(cust: dict) -> tuple:
    """Versium's pre-hashed (SHA-256) Facebook emails and phones for this customer."""
    fb = (cust.get("versium_online_audience") or {}).get("facebook") or {}
    ok = lambda vals: [v.lower() for v in vals or [] if _SHA256_HEX.match(str(v).lower())]
    return ok(fb.get("email")), ok(fb.get("phone"))


def get_threshold(cur) -> int:
    cur.execute("SELECT current_score_threshold FROM cdp_thermostat_config WHERE id = 1;")
    return int(cur.fetchone()["current_score_threshold"])


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def _provided_key(request: Request) -> str:
    return request.headers.get("x-cdp-key") or request.query_params.get("key") or ""


def _key_matches(provided: str, expected: Optional[str]) -> bool:
    return bool(expected) and hmac.compare_digest(provided.encode(), expected.encode())


def _reject(request: Request, name: str, why: str):
    # Path and reason only: the supplied key is never logged (nor is the query string).
    log.warning("Rejected %s %s: %s (%s)", request.method, request.url.path, why, name)
    raise HTTPException(status_code=401, detail="Unauthorized")


def _require_key(request: Request, expected: Optional[str], name: str):
    if not expected:
        log.warning("%s is not set; %s is accepting unauthenticated requests", name, request.url.path)
        return
    if not hmac.compare_digest(_provided_key(request).encode(), expected.encode()):
        _reject(request, name, "wrong key" if _provided_key(request) else "no key")


def require_webhook_key(request: Request):
    """The shared webhook key (WebRes, Tripleseat until its signature is enforced, Partywirks). Fails closed when
    WEBHOOK_SECRET isn't set. During a rotation the previous key is accepted too, and logged as such so it's
    visible which sender still uses it."""
    if not WEBHOOK_SECRET:
        log.error("WEBHOOK_SECRET is not set: refusing %s %s", request.method, request.url.path)
        raise HTTPException(status_code=401, detail="Unauthorized")
    provided = _provided_key(request)
    if _key_matches(provided, WEBHOOK_SECRET):
        return
    if _key_matches(provided, WEBHOOK_SECRET_PREVIOUS):
        log.info("Accepted %s with WEBHOOK_SECRET_PREVIOUS: move this sender to the new key", request.url.path)
        return
    _reject(request, "WEBHOOK_SECRET", "wrong key" if provided else "no key")


def require_cron_key(request: Request):
    _require_key(request, CRON_SECRET, "CRON_SECRET")


# Declared on every /cron/* route so the /docs page shows a "key" box for "Try it out".
# The value itself is read from the request by require_cron_key (header X-CDP-Key or ?key=).
CRON_KEY_PARAM = Query(None, description="Your CRON_SECRET (needed when it is set in Render)")


def square_signature(body: bytes, key: str, url: str) -> str:
    digest = hmac.new(key.encode("utf-8"), url.encode("utf-8") + body, hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def verify_square_signature(request: Request, body: bytes):
    if not SQUARE_WEBHOOK_SIGNATURE_KEY:
        log.warning("SQUARE_WEBHOOK_SIGNATURE_KEY is not set; Square webhooks are not verified")
        return
    provided = request.headers.get("x-square-hmacsha256-signature", "")
    expected = square_signature(body, SQUARE_WEBHOOK_SIGNATURE_KEY, SQUARE_WEBHOOK_URL)
    if not hmac.compare_digest(provided.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Invalid Square signature")


async def read_json(request: Request):
    body = await request.body()
    try:
        return body, (json.loads(body) if body else {})
    except ValueError:
        raise HTTPException(status_code=400, detail="Body is not valid JSON")


# ---------------------------------------------------------------------------
# External lookups
# ---------------------------------------------------------------------------

def fetch_square_customer_profile(sq_id: str) -> Optional[dict]:
    """Name, email, phone, address and any card-on-file billing ZIP from Square.

    Returns None when Square could not be reached (caller may retry later),
    and {} when Square answered but has no such customer.
    """
    if not SQUARE_ACCESS_TOKEN or not sq_id:
        return None
    try:
        r = requests.get(
            f"https://connect.squareup.com/v2/customers/{sq_id}",
            headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}", "Accept": "application/json"},
            timeout=4,
        )
        if r.status_code == 404:
            return {}
        if r.status_code != 200:
            return None
        cust = r.json().get("customer") or {}
    except Exception:
        return None
    addr = cust.get("address") or {}
    postal = addr.get("postal_code") or ""
    if not postal and isinstance(cust.get("cards"), list):
        for card in cust["cards"]:
            c_zip = (card.get("billing_address") or {}).get("postal_code") or ""
            if c_zip:
                postal = c_zip
                break
    return {
        "first_name": cust.get("given_name") or "",
        "last_name": cust.get("family_name") or "",
        "email": cust.get("email_address") or "",
        "phone": cust.get("phone_number") or "",
        "address": addr.get("address_line_1") or "",
        "city": addr.get("locality") or "",
        "state": addr.get("administrative_district_level_1") or "",
        "zip": postal[:5] if postal else "",
    }


def square_api(method: str, path: str, params: dict = None, json_body: dict = None, timeout: int = 15):
    """JSON from the Square API, or None if it can't be reached. Waits out rate limits."""
    if not SQUARE_ACCESS_TOKEN:
        return None
    for attempt in range(4):
        try:
            r = requests.request(method, f"https://connect.squareup.com{path}", params=params, json=json_body,
                                 timeout=timeout, headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}",
                                                           "Accept": "application/json"})
        except Exception as e:
            log.warning("Square API %s unreachable: %s", path, type(e).__name__)
            return None
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(min(2 ** attempt, 10))
            continue
        if r.status_code != 200:
            log.warning("Square API %s returned HTTP %s", path, r.status_code)
            return None
        try:
            return r.json()
        except ValueError:
            return None
    return None


def order_line_items(order: dict) -> list:
    """Compact, storable line items: price key (variation id), names, quantity and dollar amounts."""
    items = []
    for li in (order or {}).get("line_items") or []:
        items.append({
            "variation_id": li.get("catalog_object_id"),
            "item": li.get("name"),
            "variation": li.get("variation_name"),
            "qty": to_float(li.get("quantity"), 0.0),
            "gross": round(to_int((li.get("gross_sales_money") or {}).get("amount")) / 100.0, 2),
            "total": round(to_int((li.get("total_money") or {}).get("amount")) / 100.0, 2),
        })
    return items


def save_order_items(cur, txn_id: str, order: dict, square_customer_id: str = None):
    items = order_line_items(order)
    cur.execute(
        """
        UPDATE unified_transactions
        SET line_items = COALESCE(line_items, %s::jsonb),
            variation_ids = COALESCE(variation_ids, %s),
            square_customer_id = COALESCE(square_customer_id, NULLIF(%s, ''))
        WHERE transaction_id = %s;
        """,
        (json.dumps(items), [i["variation_id"] for i in items if i["variation_id"]] or None,
         square_customer_id or "", txn_id),
    )


# Melissa is used for address work only: verify a street address, coordinates, carrier route, household
# key. The account is on a credit license, which answers GE29 ("Not Available for Credit License") to the
# Append/Move actions and to demographic fields, so: act=Check only, no demographic groups. Demographics
# and email/phone -> address come from Versium; census/school fields from the free Census Geocoder.
# Override with MELISSA_ACT / MELISSA_COLS (e.g. after a subscription upgrade).
MELISSA_ACT = os.getenv("MELISSA_ACT", "Check")
MELISSA_COLS = os.getenv("MELISSA_COLS", "GrpAddressDetails,GrpGeocode")
# Melissa for new customers (live enrichment). Off by default: on a credit license it can only check a
# street address the customer already gave, which the free Census Geocoder covers. Turn on (true) for a
# direct-mail campaign that needs verified addresses and carrier routes; /cron/melissa-geocode works either way.
MELISSA_LIVE = os.getenv("MELISSA_LIVE", "false").strip().lower() in ("1", "true", "yes")
# Recorded in md_results when there was no street address to check (Check needs one); a later street
# address triggers a lookup again.
MELISSA_NO_STREET = "NO_STREET_TO_CHECK"
# Recorded in md_results when enrichment finished with Melissa switched off (MELISSA_LIVE=false), so the
# customer isn't treated as "Melissa never answered" and looked up (and Versium paid) again.
MELISSA_OFF = "MELISSA_OFF"
CENSUS_GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/geographies/address"
CENSUS_BUDGET_SECONDS = 6


def census_geocode(address: str, city: str = "", state: str = "", zip_code: str = ""):
    """US Census Geocoder (free, no key): coordinates, distance, census tract/block, county and school
    districts for a street address. Only the address is sent. Returns {} when the Census answered with no
    match, or None when it wasn't asked (not enough address) or the call failed, so it stays retryable."""
    address = (address or "").strip()
    if not address or not ((zip_code or "").strip() or ((city or "").strip() and (state or "").strip())):
        return None
    try:
        resp = requests.get(CENSUS_GEOCODER_URL, params={
            "street": address, "city": city or "", "state": state or "", "zip": (zip_code or "")[:5],
            "benchmark": "Public_AR_Current", "vintage": "Current_Current", "layers": "all", "format": "json",
        }, timeout=5)
        resp.raise_for_status()
        matches = ((resp.json() or {}).get("result") or {}).get("addressMatches") or []
    except Exception as e:
        log.warning("Census geocoder failed: %s", type(e).__name__)
        return None
    if not matches:
        return {}
    m = matches[0]
    geo = m.get("geographies") or {}
    first = lambda layer, field: ((geo.get(layer) or [{}])[0] or {}).get(field) or None
    lat, lon = to_float((m.get("coordinates") or {}).get("y")), to_float((m.get("coordinates") or {}).get("x"))
    block_layer = next((k for k in geo if k.endswith("Census Blocks")), "2020 Census Blocks")
    return {
        "md_latitude": lat or None,
        "md_longitude": lon or None,
        "distance_miles": haversine_miles(ALLEY_LAT, ALLEY_LON, lat, lon) if (lat and lon) else None,
        "md_census_tract": first("Census Tracts", "TRACT"),
        "md_census_block": first(block_layer, "BLOCK"),
        "md_county_name": first("Counties", "BASENAME"),
        "md_unified_school_district_name": first("Unified School Districts", "NAME"),
        "md_elementary_school_district_name": first("Elementary School Districts", "NAME"),
    }


def apply_census(result: dict, census: dict) -> dict:
    """Census fills the census/school fields, and coordinates only when Melissa had none."""
    if not census:
        return result
    has_coords = bool(result.get("md_latitude"))
    for k, v in census.items():
        if v is None or (has_coords and k in ("md_latitude", "md_longitude", "distance_miles")):
            continue
        result[k] = v
    return result


MELISSA_TIMEOUT = 10


def melissa_lookup(first_name: str, last_name: str, email: str, phone: str,
                   address: str = "", city: str = "", state: str = "", zip_code: str = "",
                   outcome: Optional[dict] = None, attempts: int = None) -> dict:
    """Melissa Personator: verified address, geocode + distance, census/school fields, household demographics.
    Returns {} when Melissa isn't configured or didn't answer (so `md_results` stays None). `outcome`, when
    given, says what happened: answered / failed (HTTP status or error) / rejected (TransmissionResults)."""
    out = outcome if outcome is not None else {}
    if not MELISSA_API_KEY:
        out["result"] = "not_configured"
        return {}
    zip_code = (zip_code or "")[:5]
    if "Append" not in MELISSA_ACT and not (address or "").strip():
        out["result"] = "skipped_no_street"
        return {"md_results": MELISSA_NO_STREET}  # nothing Melissa can check; don't spend a credit
    if not any([(address or "").strip(), (email or "").strip(), clean_phone(phone)]):
        out["result"] = "skipped_no_inputs"
        return {}  # nothing to look up: a request would only spend a credit on a rejection
    try:
        m_resp = provider_get(
            "Melissa", "Personator", "https://personator.melissadata.net/v3/WEB/ContactVerify/doContactVerify",
            params={
                "id": MELISSA_API_KEY,
                "act": MELISSA_ACT,
                "cols": MELISSA_COLS,
                "format": "json",
                "full": f"{first_name} {last_name}".strip(),
                "email": email,
                "phone": phone,
                "a1": address,
                "city": city,
                "state": state,
                "postal": zip_code,
            },
            timeout=MELISSA_TIMEOUT, attempts=attempts, outcome=out,
        )
        if m_resp is None:
            out["result"] = "failed"
            return {}  # timeouts / connection errors on every attempt (logged by provider_get)
        if m_resp.status_code != 200:
            out["result"] = "failed"
            log.error("Melissa Personator failed (HTTP %s): %s", m_resp.status_code, _redact(m_resp.text))
            return {}
        body = m_resp.json()
        rec = (body.get("Records") or [{}])[0]
    except Exception as e:
        out["result"] = "failed"
        log.warning("Melissa Personator returned an unreadable answer: %s", type(e).__name__)
        return {}
    # Melissa pads missing values with spaces; never let a blank overwrite real data.
    rec = {k: (v.strip() if isinstance(v, str) else v) for k, v in (rec or {}).items()}
    rec = {k: v for k, v in rec.items() if v not in ("", None)}
    transmission = str((body or {}).get("TransmissionResults") or "").strip()
    if transmission:
        # Melissa rejected the request itself (bad parameter, license, credits): permanent for this request, so
        # it isn't retried here (a retry would fail the same way); the customer stays retryable for later.
        out["result"] = f"rejected:{transmission[:40]}"
        log.error("Melissa Personator rejected the request (TransmissionResults=%s)", transmission[:100])
        return {}
    out["result"] = "answered"
    has_street = bool((rec.get("AddressLine1") or address or "").strip())
    lat = to_float(rec.get("Latitude")) if has_street else 0.0
    lon = to_float(rec.get("Longitude")) if has_street else 0.0
    dist = haversine_miles(ALLEY_LAT, ALLEY_LON, lat, lon) if (lat and lon) else None
    street = lambda k: (rec.get(k) or None) if has_street else None
    return {
        "md_address1": rec.get("AddressLine1") or address or None,
        "md_city": street("City") or city or None,
        "md_state": street("State") or state or None,
        "zip5": ((rec.get("PostalCode") or zip_code)[:5] if has_street else None) or zip_code or None,
        "md_plus4": street("Plus4"),
        "md_carrier_route": street("CarrierRoute"),
        "md_latitude": lat or None,
        "md_longitude": lon or None,
        "distance_miles": dist,
        "md_census_tract": street("CensusTract"),
        "md_census_block": street("CensusBlock"),
        "md_county_name": street("CountyName"),
        "md_unified_school_district_name": street("UnifiedSchoolDistrictName"),
        "md_elementary_school_district_name": street("ElementarySchoolDistrictName"),
        "md_household_income": rec.get("HouseholdIncome") or None,
        "md_presence_of_children": rec.get("PresenceOfChildren") or None,
        "md_children_age_range": rec.get("ChildrenAgeRange") or None,
        "md_own_rent": rec.get("HomeOwnerStatus") or None,
        "md_marital_status": rec.get("MaritalStatus") or None,
        "md_household_size": rec.get("HouseholdSize") or None,
        "md_length_of_residence": rec.get("LengthOfResidence") or None,
        "md_education": rec.get("Education") or None,
        "md_occupation": rec.get("Occupation") or None,
        "md_date_of_birth": rec.get("DateOfBirth") or None,
        "md_gender": rec.get("Gender") or None,
        "md_delivery_indicator": rec.get("DeliveryIndicator") or None,
        "md_mik": rec.get("MelissaIdentityKey") or None,
        "md_mak": rec.get("MelissaAddressKey") or None,
        "md_results": rec.get("Results") or "NO_RESULT_CODES",
    }


# ---------------------------------------------------------------------------
# Versium REACH contact + demographic append. Versium answers with display-name fields ("Phone",
# "Postal Address", "Estimated Net Worth", ...); they are stored under the same snake_case names the
# Nov 18 CSV import used. Valid demographic outputs: demographic, financial, lifestyle (political and
# full_demographic are never requested). A rejected call is logged and returns None so it stays retryable.
# ---------------------------------------------------------------------------
VERSIUM_DEMOGRAPHIC_OUTPUTS = ("demographic", "financial", "lifestyle")
VERSIUM_TIMEOUT = 8  # was 5: the observed ReadTimeouts were slow answers, not outages
# Versium is required for enrichment to count as finished (production). Tests and deployments without Versium
# set it to false; then a customer with no Versium key is treated as fully enriched, as before.
VERSIUM_REQUIRED = os.getenv("VERSIUM_REQUIRED", "true").strip().lower() in ("1", "true", "yes")


def versium_key(name) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).lower()).strip("_")


def versium_call(endpoint: str, v_headers: dict, params: dict, timeout: float = VERSIUM_TIMEOUT,
                 attempts: int = None, outcome: Optional[dict] = None):
    """First result of a Versium /v2/<endpoint> call: a dict ({} = no match), or None when the call failed
    or was rejected (logged with the HTTP status and Versium's answer). Transient failures are retried."""
    resp = provider_get("Versium", endpoint, f"https://api.versium.com/v2/{endpoint}", headers=v_headers,
                        params=params, timeout=timeout, attempts=attempts, outcome=outcome)
    if resp is None:
        return None  # every attempt timed out or failed to connect (logged)
    try:
        body = resp.json()
    except ValueError:
        body = None
    v = body.get("versium") if isinstance(body, dict) and isinstance(body.get("versium"), dict) else {}
    errors = v.get("errors") or (body.get("errors") if isinstance(body, dict) else None)
    if resp.status_code != 200 or not isinstance(body, dict) or errors:
        log.error("Versium %s rejected (HTTP %s): %s", endpoint, resp.status_code,
                  _redact(errors if errors else resp.text))
        return None
    results = v.get("results") or []
    first = results[0] if isinstance(results, list) and results else {}
    return first if isinstance(first, dict) else {}


def parse_versium_contact(res: dict, email: str = "", phone: str = "") -> dict:
    """Alternate emails/phones (not the ones we sent) and the postal address from a contact answer."""
    email, phone = (email or "").strip().lower(), clean_phone(phone or "")
    emails, phones = [], []
    for k, v in (res or {}).items():
        key = versium_key(k)
        for val in (v if isinstance(v, list) else [v]):
            val = str(val or "").strip()
            if "email" in key and "@" in val and val.lower() != email and val.lower() not in emails:
                emails.append(val.lower())
            elif "phone" in key and clean_phone(val) and clean_phone(val) != phone and clean_phone(val) not in phones:
                phones.append(clean_phone(val))
    by = {versium_key(k): str(v or "").strip() for k, v in (res or {}).items() if not isinstance(v, list)}
    plus4 = by.get("zip_4", "").split("-")
    address = {"md_address1": by.get("postal_address") or by.get("address") or None,
               "md_city": by.get("city") or None, "md_state": by.get("state") or None,
               "zip5": (by.get("zip") or "")[:5] or None,
               "md_plus4": plus4[1] if len(plus4) > 1 and plus4[1] else None}
    return {"alt_emails": emails[:5], "alt_phones": phones[:5],
            "address": address if address["md_address1"] and (address["zip5"] or address["md_city"]) else None}


def parse_versium_demographics(res: dict) -> dict:
    """Lifestyle flags (snake_case, blocked categories removed) plus the three wealth/auto columns."""
    flags = strip_blocked_versium_fields(
        {versium_key(k): str(v).strip() for k, v in (res or {}).items()
         if v not in (None, "") and not isinstance(v, (dict, list)) and str(v).strip()})
    auto = " ".join(x for x in (flags.get("auto_make", ""), flags.get("auto_model", "").title()) if x)
    return {"lifestyle_flags": flags,
            "versium_net_worth": flags.get("estimated_net_worth") or flags.get("net_worth") or None,
            "versium_home_market_value": flags.get("home_market_value") or flags.get("home_value") or None,
            "versium_auto_make_model": auto or None}


def _norm_state(st) -> str:
    st = str(st or "").strip().upper()
    return "MI" if st == "MICHIGAN" else st


def versium_address_consistent(a: dict, zip5, state, city) -> bool:
    """A Versium address is used only if it doesn't contradict what we know: same ZIP when we have one,
    otherwise the same state and city."""
    if zip5:
        return not a.get("zip5") or a["zip5"] == str(zip5)[:5]
    if state and a.get("md_state") and _norm_state(a["md_state"]) != _norm_state(state):
        return False
    letters = lambda x: re.sub(r"[^a-z]", "", str(x or "").lower())
    if city and a.get("md_city") and letters(a["md_city"]) != letters(city):
        return False
    return True


def versium_checked(cust: dict) -> bool:
    """True once Versium has answered for this customer (live API or the CSV import), match or not."""
    flags = (cust or {}).get("versium_lifestyle_flags") or {}
    return bool(isinstance(flags, dict) and (flags.get("_versium_checked") or flags.get("_source")))


def _call_outcome(o: dict, answer) -> str:
    if answer is None:
        return f"failed:{o.get('status') or o.get('error') or 'unknown'}"
    return "answered" if answer else "no_match"


def run_versium_enrichment(result: dict, first_name: str, last_name: str, email: str, phone: str,
                           address: str, city: str, state: str, zip_code: str, known: dict,
                           started: float, online_budget: float = None, attempts: int = None) -> dict:
    """Versium contact (alt emails/phones; the address when we have no street), then demographics, then the
    online audience append, updating `result` in place. Geography gates use the best address known so far,
    including one Versium just found, so excluded or out-of-state people aren't enriched further."""
    known = known or {}
    gate = lambda: (result.get("md_city") or known.get("md_city"), result.get("md_state") or known.get("md_state"),
                    result.get("zip5") or known.get("zip5"))
    g_city, g_state, g_zip = gate()
    outcome = result.setdefault("_outcome", {})
    if is_detroit_excluded(g_city, g_zip):
        outcome["versium"] = "skipped_excluded_area"
        return result
    if not VERSIUM_API_KEY:
        if versium_checked(known):
            outcome["versium"] = "already"   # an answer already stored stays valid without the key
            return result
        if VERSIUM_REQUIRED:
            # Not an answer: enrichment can't finish, so held events wait (and the customer is retried) instead
            # of being judged on a score that's missing Versium's data.
            log.error("Versium enrichment skipped: VERSIUM_API_KEY is not set")
            result["_versium_failed"] = True
            outcome["versium"] = "failed:not_configured"
        return result
    already = versium_checked(known)  # answered before (live or CSV): never buy contact/demographics again
    kflags = known.get("versium_lifestyle_flags") if isinstance(known.get("versium_lifestyle_flags"), dict) else {}
    contact_done = already or bool(kflags.get("_versium_contact_done"))
    demo_done = already or bool(kflags.get("_versium_demo_done"))
    v_headers = {"x-versium-api-key": VERSIUM_API_KEY, "Accept": "application/json"}
    street = result.get("md_address1") or address or known.get("md_address1")
    v_input = {k: v for k, v in {
        "first": first_name, "last": last_name, "email": email, "phone": phone,
        "address": street, "city": result.get("md_city") or city or known.get("md_city"),
        "state": result.get("md_state") or state or known.get("md_state"),
        "zip": result.get("zip5") or zip_code or known.get("zip5"), "country": "US"}.items() if v}
    outputs = ["email_multiple", "phone_multiple"] + ([] if street else ["address"])
    oc = {}
    contact = None if contact_done else versium_call("contact", v_headers,
                                                     {**v_input, "output[]": outputs, "rcfg_max_emails": 5},
                                                     attempts=attempts, outcome=oc)
    outcome["versium_contact"] = "already" if contact_done else _call_outcome(oc, contact)
    if contact is None and not contact_done:
        result["_versium_failed"] = True
        result["_versium_contact_failed"] = True
    elif contact is not None:
        c = parse_versium_contact(contact, email, phone)
        result["alt_emails"], result["alt_phones"] = c["alt_emails"], c["alt_phones"]
        a = c["address"]
        # Only fill a missing address, and only when it agrees with the location we already have; then take
        # it whole, so street, city, state and ZIP always describe one place.
        if a and not street and versium_address_consistent(a, result.get("zip5") or known.get("zip5"),
                                                           result.get("md_state") or known.get("md_state"),
                                                           result.get("md_city") or known.get("md_city")):
            for k, v in a.items():
                if v:
                    result[k] = v
            result["versium_address"] = True
            v_input.update({k2: v for k2, v in (("address", result.get("md_address1")), ("city", result.get("md_city")),
                                                ("state", result.get("md_state")), ("zip", result.get("zip5"))) if v})
    g_city, g_state, g_zip = gate()
    if is_detroit_excluded(g_city, g_zip):
        return result  # Versium's address put them in an excluded area: no demographics or ad IDs
    od = {}
    demo = None if demo_done else versium_call("demographic", v_headers,
                                               {**v_input, "output[]": list(VERSIUM_DEMOGRAPHIC_OUTPUTS)},
                                               attempts=attempts, outcome=od)
    outcome["versium_demographic"] = "already" if demo_done else _call_outcome(od, demo)
    if demo is None and not demo_done:
        result["_versium_failed"] = True
        result["_versium_demo_failed"] = True
    elif demo is not None:
        d = parse_versium_demographics(demo)
        result.update({k: v for k, v in d.items() if k != "lifestyle_flags"})
        result["lifestyle_flags"] = d["lifestyle_flags"]
    marks = {}
    if contact is not None:
        marks["_versium_contact_done"] = date.today().isoformat()
    if demo is not None:
        marks["_versium_demo_done"] = date.today().isoformat()
    if not already and (contact is not None or contact_done) and (demo is not None or demo_done):
        # Both answered (now or earlier): neither webhooks nor the catch-up pay for this customer again.
        marks["_versium_checked"] = date.today().isoformat()
    if marks:
        result["lifestyle_flags"] = {**result.get("lifestyle_flags", {}), **marks}
    budget = VERSIUM_ONLINE_BUDGET_SECONDS if online_budget is None else online_budget
    if not known.get("versium_online_audience") and versium_online_append_eligible(g_city, g_state, g_zip) and \
            time.monotonic() - started < budget:
        oo = {}
        try:
            oa = fetch_versium_online_audience(v_headers, v_input, attempts=attempts, outcome=oo)
            result["online_audience"] = oa or {}
            outcome["versium_online_audience"] = _call_outcome(oo, oa)
        except Exception as e:
            log.warning("Versium onlineAudience lookup failed: %s", type(e).__name__)
            outcome["versium_online_audience"] = f"failed:{type(e).__name__}"
    return result


def run_melissa_and_versium_waterfall(first_name: str, last_name: str, email: str, phone: str,
                                      address: str = "", city: str = "", state: str = "", zip_code: str = "",
                                      known: dict = None, unhurried: bool = False) -> dict:
    """Step 1: Melissa Personator (Address, Lat/Long, Demographics). Step 2: Versium REACH (Alt Emails/Phones, Wealth).

    `md_results` is only set when Melissa actually answered. It stays None when Melissa is not
    configured or the call failed, so the customer is tried again later instead of being written off.

    `known` is the customer's stored record (md_city / md_state / zip5). It is only used by the Versium
    geography gates, as a fallback after Melissa's answer and the incoming fields, so a webhook that
    arrives without an address can't make a known Detroit or out-of-state customer look "unknown".
    """
    zip_code = (zip_code or "")[:5]
    started = time.monotonic()
    result = {
        "alt_emails": [],
        "alt_phones": [],
        "lifestyle_flags": {},
        "online_audience": {},
        # What we were given, so the geo gate still works if Melissa is unavailable.
        "md_city": city or None,
        "md_state": state or None,
        "zip5": zip_code or None,
        "md_results": None,
    }
    if not any([email, phone, address, (first_name and last_name and zip_code)]):
        return result

    # --- STEP 1: MELISSA PERSONATOR (only when MELISSA_LIVE) ---
    # Inside a webhook (ENRICH_DELAY_MINUTES=0) someone is waiting, so one attempt per call; the deferred worker
    # retries transient failures.
    attempts = PROVIDER_ATTEMPTS if unhurried else 1
    if MELISSA_LIVE and not (known or {}).get("md_mik"):  # not twice: the webhook may have run it already
        mo = {}
        result.update(melissa_lookup(first_name, last_name, email, phone, address, city, state, zip_code,
                                     outcome=mo, attempts=attempts))
        result.setdefault("_outcome", {})["melissa"] = mo.get("result")

    if address and not result.get("md_address1"):
        result["md_address1"] = address.strip()  # the customer's own street (Melissa off or no answer)

    # --- STEP 2: VERSIUM REACH: alt contacts, the address when there's no street, demographics, ad IDs ---
    # unhurried (the deferred worker): nobody is waiting, so no step is skipped for time.
    run_versium_enrichment(result, first_name, last_name, email, phone, address, city, state, zip_code,
                           known, started, online_budget=float("inf") if unhurried else None, attempts=attempts)

    # --- STEP 3: US CENSUS GEOCODER (free): census/school fields, coordinates + distance when none ---
    street = result.get("md_address1") or address
    if street and not result.get("md_census_tract") and \
            (unhurried or time.monotonic() - started < CENSUS_BUDGET_SECONDS):
        census = census_geocode(street, result.get("md_city") or city,
                                result.get("md_state") or state, result.get("zip5") or zip_code)
        apply_census(result, census or {})
        if census is not None:
            result["census_checked_at"] = datetime.now(timezone.utc).isoformat()

    # With Melissa off, a finished lookup is marked so it isn't retried; a failed Versium call leaves
    # md_results empty, which every caller already treats as "try again later".
    if not MELISSA_LIVE and result.get("md_results") is None and not result.get("_versium_failed"):
        result["md_results"] = MELISSA_OFF
    return result


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

# Personas are first-match labels for reporting. The score is 0-100, recomputed from current facts every time
# (it can go down), and is the best of three goal propensities: behaviour (proven money, shared by all three)
# plus that goal's fit. Behaviour leads because it covers everyone and predicts far better than demographics.
# Verified against the customer base on 2026-11-19 (spend-correlators-2026-11-19.md).
PERSONA_CORPORATE = "PG-1: Corporate & Group Organisers"
PERSONA_BIRTHDAY = "PG-2: Birthday Party Families"
PERSONA_CELEBRATION = "PG-3: High-Value Open Bowlers / Celebrations"
PERSONA_EXCLUDED = "PG-4: Excluded Geo (Detroit / Southfield / Downriver)"
PERSONA_NOT_ENOUGH_DATA = "PG-5: Not Enough Data Yet"

GOALS = ("birthday", "corporate", "celebration")
TOP_QUARTER_SPEND = 279.0   # lifetime spend at the top quarter of spenders
HIGH_SPEND = 600.0
RECENT_DAYS = 90
# Words that make an account named after its contact still an organisation ("Noonan Electrical Services").
_ORG_WORDS = re.compile(
    r"\b(inc|llc|ltd|corp|corporation|company|co|group|services?|foundation|church|school|academy|"
    r"association|club|team|league|center|centre|council|society|university|college|hospital|clinic|"
    r"dental|law|bank|insurance|realty|construction|electric(al)?|plumbing|consulting|solutions|"
    r"partners|agency|department|dept|city|township|ministries|pta|pto)\b", re.I)


def _letters(v) -> str:
    return re.sub(r"[^a-z]", "", str(v or "").lower())


def _band_floor(v) -> Optional[float]:
    """Lower bound of a band like '$150,000-199,999', '$150,001+', '> $250,000', '< $20,000' or a raw number."""
    s = str(v or "").strip()
    if not s:
        return None
    if s.startswith("<"):
        return 0.0
    m = re.search(r"\d[\d,]*(\.\d+)?", s)
    return float(m.group(0).replace(",", "")) if m else None


def is_real_organisation(cust: dict) -> bool:
    """A Tripleseat account that is an organisation, not the contact's own name, or a business mailing address."""
    if cust.get("md_delivery_indicator") == "B":
        return True
    acct = str(cust.get("ts_account") or "").strip()
    a = _letters(acct)
    if len(a) < 2:
        return False
    fn, ln = _letters(cust.get("first_name")), _letters(cust.get("last_name"))
    personal = (fn and fn in a) or (ln and ln in a) or a in (fn + ln, ln + fn)
    return not personal or bool(_ORG_WORDS.search(acct))


def ts_confirmed_booker(cust: dict, facts: Optional[dict] = None) -> bool:
    """A DEFINITE/CLOSED Tripleseat booking: the customer's status (a later inquiry doesn't downgrade it, see
    UPSERT_UPDATE_RULES) or any booked event/booking in the API-synced Tripleseat tables."""
    return str(cust.get("ts_lead_status") or "").upper() in TRIPLESEAT_BOOKED_STATUSES or \
        bool((facts or {}).get("ts_booked"))


def is_party_booker(cust: dict, facts: Optional[dict] = None) -> bool:
    """A Partywirks booking, or a DEFINITE/CLOSED Tripleseat event."""
    return int(cust.get("pw_bookings") or 0) > 0 or ts_confirmed_booker(cust, facts)


def _fit_facts(cust: dict) -> dict:
    """Demographic facts, Versium first and Melissa as fallback. Never ethnicity, religion, politics or credit."""
    vf = cust.get("versium_lifestyle_flags") or {}
    if isinstance(vf, str):
        try:
            vf = json.loads(vf)
        except ValueError:
            vf = {}
    age = str(vf.get("age_range") or "")
    income = _band_floor(vf.get("household_income"))
    if income is None:
        income = _band_floor(cust.get("md_household_income"))
    home = _band_floor(vf.get("home_value"))
    if home is None:
        home = _band_floor(cust.get("versium_home_market_value"))
    lor = vf.get("length_of_residence")
    if lor:
        tenure = lor in ("5-10 Years", "11-14 Years")
    else:
        lo = _band_floor(cust.get("md_length_of_residence"))
        tenure = lo is not None and 5 <= lo <= 11  # Melissa bands "5-6 years" .. "11-12 years"
    kids = str(cust.get("md_children_age_range") or "")
    dist = cust.get("distance_miles")
    dist = float(dist) if dist is not None else None
    return {
        "age_35_55": age in ("35-45", "45-55"),
        "age_35_65": age in ("35-45", "45-55", "55-65"),
        "affluent": (income or 0) >= 150000 or (home or 0) >= 450000,
        "tenure_5_14": tenure,
        "kids_3_10": "3-5" in kids or "6-10" in kids,
        "female": vf.get("gender") == "Female",
        "married": vf.get("marital_status") == "Married",
        "household_2": str(vf.get("household_size") or "") == "2",
        "young_adult": vf.get("young_adult_in_household") == "Yes",
        "within_5": dist is not None and dist <= 5,
        "within_12": dist is not None and dist <= 12,
        "within_15": dist is not None and dist <= 15,
        "org": is_real_organisation(cust),
    }


def _near(f: dict, five: int, twelve: int, fifteen: int = 0) -> int:
    return five if f["within_5"] else twelve if f["within_12"] else fifteen if f["within_15"] else 0


def goal_fits(cust: dict) -> dict:
    """Each goal's fit points (added to the shared behaviour points)."""
    f = _fit_facts(cust)
    birthday = (10 * f["age_35_55"] + 10 * f["kids_3_10"] + 5 * f["affluent"] + 5 * f["tenure_5_14"]
                + 5 * f["female"] + _near(f, 10, 5) + 5 * f["married"] + 5 * f["household_2"]
                - 10 * f["young_adult"])
    corporate = 15 * f["org"] + 10 * f["age_35_65"] + 5 * f["female"] + _near(f, 10, 5, 2) + 5 * f["married"]
    celebration = (_near(f, 10, 5) + 5 * f["age_35_55"] + 5 * f["affluent"] + 5 * f["tenure_5_14"]
                   + 5 * f["household_2"] + 5 * f["married"])
    return {"birthday": birthday, "corporate": corporate, "celebration": celebration}


def behaviour_points(cust: dict, facts: dict) -> int:
    """Proven money. `facts`: visit_days (separate days with a payment) and recent (bought/booked in 90 days)."""
    spend = float(cust.get("true_lifetime_spend") or 0)
    return (25 * is_party_booker(cust, facts)
            + (20 if spend >= HIGH_SPEND else 15 if spend >= TOP_QUARTER_SPEND else 0)
            + 10 * bool(facts.get("recent"))
            + 5 * (int(facts.get("visit_days") or 0) >= 3))


def assign_persona(cust: dict, facts: dict) -> str:
    f = _fit_facts(cust)
    if f["org"] or ts_confirmed_booker(cust, facts):
        return PERSONA_CORPORATE
    if int(cust.get("pw_bookings") or 0) > 0 or \
            (f["kids_3_10"] and f["age_35_55"] and f["within_12"] and f["affluent"]):
        return PERSONA_BIRTHDAY
    if float(cust.get("true_lifetime_spend") or 0) >= TOP_QUARTER_SPEND or \
            (int(facts.get("visit_days") or 0) >= 3 and f["within_15"]):
        return PERSONA_CELEBRATION
    if is_detroit_excluded(cust.get("md_city"), cust.get("zip5")):
        return PERSONA_EXCLUDED
    return PERSONA_NOT_ENOUGH_DATA


_PERSONA_GOAL = {PERSONA_CORPORATE: "corporate", PERSONA_BIRTHDAY: "birthday"}


def winning_goal(cust: dict, persona: Optional[str] = None) -> str:
    """The goal with the best fit (behaviour is shared, so it doesn't change the winner). Ties go to the
    persona's own goal, then celebration."""
    fits = goal_fits(cust)
    best = max(fits.values())
    persona = persona if persona is not None else (cust.get("pg_mosaic_persona") or "")
    preferred = [g for p, g in _PERSONA_GOAL.items() if persona.startswith(p[:4])]
    for g in preferred + ["celebration", "birthday", "corporate"]:
        if fits[g] == best:
            return g
    return "celebration"


def score_and_assign_persona(cust: dict, facts: Optional[dict] = None) -> tuple:
    """(persona, score). No free base: no behaviour and no fit scores 0."""
    facts = facts or {}
    behaviour = behaviour_points(cust, facts)
    score = max(behaviour + fit for fit in goal_fits(cust).values())
    return assign_persona(cust, facts), max(0, min(100, score))


def _recent_date(v) -> bool:
    d = ts_date(v) if isinstance(v, str) else (v.isoformat() if isinstance(v, (date, datetime)) else None)
    if not d:
        return False
    try:
        return (date.fromisoformat(d[:10]) - date.today()).days >= -RECENT_DAYS
    except ValueError:
        return False


# Visit days and recency from Square payments only: those carry the real payment date. Partywirks/Tripleseat
# ledger rows carry the date they were first recorded, and WebRes rows from the Nov 16-17 backfill their import
# date (and a WebRes booking's money also arrives as a Square payment). Several payments on one day (deposit +
# balance, split checks) are one visit.
_FACTS_SQL = """
    SELECT COUNT(DISTINCT u.transaction_date::date) FILTER (
               WHERE u.total_amount > 0 AND u.source_system = 'square') AS visit_days,
           COALESCE(BOOL_OR(u.total_amount > 0 AND u.source_system = 'square'
                            AND u.transaction_date >= NOW() - make_interval(days => %(days)s)), FALSE) AS recent_paid,
           EXISTS (SELECT 1 FROM tripleseat_events e WHERE e.cluster_id = %(cid)s
                     AND UPPER(COALESCE(e.status, '')) IN ('DEFINITE', 'CLOSED'))
             OR EXISTS (SELECT 1 FROM tripleseat_bookings b WHERE b.cluster_id = %(cid)s
                          AND UPPER(COALESCE(b.status, '')) IN ('DEFINITE', 'CLOSED')) AS ts_booked
    FROM unified_transactions u WHERE u.cluster_id = %(cid)s;"""


def customer_facts(cur, cust: dict) -> dict:
    """Behaviour facts: visit days and payment recency from the ledger, and booked Tripleseat records. Bookings'
    recency comes from their own date (reservation / party / event, past 90 days or upcoming)."""
    cur.execute(_FACTS_SQL, {"days": RECENT_DAYS, "cid": cust["cluster_id"]})
    row = cur.fetchone() or {}
    facts = {"visit_days": int(row.get("visit_days") or 0), "ts_booked": bool(row.get("ts_booked"))}
    facts["recent"] = bool(row.get("recent_paid")) or _recent_date(cust.get("wr_last_reservation_date")) or \
        _recent_date(cust.get("pw_last_party_date")) or \
        (ts_confirmed_booker(cust, facts) and _recent_date(cust.get("ts_last_event_date")))
    return facts


def rescore_customer(cur, cust: dict, exact: bool = True) -> dict:
    """Recomputes persona & score from the customer's current facts. Scores go down as well as up
    (`exact` is kept for callers; there is no ratchet any more)."""
    persona, score = score_and_assign_persona(cust, customer_facts(cur, cust))
    cur.execute("UPDATE master_customers SET pg_mosaic_persona = %(persona)s, bowler_value_score = %(score)s "
                "WHERE cluster_id = %(cid)s RETURNING *;",
                {"persona": persona, "score": score, "cid": cust["cluster_id"]})
    return cur.fetchone()


# ---------------------------------------------------------------------------
# Meta
# ---------------------------------------------------------------------------

def build_meta_user_identifiers(cust: dict) -> dict:
    """Primary + Versium alternate emails & phones, formatted the way Meta expects before hashing."""
    emails = [cust.get("email")] + list(cust.get("versium_alt_emails") or [])
    phones = [cust.get("phone")] + list(cust.get("versium_alt_phones") or [])
    hashed_emails = sorted({sha256_hash(e) for e in emails if e})
    hashed_phones = sorted({sha256_hash(meta_phone(p)) for p in phones if meta_phone(p)})
    v_em, v_ph = versium_meta_hashes(cust)
    hashed_emails = sorted(set(hashed_emails) | set(v_em))
    hashed_phones = sorted(set(hashed_phones) | set(v_ph))
    city = meta_city(cust.get("md_city"))
    state = meta_state(cust.get("md_state"))

    user_data = {
        "em": hashed_emails,
        "ph": hashed_phones,
        "fn": [sha256_hash(cust.get("first_name"))] if cust.get("first_name") else [],
        "ln": [sha256_hash(cust.get("last_name"))] if cust.get("last_name") else [],
        "ct": [sha256_hash(city)] if city else [],
        "st": [sha256_hash(state)] if state else [],
        "zp": [sha256_hash(cust.get("zip5"))] if cust.get("zip5") else [],
        "country": [sha256_hash("us")],
        "external_id": [sha256_hash(str(cust["cluster_id"]))],
    }
    return {k: v for k, v in user_data.items() if v}


AUDIENCE_SCHEMA = ["EMAIL", "PHONE", "FN", "LN", "CT", "ST", "ZIP", "COUNTRY", "LOOKALIKE_VALUE"]


def audience_names() -> dict:
    return {AUDIENCE_ALL_NONDETROIT: "Master (All Non-Detroit)", AUDIENCE_PG1_CORP: "Corporate",
            AUDIENCE_PG2_FAMILIES: "Birthday", AUDIENCE_PRIME_SUBURBS: "Prime Suburbs",
            **({AUDIENCE_BROAD_LOCAL: "Broad Local"} if AUDIENCE_BROAD_LOCAL else {})}


def seed_memberships(cust: dict, facts: Optional[dict] = None) -> dict:
    """Which goal seeds a customer belongs in. Built from who the customer is (overlapping, so a corporate
    organiser with young kids is in both), never from the score cutoff or distance: the ad set's radius does the
    geography, and a party-age family 20 miles away still teaches Meta what a birthday family looks like.
    Never reads the stored persona label."""
    f = _fit_facts(cust)
    spend = float(cust.get("true_lifetime_spend") or 0)
    return {
        "corporate": f["org"] or ts_confirmed_booker(cust, facts),
        "birthday": int(cust.get("pw_bookings") or 0) > 0 or (f["kids_3_10"] and f["age_35_55"] and f["affluent"]),
        # Broad Local: the old celebration seed (top-quarter spend or 3+ visit days) plus the demographic gate.
        "broad_local": spend >= TOP_QUARTER_SPEND or int((facts or {}).get("visit_days") or 0) >= 3
                       or (f["age_35_55"] and f["affluent"]),
    }


def audience_targets(cust: dict, facts: Optional[dict] = None) -> tuple:
    """Which audiences a customer belongs in, plus the LTV sent as LOOKALIKE_VALUE: their real lifetime spend.
    Excluded-area customers belong in none."""
    if is_detroit_excluded(cust.get("md_city"), cust.get("zip5")):
        return [], 0.0
    ltv = float(cust.get("true_lifetime_spend") or 0)
    seeds = seed_memberships(cust, facts)
    targets = [AUDIENCE_ALL_NONDETROIT]
    if seeds["corporate"]:
        targets.append(AUDIENCE_PG1_CORP)
    if seeds["birthday"]:
        targets.append(AUDIENCE_PG2_FAMILIES)
    if seeds["broad_local"]:
        targets.append(AUDIENCE_BROAD_LOCAL)
    if (cust.get("zip5") or "") in PRIME_SUBURB_ZIPS:
        targets.append(AUDIENCE_PRIME_SUBURBS)
    return [a for a in dict.fromkeys(targets) if a], ltv


# Extra rows per customer beyond the first (each carries one more email and/or phone).
AUDIENCE_EXTRA_ROWS_MAX = 10


def audience_rows(cust: dict, ltv: float) -> list:
    """The customer's audience rows. The upload schema holds one email and one phone per row, so every other
    known identifier goes on an extra row with the same name/city/state/zip and LOOKALIKE_VALUE; Meta matches
    each row to the same person. Extras: Versium's alternate emails/phones and its Facebook-linked emails/phones
    (already SHA-256, sent as they are), the same identifiers the CAPI events carry (build_meta_user_identifiers).
    The first row is always the customer's own email and phone."""
    first_email = sha256_hash(cust.get("email"))
    first_phone = sha256_hash(meta_phone(cust.get("phone")))
    v_em, v_ph = versium_meta_hashes(cust)
    emails = [sha256_hash(e) for e in cust.get("versium_alt_emails") or [] if e] + v_em
    phones = [sha256_hash(meta_phone(p)) for p in cust.get("versium_alt_phones") or [] if meta_phone(p)] + v_ph
    emails = [e for e in dict.fromkeys(emails) if e and e != first_email]
    phones = [p for p in dict.fromkeys(phones) if p and p != first_phone]
    rest = [
        sha256_hash(cust.get("first_name")),
        sha256_hash(cust.get("last_name")),
        sha256_hash(meta_city(cust.get("md_city"))),
        sha256_hash(meta_state(cust.get("md_state"))),
        sha256_hash(cust.get("zip5")),
        sha256_hash("us"),
        round(ltv, 2),
    ]
    rows = [[first_email, first_phone] + rest]
    for i in range(min(max(len(emails), len(phones)), AUDIENCE_EXTRA_ROWS_MAX)):
        rows.append([emails[i] if i < len(emails) else "", phones[i] if i < len(phones) else ""] + rest)
    return rows


def upload_audience_rows(aud_id: str, rows: list) -> dict:
    """Uploads hashed rows (max 10,000 per call) to one audience. Logs and returns Meta's answer,
    so a rejected upload is never silent."""
    label = audience_names().get(aud_id, aud_id)
    try:
        resp = requests.post(
            f"https://graph.facebook.com/v21.0/{aud_id}/users",
            params={"access_token": META_ACCESS_TOKEN},
            json={"payload": {"schema": AUDIENCE_SCHEMA, "data": rows}},
            timeout=30 if len(rows) > 1 else 5,
        )
    except Exception as e:
        log.error("Meta audience upload to %s failed: %s", label, type(e).__name__)
        return {"ok": False, "error": type(e).__name__}
    try:
        body = resp.json()
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    ok = resp.status_code == 200 and "error" not in body
    if not ok:
        err = body.get("error") if isinstance(body, dict) else None
        msg = (err.get("message") if isinstance(err, dict) else err) or resp.text[:300]
        log.error("Meta audience upload to %s rejected (HTTP %s): %s", label, resp.status_code, msg)
        return {"ok": False, "status": resp.status_code, "error": msg}
    return {"ok": True, "received": body.get("num_received"), "invalid": body.get("num_invalid_entries")}


def sync_customer_to_meta_custom_audiences(cust: dict, facts: Optional[dict] = None):
    """Pushes one non-excluded customer into the Meta value-based Custom Audiences they belong in (any score).
    Reads the customer's visit/booking facts (seed rules use them) when the caller doesn't pass them."""
    if not META_ACCESS_TOKEN or cust.get("enrich_after"):
        return  # a customer waiting for enrichment is synced by the worker once it's done
    if facts is None:
        try:
            conn = get_conn()
            try:
                with conn, conn.cursor() as cur:
                    facts = customer_facts(cur, cust)
            finally:
                conn.close()
        except Exception as e:
            log.warning("Audience sync could not read customer facts: %s", type(e).__name__)
    targets, ltv = audience_targets(cust, facts)
    rows = audience_rows(cust, ltv)
    for aud_id in targets:
        upload_audience_rows(aud_id, rows)


# --- Event time --------------------------------------------------------------------------------------------
# Meta attributes an event by its event_time, so it's the moment the purchase/booking/lead happened, not when we
# send it (after the enrichment hold, or a retry hours later). Meta only accepts events up to 7 days old.
META_MAX_EVENT_AGE = timedelta(days=7)
# Partywirks sends local times without an offset. Checked 2026-11-20 against production: for the six orders that
# had sent a QB, the API's created_at (read as UTC) was 5.03-16.8 h before our first record of the order
# (unified_transactions.recorded_at). The smallest gap, 5.03 h, is the EST offset (UTC-5) plus ~2 minutes of
# webhook delay, i.e. created_at is venue local time; the longer gaps are orders first recorded by the nightly
# catch-up sync. Six samples, one of them real-time: re-check after deploy (see PR #33).
VENUE_TZ = ZoneInfo("America/Detroit")


def parse_source_time(val, naive_tz=timezone.utc) -> Optional[datetime]:
    """A source system's timestamp as an aware datetime: ISO 8601 (with or without offset/'Z'),
    'YYYY-MM-DD HH:MM:SS', or epoch seconds. A time without an offset is read in naive_tz. A plain date
    (no time) is not a transaction time and returns None."""
    if val is None or val == "":
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=naive_tz)
    if isinstance(val, (int, float)):
        return datetime.fromtimestamp(float(val), tz=timezone.utc)
    v = str(val).strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}", v):
        return None
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00").replace(" ", "T", 1))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=naive_tz)


def meta_event_time(txn_time: Optional[datetime], now: Optional[datetime] = None) -> datetime:
    """The event_time to send: the transaction time when known, never in the future (clamped to now), and now
    when unknown or older than Meta accepts (e.g. an old Tripleseat record whose status changed today: we learn
    of the conversion now)."""
    now = now or datetime.now(timezone.utc)
    if not txn_time or txn_time > now:
        return now
    if txn_time < now - META_MAX_EVENT_AGE + timedelta(minutes=10):
        return now
    return txn_time


# --- Browser identifiers (fbp / fbc / user agent) ----------------------------------------------------------
# Only one source passes them: the Tripleseat lead form on myperfectgame.com (/groups-and-parties), whose page
# script copies the _fbp/_fbc cookies (or builds fbc from an fbclid in the URL, Meta's documented format) and the
# browser's user agent into hidden custom fields (site-snippets/tripleseat_lead_form_meta_ids.html). Each is
# stored on the customer with its own capture time (a later visit without an ad click keeps the stored fbc) and
# sent only on that customer's Tripleseat events (qb_ts_ / lead_ts_), only while younger than BROWSER_IDS_MAX_AGE. Square, WebRes and Partywirks
# carry no browser identifiers (in-store / Gmail / server-to-server), so their events send none. No source gives
# the client IP address: it's never sent.
BROWSER_IDS_MAX_AGE = timedelta(days=90)
_FBP_RE = re.compile(r"^fb\.[0-2]\.\d{10,14}\.\d{1,20}$")
_FBC_RE = re.compile(r"^fb\.[0-2]\.\d{10,14}\.[A-Za-z0-9_\-]{10,500}$")
_BROWSER_FIELD_NAMES = {"fbp": "fbp", "_fbp": "fbp", "meta_fbp": "fbp", "fbc": "fbc", "_fbc": "fbc",
                        "meta_fbc": "fbc", "user_agent": "ua", "useragent": "ua", "client_user_agent": "ua"}


def ts_browser_ids(obj: dict) -> dict:
    """fbp / fbc / user agent from a Tripleseat record's custom fields, only values in Meta's formats."""
    found = {}
    fields = obj.get("custom_fields") if isinstance(obj, dict) else None
    items = []
    if isinstance(fields, dict):
        items = list(fields.items())
    elif isinstance(fields, list):
        for f in fields:
            if isinstance(f, dict):
                name = f.get("custom_field_slug") or f.get("slug") or f.get("custom_field_name") or f.get("name")
                items.append((name, f.get("value")))
    for name, val in items:
        key = _BROWSER_FIELD_NAMES.get(re.sub(r"[^a-z_]", "", str(name or "").strip().lower().replace(" ", "_")))
        val = as_text(val)
        val = val.strip() if val else ""
        if not key or not val:
            continue
        if (key == "fbp" and _FBP_RE.match(val)) or (key == "fbc" and _FBC_RE.match(val)) or \
                (key == "ua" and 10 <= len(val) <= 1000):
            found[key] = val
    return found


_BROWSER_COLUMNS = {"fbp": "meta_fbp", "fbc": "meta_fbc", "ua": "meta_client_user_agent"}


def save_browser_ids(cur, cluster_id, ids: dict, captured_at: Optional[datetime]):
    """Stores each identifier the capture has, with its own capture time. Per field, the newest capture wins: an
    older capture never replaces a newer value, and a field the capture lacks (a visit not from an ad click has
    no fbc) keeps its stored value and its original capture time, so it still ages out on schedule."""
    if not cluster_id or not ids:
        return
    when = min(captured_at or datetime.now(timezone.utc), datetime.now(timezone.utc))
    for key, col in _BROWSER_COLUMNS.items():
        if not ids.get(key):
            continue
        cur.execute(f"""UPDATE master_customers SET {col} = %s, {col}_at = %s
                        WHERE cluster_id = %s AND ({col}_at IS NULL OR {col}_at <= %s);""",
                    (ids[key], when, cluster_id, when))


def browser_user_data(cust: dict, event_id: str) -> dict:
    """The fbp / fbc / client_user_agent to add to this event's user_data: only Tripleseat events, only a
    capture younger than BROWSER_IDS_MAX_AGE, and only fields that are known (never empty strings)."""
    if not (event_id.startswith("qb_ts_") or event_id.startswith("lead_ts_")):
        return {}
    oldest = datetime.now(timezone.utc) - BROWSER_IDS_MAX_AGE
    out = {}
    for meta_key, col in (("fbp", "meta_fbp"), ("fbc", "meta_fbc"), ("client_user_agent", "meta_client_user_agent")):
        at = cust.get(f"{col}_at")
        if cust.get(col) and at and at >= oldest:   # each value by its own capture time
            out[meta_key] = cust[col]
    return out


def fire_meta_capi_event(cur, cust: dict, event_name: str, value: float, threshold: int,
                         event_id: str, action_source: str = "system_generated",
                         event_time: Optional[datetime] = None):
    """Sends one CAPI event, at most once per event_id.

    A Meta error or timeout is logged with sent_to_meta = FALSE and never raises, so it can't roll back
    the customer update. If the same event comes through again later, it's retried.

    event_time is when the transaction happened (see meta_event_time). It's stored with the event the first
    time it's logged, and a retry or a release after the enrichment hold reuses the stored time.
    """
    if not META_PIXEL_ID or not META_ACCESS_TOKEN:
        return
    cur.execute("SELECT sent_to_meta, event_time FROM capi_events_log WHERE event_id = %s;", (event_id,))
    prior = cur.fetchone()
    if prior and prior["sent_to_meta"]:
        return
    # A stored time is reused (retries, release after the enrichment hold) while Meta still accepts it. One that
    # aged past Meta's 7 days while waiting (a hold or retry near the limit) goes through the same rule as a new
    # event: it becomes now, when it's actually sent, instead of being rejected and then never retried.
    stored = (prior or {}).get("event_time")
    stamped = meta_event_time(stored) if stored else meta_event_time(event_time)
    if stored and stamped != stored:
        log.info("Meta event %s: stored event_time is past Meta's 7-day limit; sending it with the send time",
                 event_id[:12])
    twin, dup_status = purchase_twin(cur, cust["cluster_id"], event_name, event_id, value), "DUPLICATE_PURCHASE"
    if not twin:
        twin, dup_status = party_qb_twin(cur, cust["cluster_id"], event_name, event_id), "DUPLICATE_QUALIFIED_BOWLER"
    if twin:
        # The same money (Purchase) or the same party (Qualified_Bowler) already went, or is going, to Meta
        # under the other source's event ID. Logged, pointing at that event, and never retried.
        cur.execute("""INSERT INTO capi_events_log (event_id, cluster_id, event_name, event_value, bowler_value_score,
                           threshold_at_send, sent_to_meta, meta_response_status, meta_response_body, event_time)
                       VALUES (%s, %s, %s, %s, %s, %s, FALSE, %s, %s, %s)
                       ON CONFLICT (event_id) DO UPDATE SET meta_response_status = EXCLUDED.meta_response_status,
                           meta_response_body = EXCLUDED.meta_response_body,
                           event_time = COALESCE(capi_events_log.event_time, EXCLUDED.event_time)
                       WHERE NOT capi_events_log.sent_to_meta;""",
                    (event_id, cust["cluster_id"], event_name, value, int(cust.get("bowler_value_score") or 0),
                     threshold, dup_status, twin, stamped))
        return
    if cust.get("enrich_after"):
        # Waiting for enrichment: hold the event until the address (Detroit gate) and score are known.
        cur.execute("""INSERT INTO capi_events_log (event_id, cluster_id, event_name, event_value, bowler_value_score,
                           threshold_at_send, sent_to_meta, meta_response_status, meta_response_body, event_time)
                       VALUES (%s, %s, %s, %s, %s, %s, FALSE, 'DEFERRED_ENRICHMENT', %s, %s)
                       ON CONFLICT (event_id) DO NOTHING;""",
                    (event_id, cust["cluster_id"], event_name, value, int(cust.get("bowler_value_score") or 0),
                     threshold, action_source, stamped))
        return

    cluster_id = cust["cluster_id"]
    score = int(cust.get("bowler_value_score") or 0)
    payload = {
        "data": [{
            "event_name": event_name,
            "event_time": int(stamped.timestamp()),
            "event_id": event_id,
            "action_source": action_source,
            "user_data": {**build_meta_user_identifiers(cust), **browser_user_data(cust, event_id)},
            "custom_data": {
                "pg_persona": cust.get("pg_mosaic_persona"),
                "bowler_value_score": score,
            },
        }]
    }
    if event_name == "Qualified_Bowler":
        # Which goal won the score (birthday / corporate / celebration), for per-goal custom conversions.
        payload["data"][0]["custom_data"]["qb_goal"] = winning_goal(cust)
    if value and float(value) > 0:
        # Only real money for this transaction. No money known = no value, never a made-up estimate:
        # Meta treats value as revenue in bidding and in reporting.
        payload["data"][0]["custom_data"].update(currency="USD", value=round(float(value), 2))
    try:
        resp = requests.post(
            f"https://graph.facebook.com/v21.0/{META_PIXEL_ID}/events",
            params={"access_token": META_ACCESS_TOKEN},
            json=payload,
            timeout=5,
        )
        status, body, ok = str(resp.status_code), resp.text[:1000], 200 <= resp.status_code < 300
    except Exception as e:
        status, body, ok = "NETWORK_ERROR", type(e).__name__, False

    cur.execute(
        """
        INSERT INTO capi_events_log (
            event_id, cluster_id, event_name, event_value, bowler_value_score,
            threshold_at_send, sent_to_meta, meta_response_status, meta_response_body, event_time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO UPDATE SET
            sent_at = NOW(),
            sent_to_meta = EXCLUDED.sent_to_meta,
            meta_response_status = EXCLUDED.meta_response_status,
            meta_response_body = EXCLUDED.meta_response_body,
            event_time = COALESCE(capi_events_log.event_time, EXCLUDED.event_time)
        WHERE capi_events_log.sent_to_meta = FALSE;
        """,
        (event_id, cluster_id, event_name, value, score, threshold, ok, status, body, stamped),
    )


# Held for enrichment, dropped once enrichment showed the customer doesn't qualify, the other half of a
# WebRes + Square pair for the same money, or the second Qualified_Bowler for one party: never retried as failures.
_NOT_HELD = ("COALESCE(meta_response_status, '') NOT IN "
             "('DEFERRED_ENRICHMENT', 'DROPPED_AFTER_ENRICHMENT', 'DUPLICATE_PURCHASE', 'DUPLICATE_QUALIFIED_BOWLER')")

# A WebRes booking is paid through Square online, so the same money arrives twice: the Square payment first,
# then the WebRes booking (~1-6 minutes later, from the Gmail script), under different event IDs (sq_pay_ / wr_)
# that Meta can't dedupe. The Square payment is the source of truth for the money (it's the actual charge and
# carries refunds). A WebRes Purchase is skipped when the same customer has a Square Purchase of the same amount
# recorded within PURCHASE_TWIN_HOURS of the booking (sent, failed and being retried, or held for enrichment). A Square Purchase is only
# skipped when a matching WebRes Purchase already reached Meta (the rare reverse order). A WebRes booking whose
# Square payment couldn't be tied to the customer still sends its Purchase: it's the only one Meta gets.
PURCHASE_TWIN_HOURS = 6


def purchase_twin(cur, cluster_id, event_name: str, event_id: str, value) -> Optional[str]:
    """The event ID of the other source's Purchase for the same money, if there is one.

    Times are when each payment/booking was first recorded (unified_transactions.recorded_at), never when an
    event was (re)sent, so a retry days later pairs with the same twin it would have paired with at first.
    A payment pairs with at most one booking: one already claimed by another skipped copy isn't reused."""
    if event_name != "Purchase" or not cluster_id:
        return None
    if event_id.startswith("wr_"):
        other, need_sent = "sq\\_pay\\_%", False
    elif event_id.startswith("sq_pay_"):
        other, need_sent = "wr\\_%", True
    else:
        return None
    cur.execute("""SELECT l.event_id
                   FROM capi_events_log l
                   JOIN unified_transactions o ON o.transaction_id = l.event_id
                   JOIN unified_transactions me ON me.transaction_id = %(eid)s
                   WHERE l.cluster_id = %(cid)s AND l.event_name = 'Purchase' AND l.event_id LIKE %(other)s
                     AND l.event_value = ROUND(%(value)s::numeric, 2)
                     AND COALESCE(l.meta_response_status, '') NOT IN ('DROPPED_AFTER_ENRICHMENT', 'DUPLICATE_PURCHASE')
                     AND (l.sent_to_meta OR NOT %(need_sent)s)
                     AND o.recorded_at BETWEEN me.recorded_at - make_interval(hours => %(hours)s)
                                           AND me.recorded_at + make_interval(hours => %(hours)s)
                     AND NOT EXISTS (SELECT 1 FROM capi_events_log d
                                     WHERE d.meta_response_status = 'DUPLICATE_PURCHASE'
                                       AND d.meta_response_body = l.event_id AND d.event_id <> %(eid)s)
                   ORDER BY ABS(EXTRACT(EPOCH FROM o.recorded_at - me.recorded_at)), l.event_id
                   LIMIT 1;""",
                {"eid": event_id, "cid": cluster_id, "other": other, "value": value, "need_sent": need_sent,
                 "hours": PURCHASE_TWIN_HOURS})
    row = cur.fetchone()
    return row["event_id"] if row else None


# Staff re-enter Partywirks birthday bookings into Tripleseat (for BEOs and kitchen sheets), so one party can
# fire a Partywirks Qualified_Bowler (qb_pw_<order>) and a Tripleseat one (qb_ts_<cluster>_<date>). One QB per
# customer per party date: the first one wins and the other is logged as DUPLICATE_QUALIFIED_BOWLER. The
# Partywirks party date is on its ledger row (unified_transactions pw_<order>.party_date, NULL = unknown, which
# never matches); the Tripleseat
# date is in its event ID (ISO since PR #28, M/D/YYYY before). Accepted edge case: a customer with two genuinely
# different parties on the same date, one in each system, gets one QB. Same-day double parties for one customer
# are rare; the re-entry is systematic. (Guest-count matching could refine this later.)
_QB_TWIN_SKIP = ("COALESCE(l.meta_response_status, '') NOT IN "
                 "('DROPPED_AFTER_ENRICHMENT', 'DUPLICATE_QUALIFIED_BOWLER')")


def _pw_party_date(cur, event_id: str) -> Optional[str]:
    """The booking's party date, or None while it isn't known (never the day the row was recorded)."""
    cur.execute("SELECT to_char(party_date, 'YYYY-MM-DD') AS d FROM unified_transactions "
                "WHERE transaction_id = %s AND source_system = 'partywirks';", ("pw_" + event_id[len("qb_pw_"):],))
    row = cur.fetchone()
    return row["d"] if row else None


def _ts_qb_date(event_id: str) -> Optional[str]:
    parts = event_id.split("_", 3)  # qb, ts, <cluster>, <date or record key>
    return ts_date(parts[3]) if len(parts) == 4 else None


def party_qb_twin(cur, cluster_id, event_name: str, event_id: str) -> Optional[str]:
    """The event ID of the other system's Qualified_Bowler for the same customer and party date, if it came first.

    It came first if Meta already has it, or if this event isn't logged yet, or if it was logged before this one
    (both held for enrichment or waiting for a retry: they're released oldest first, so the earlier one sends).
    An unsent twin only counts while it can still be sent: held for enrichment, or a failure young enough for
    the hourly retry (RETRY_MAX_AGE_DAYS). An expired failure never blocks a newer event."""
    if event_name != "Qualified_Bowler" or not cluster_id:
        return None
    if event_id.startswith("qb_pw_"):
        day, other = _pw_party_date(cur, event_id), "qb\\_ts\\_%"
    elif event_id.startswith("qb_ts_"):
        day, other = _ts_qb_date(event_id), "qb\\_pw\\_%"
    else:
        return None
    if not day:
        return None
    cur.execute("SELECT sent_at FROM capi_events_log WHERE event_id = %s;", (event_id,))
    me = cur.fetchone()
    cur.execute(f"""SELECT l.event_id, l.sent_to_meta, l.sent_at FROM capi_events_log l
                    WHERE l.cluster_id = %s AND l.event_name = 'Qualified_Bowler' AND l.event_id LIKE %s
                      AND l.event_id <> %s AND {_QB_TWIN_SKIP}
                      AND (l.sent_to_meta OR l.meta_response_status = 'DEFERRED_ENRICHMENT'
                           OR COALESCE(l.event_time, l.sent_at) > NOW() - make_interval(days => %s))
                    ORDER BY l.sent_to_meta DESC, l.sent_at, l.event_id;""",
                (cluster_id, other, event_id, RETRY_MAX_AGE_DAYS))
    for r in cur.fetchall():
        theirs = _pw_party_date(cur, r["event_id"]) if r["event_id"].startswith("qb_pw_") else _ts_qb_date(r["event_id"])
        if theirs != day:
            continue
        if r["sent_to_meta"] or not me or (r["sent_at"], r["event_id"]) < (me["sent_at"], event_id):
            return r["event_id"]
    return None


def _action_source_for(event_id: str) -> str:
    return "physical_store" if event_id.startswith("sq_pay_") else "system_generated"


def retry_unsent_event(cur, event_id: str) -> bool:
    """Re-sends a logged event Meta didn't accept. Safe to call for any ID: does nothing otherwise."""
    cur.execute("SELECT * FROM capi_events_log WHERE event_id = %s AND NOT sent_to_meta "
                f"AND {_NOT_HELD} FOR UPDATE SKIP LOCKED;", (event_id,))
    row = cur.fetchone()
    if not row or not row["cluster_id"]:
        return False
    cur.execute("SELECT * FROM master_customers WHERE cluster_id = %s;", (row["cluster_id"],))
    cust = cur.fetchone()
    if not cust:
        return False
    fire_meta_capi_event(cur, cust, row["event_name"], float(row["event_value"] or 0),
                         int(row["threshold_at_send"] or 0), event_id=event_id,
                         action_source=_action_source_for(event_id))
    return True


# Meta only takes events up to 7 days old; failures younger than this are re-sent by the hourly retry.
RETRY_MAX_AGE_DAYS = 6


def retry_unsent_events(cur, max_age_days: int = RETRY_MAX_AGE_DAYS) -> dict:
    """Re-sends every event Meta hasn't accepted yet. Meta only takes events up to 7 days old."""
    # Age by the event's own time when known (sent_at moves on every failed attempt), so a retry never sends an
    # event older than Meta accepts.
    cur.execute("SELECT event_id FROM capi_events_log WHERE NOT sent_to_meta "
                f"AND {_NOT_HELD} AND COALESCE(event_time, sent_at) > NOW() - make_interval(days => %s) "
                "ORDER BY sent_at;", (max_age_days,))
    ids = [r["event_id"] for r in cur.fetchall()]
    tried = sum(1 for eid in ids if retry_unsent_event(cur, eid))
    cur.execute("SELECT COUNT(*) AS n FROM capi_events_log WHERE event_id = ANY(%s) AND NOT sent_to_meta;", (ids,))
    return {"retried": tried, "still_unsent": cur.fetchone()["n"]}


# ---------------------------------------------------------------------------
# Transactions ledger (unified_transactions): one row per external payment / booking / record.
# This is what makes every webhook safe to receive more than once.
# ---------------------------------------------------------------------------

def claim_transaction(cur, transaction_id: str, source_system: str, amount: float = 0.0,
                      transaction_date=None, source_updated_at=None) -> bool:
    """Returns True the first time a transaction is seen. A concurrent duplicate blocks on the
    uncommitted row until the first request finishes, then gets False."""
    cur.execute(
        """
        INSERT INTO unified_transactions (transaction_id, source_system, total_amount, transaction_date,
                                          source_updated_at)
        VALUES (%s, %s, %s, COALESCE(%s::timestamptz, NOW()), %s::timestamptz)
        ON CONFLICT (transaction_id) DO NOTHING
        RETURNING transaction_id;
        """,
        (transaction_id, source_system, round(amount, 2), transaction_date, source_updated_at),
    )
    return cur.fetchone() is not None


def adjust_spend(cur, cluster_id: int, sq_diff: float = 0.0, wr_diff: float = 0.0, sq_visits: int = 0):
    cur.execute(
        """
        UPDATE master_customers SET
            sq_spend = GREATEST(0, COALESCE(sq_spend, 0) + %(sq)s),
            wr_spend = GREATEST(0, COALESCE(wr_spend, 0) + %(wr)s),
            webres_online_spend = GREATEST(0, COALESCE(wr_spend, 0) + %(wr)s),
            true_lifetime_spend = GREATEST(GREATEST(0, COALESCE(sq_spend, 0) + %(sq)s),
                                           GREATEST(0, COALESCE(wr_spend, 0) + %(wr)s), COALESCE(pw_spend, 0)),
            instore_pos_spend = GREATEST(0, GREATEST(0, COALESCE(sq_spend, 0) + %(sq)s)
                                            - GREATEST(0, COALESCE(wr_spend, 0) + %(wr)s)),
            sq_visits = COALESCE(sq_visits, 0) + %(v)s,
            updated_at = NOW()
        WHERE cluster_id = %(cid)s RETURNING *;
        """,
        {"sq": sq_diff, "wr": wr_diff, "v": sq_visits, "cid": cluster_id},
    )
    return cur.fetchone()


def reprice_transaction(cur, transaction_id: str, new_amount: float, kind: str, source_updated_at=None) -> dict:
    """A repeat notice for a known transaction: only apply a change in amount (e.g. a refund).
    A notice older than the one already applied (Square can deliver out of order) is ignored."""
    cur.execute(
        """
        SELECT cluster_id, total_amount,
               (%s::timestamptz IS NOT NULL AND source_updated_at IS NOT NULL
                AND %s::timestamptz < source_updated_at) AS is_stale
        FROM unified_transactions WHERE transaction_id = %s FOR UPDATE;
        """,
        (source_updated_at, source_updated_at, transaction_id),
    )
    row = cur.fetchone()
    if row["is_stale"]:
        return {"status": "ignored_stale_notice", "transaction_id": transaction_id}
    diff = round(new_amount - float(row["total_amount"] or 0), 2)
    if source_updated_at:
        cur.execute("UPDATE unified_transactions SET source_updated_at = GREATEST(COALESCE(source_updated_at, "
                    "%s::timestamptz), %s::timestamptz) WHERE transaction_id = %s;",
                    (source_updated_at, source_updated_at, transaction_id))
    if diff == 0:
        return {"status": "ignored_duplicate", "transaction_id": transaction_id}
    cur.execute("UPDATE unified_transactions SET total_amount = %s WHERE transaction_id = %s;",
                (round(new_amount, 2), transaction_id))
    if row["cluster_id"]:
        adjust_spend(cur, row["cluster_id"], sq_diff=diff if kind == "square" else 0.0,
                     wr_diff=diff if kind == "webres" else 0.0)
    return {"status": "amount_adjusted", "transaction_id": transaction_id, "delta": diff}


def attach_square_history(cur, cust: dict, sq_id: str, exclude_txn: str = None) -> dict:
    """A brand-new customer just got linked to a Square customer ID: credit the payments that Square
    ID made before we knew who they were (recorded by the history backfill, never counted by anyone).

    Only called for customers created in this request. Existing customers' imported totals may already
    include payments made under other Square IDs (Square merges profiles), so crediting those could
    double-count."""
    if not sq_id:
        return cust
    cur.execute(
        """
        UPDATE unified_transactions SET cluster_id = %s
        WHERE source_system = 'square' AND square_customer_id = %s
          AND cluster_id IS NULL AND external_customer_id IS NULL
          AND transaction_id <> COALESCE(%s, '')
        RETURNING total_amount, transaction_date;
        """,
        (cust["cluster_id"], sq_id, exclude_txn),
    )
    rows = cur.fetchall()
    if not rows:
        return cust
    total = round(sum(float(r["total_amount"] or 0) for r in rows), 2)
    first = min((r["transaction_date"] for r in rows if r["transaction_date"]), default=None)
    cust = adjust_spend(cur, cust["cluster_id"], sq_diff=total, sq_visits=len(rows))
    if first:
        cur.execute("UPDATE master_customers SET sq_first_visit_date = LEAST(COALESCE(sq_first_visit_date, %s::date), %s::date) "
                    "WHERE cluster_id = %s RETURNING *;", (first, first, cust["cluster_id"]))
        cust = cur.fetchone()
    log.info("Attached %s earlier Square payments to a newly identified customer", len(rows))
    return cust


def claim_pending_square_payments(cur, cust: dict, sq_id: str) -> dict:
    """Payments from a Square customer we couldn't identify yet are parked with no customer.
    Once that Square customer ID is linked to someone, their spend and visits are credited."""
    if not sq_id:
        return cust
    cur.execute(
        """
        UPDATE unified_transactions SET cluster_id = %s
        WHERE source_system = 'square' AND external_customer_id = %s AND cluster_id IS NULL
        RETURNING total_amount;
        """,
        (cust["cluster_id"], sq_id),
    )
    rows = cur.fetchall()
    if not rows:
        return cust
    total = sum(float(r["total_amount"] or 0) for r in rows)
    return adjust_spend(cur, cust["cluster_id"], sq_diff=total, sq_visits=len(rows))


# ---------------------------------------------------------------------------
# Identity resolution + upsert
# ---------------------------------------------------------------------------

ENRICHMENT_FIELDS = [
    "md_address1", "md_city", "md_state", "zip5", "md_plus4", "md_carrier_route",
    "distance_miles", "md_latitude", "md_longitude", "md_household_income",
    "md_presence_of_children", "md_children_age_range", "md_own_rent", "md_marital_status",
    "md_household_size", "md_length_of_residence", "md_education", "md_occupation",
    "md_date_of_birth", "md_gender", "md_delivery_indicator", "md_mik", "md_mak", "md_results",
    "md_census_tract", "md_census_block", "md_county_name", "md_unified_school_district_name",
    "md_elementary_school_district_name", "census_checked_at",
    "versium_net_worth", "versium_home_market_value", "versium_auto_make_model",
]
JSONB_FIELDS = {"versium_alt_emails", "versium_alt_phones", "versium_lifestyle_flags", "versium_online_audience"}

_MC = "master_customers"


def _keep(c):
    return f"COALESCE(EXCLUDED.{c}, {_MC}.{c})"


def _keep_text(c):
    return f"COALESCE(NULLIF(EXCLUDED.{c}, ''), {_MC}.{c})"


def _flag(c):
    return f"GREATEST(COALESCE({_MC}.{c}, 0), EXCLUDED.{c})"


def _add(c):
    return f"COALESCE({_MC}.{c}, 0) + EXCLUDED.{c}"


def _jsonb(c, empty):
    return f"CASE WHEN EXCLUDED.{c} <> '{empty}'::jsonb THEN EXCLUDED.{c} ELSE {_MC}.{c} END"


_SQ_NEW = f"GREATEST(0, COALESCE({_MC}.sq_spend, 0) + EXCLUDED.sq_spend)"
_PW_NEWER = (f"(EXCLUDED.pw_last_party_date IS NOT NULL AND ({_MC}.pw_last_party_date IS NULL "
             f"OR EXCLUDED.pw_last_party_date >= {_MC}.pw_last_party_date))")
_WR_NEW = f"GREATEST(0, COALESCE({_MC}.wr_spend, 0) + EXCLUDED.wr_spend)"

UPSERT_UPDATE_RULES = {
    "first_name": _keep_text("first_name"),
    "last_name": _keep_text("last_name"),
    "full_name": _keep_text("full_name"),
    "email": _keep_text("email"),
    "phone": _keep_text("phone"),
    "sq_id": _keep_text("sq_id"),
    "avs_postal_code": _keep_text("avs_postal_code"),
    "in_tripleseat": _flag("in_tripleseat"),
    "in_square": _flag("in_square"),
    "in_webres": _flag("in_webres"),
    "in_partywirks": _flag("in_partywirks"),
    "sq_spend": _SQ_NEW,
    "wr_spend": _WR_NEW,
    # Max, never a sum: a WebRes booking or Partywirks party and its Square payment are the same money.
    "true_lifetime_spend": f"GREATEST({_SQ_NEW}, {_WR_NEW}, COALESCE({_MC}.pw_spend, 0))",
    "webres_online_spend": _WR_NEW,
    "instore_pos_spend": f"GREATEST(0, {_SQ_NEW} - {_WR_NEW})",
    "sq_visits": _add("sq_visits"),
    "wr_bookings": _add("wr_bookings"),
    "ts_events": _add("ts_events"),
    "pw_bookings": _add("pw_bookings"),
    "ts_account": _keep("ts_account"),
    # Last-booking fields only move forward: recording an older booking never replaces a newer one.
    "pw_last_party_date": f"CASE WHEN {_PW_NEWER} THEN EXCLUDED.pw_last_party_date ELSE {_MC}.pw_last_party_date END",
    "pw_last_package": (f"CASE WHEN EXCLUDED.pw_last_package IS NOT NULL AND ({_PW_NEWER} OR "
                        f"(EXCLUDED.pw_last_party_date IS NULL AND {_MC}.pw_last_party_date IS NULL)) "
                        f"THEN EXCLUDED.pw_last_package ELSE {_MC}.pw_last_package END"),
    "wr_last_reservation_date": f"GREATEST({_MC}.wr_last_reservation_date, EXCLUDED.wr_last_reservation_date)",
    "sq_first_visit_date": f"COALESCE({_MC}.sq_first_visit_date, EXCLUDED.sq_first_visit_date)",
    "sq_last_visit_date": _keep("sq_last_visit_date"),
    **{c: _keep(c) for c in ENRICHMENT_FIELDS},
    "versium_alt_emails": _jsonb("versium_alt_emails", "[]"),
    "versium_alt_phones": _jsonb("versium_alt_phones", "[]"),
    # Merged, so a live lookup adds to (never wipes) what the CSV import stored.
    "versium_lifestyle_flags": f"COALESCE({_MC}.versium_lifestyle_flags, '{{}}'::jsonb) || EXCLUDED.versium_lifestyle_flags",
    "versium_online_audience": f"COALESCE({_MC}.versium_online_audience, '{{}}'::jsonb) || EXCLUDED.versium_online_audience",
    # A new inquiry (prospect/tentative) from a customer who has a confirmed booking doesn't un-book them.
    "ts_lead_status": (f"CASE WHEN UPPER({_MC}.ts_lead_status) IN ('DEFINITE', 'CLOSED') AND "
                       f"UPPER(EXCLUDED.ts_lead_status) IN ('PROSPECT', 'TENTATIVE', 'ACCOUNT_CONTACT_ONLY') "
                       f"THEN {_MC}.ts_lead_status ELSE {_keep('ts_lead_status')} END"),
    "ts_max_guests": f"GREATEST(COALESCE({_MC}.ts_max_guests, 0), COALESCE(EXCLUDED.ts_max_guests, 0))",
    "ts_last_event_date": _keep("ts_last_event_date"),
    "ts_turned_down_reason": _keep("ts_turned_down_reason"),
}


def _find_existing(cur, sq_id: str, email: str, phone: str, first_name: str, last_name: str):
    # 1A. Deterministic identity lookup (sq_id, email, phone)
    for id_type, id_val in [("sq_id", sq_id), ("email", email), ("phone", phone)]:
        if id_val:
            cur.execute(
                """
                SELECT mc.* FROM customer_identities ci
                JOIN master_customers mc ON mc.cluster_id = ci.cluster_id
                WHERE ci.id_type = %s AND ci.id_value = %s LIMIT 1;
                """,
                (id_type, id_val),
            )
            row = cur.fetchone()
            if row:
                return row

    # 1B. Same-day POS-to-WebRes/Partywirks/Tripleseat name stitch
    if len(first_name) >= 2 and len(last_name) >= 2:
        full_name = f"{first_name} {last_name}"
        cur.execute(
            """
            SELECT * FROM master_customers
            WHERE LOWER(TRIM(full_name)) = LOWER(%s)
              AND source != 'Square_Instant_Profile'
              AND (
                  (pw_last_party_date IS NOT NULL AND ABS(pw_last_party_date - CURRENT_DATE) <= 1)
                  OR (wr_last_reservation_date IS NOT NULL AND ABS(wr_last_reservation_date - CURRENT_DATE) <= 1)
                  OR (ts_last_event_date IS NOT NULL AND ABS(ts_last_event_date::date - CURRENT_DATE) <= 1)
                  OR (updated_at >= NOW() - INTERVAL '36 hours' AND (in_webres = 1 OR in_partywirks = 1 OR in_tripleseat = 1))
              )
            ORDER BY updated_at DESC NULLS LAST, true_lifetime_spend DESC NULLS LAST
            LIMIT 1;
            """,
            (full_name,),
        )
        row = cur.fetchone()
        if row:
            return row

        # Fallback: exactly one known profile in the database has this first + last name
        cur.execute(
            """
            SELECT * FROM master_customers
            WHERE LOWER(TRIM(full_name)) = LOWER(%s)
              AND source != 'Square_Instant_Profile'
            LIMIT 2;
            """,
            (full_name,),
        )
        matches = cur.fetchall()
        if len(matches) == 1:
            return matches[0]
    return None


def _needs_enrichment(existing, email: str, phone: str, data: dict) -> bool:
    has_input = bool(email or phone or data.get("address") or data.get("zip"))
    if not has_input:
        return False
    if existing is None:
        return True
    if existing.get("md_address1"):
        return False
    # Never pay twice for the same inputs: only retry if Melissa never answered for this person,
    # or we now have an email/phone we didn't have the last time.
    never_answered = existing.get("md_results") is None
    new_contact = (email and not existing.get("email")) or (phone and not existing.get("phone"))
    new_address = bool(data.get("address"))  # we have a street address and the record has none
    return bool(never_answered or new_contact or new_address)


def versium_matched(cust: dict) -> bool:
    """Versium returned demographics for this customer (not just our bookkeeping markers)."""
    flags = (cust or {}).get("versium_lifestyle_flags") or {}
    return isinstance(flags, dict) and any(not str(k).startswith("_") for k in flags)


def enrichment_plan(existing, email: str, phone: str, data: dict) -> Optional[str]:
    """With deferred enrichment: "new" (first sighting), "pending" (already waiting: just add the details),
    "retry" (Versium found nothing before and this webhook brings an email/phone/street we didn't have; once),
    "legacy" (never answered, same rule as before), or None."""
    has_input = bool(email or phone or data.get("address") or data.get("zip"))
    if not has_input:
        return None
    if existing is None:
        return "new"
    if existing.get("enrich_after"):
        return "pending"
    flags = existing.get("versium_lifestyle_flags") or {}
    if versium_checked(existing) and not versium_matched(existing) and not flags.get("_versium_retried") \
            and not is_detroit_excluded(existing.get("md_city"), existing.get("zip5")):
        alt_e = [str(x).lower() for x in existing.get("versium_alt_emails") or []]
        alt_p = [clean_phone(x) for x in existing.get("versium_alt_phones") or []]
        new_email = email and email != (existing.get("email") or "").strip().lower() and email not in alt_e
        new_phone = phone and phone != clean_phone(existing.get("phone") or "") and phone not in alt_p
        new_street = bool((data.get("address") or "").strip()) and not existing.get("md_address1")
        if new_email or new_phone or new_street:
            return "retry"
    return "legacy" if _needs_enrichment(existing, email, phone, data) else None


def upsert_and_enrich_customer(cur, data: dict) -> tuple:
    """Resolves identity across Square/WebRes/Tripleseat/Partywirks, enriches via Melissa+Versium
    when worthwhile, and upserts master_customers.

    Counter/spend changes are explicit deltas (sq_spend_delta, sq_visit_delta, wr_spend_delta,
    wr_booking_delta, ts_event_delta, pw_booking_delta); callers only pass them for
    transactions they have just claimed, so repeated webhooks never double count.
    """
    sq_id = (data.get("sq_id") or "").strip()
    email = (data.get("email") or "").strip().lower()
    phone = clean_phone(data.get("phone") or "")
    first_name = (data.get("first_name") or "").strip()
    last_name = (data.get("last_name") or "").strip()

    # Two webhooks for the same brand-new person (e.g. Square's customer.created and payment) can arrive
    # together. Lock each identifier for this transaction, in a fixed order so two requests can't deadlock;
    # the second waits, then finds the customer the first one created.
    for key in sorted({f"{t}:{v}" for t, v in (("sq_id", sq_id), ("email", email), ("phone", phone)) if v}):
        cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0));", (key,))
    existing = _find_existing(cur, sq_id, email, phone, first_name, last_name)

    # 2. Progressive enrichment: deferred (scheduled below) or, with ENRICH_DELAY_MINUTES=0, right here
    deferred = ENRICH_DELAY_MINUTES > 0
    plan = enrichment_plan(existing, email, phone, data) if deferred else None
    needs_enrichment = bool(plan) if deferred else _needs_enrichment(existing, email, phone, data)
    enriched = {}
    if needs_enrichment and not deferred:
        prior = existing or {}
        enriched = run_melissa_and_versium_waterfall(
            first_name or prior.get("first_name") or "",
            last_name or prior.get("last_name") or "",
            email or prior.get("email") or "",
            phone or prior.get("phone") or "",
            data.get("address") or "",
            data.get("city") or "",
            data.get("state") or "",
            data.get("zip") or "",
            known=prior,
        )
    elif deferred and plan == "new" and MELISSA_LIVE and (data.get("address") or "").strip():
        # Melissa's household key can match this person to an existing household, which must be known before
        # a new record is created; so with Melissa on, its street check stays in the webhook (about a second).
        street = {"md_address1": data["address"].strip(), "zip5": str(data.get("zip") or "")[:5] or None,
                  "md_city": data.get("city") or None, "md_state": data.get("state") or None}
        enriched = {**street, **{k: v for k, v in melissa_lookup(
            first_name, last_name, email, phone, data.get("address") or "", data.get("city") or "",
            data.get("state") or "", data.get("zip") or "").items() if v is not None}}
    elif not (existing or {}).get("md_address1") and (data.get("address") or "").strip():
        # Keep the street we were handed (the deferred lookup and Census work from it).
        enriched = {"md_address1": data["address"].strip(), "zip5": str(data.get("zip") or "")[:5] or None,
                    "md_city": data.get("city") or None, "md_state": data.get("state") or None}
    elif data.get("zip") and not (existing or {}).get("zip5"):
        # No paid lookup needed, but keep the ZIP we were handed so the geo gate can use it.
        enriched = {"zip5": str(data["zip"])[:5], "md_city": data.get("city") or None,
                    "md_state": data.get("state") or None}
    if not existing and enriched.get("md_mik"):
        cur.execute(
            """
            SELECT mc.* FROM customer_identities ci
            JOIN master_customers mc ON mc.cluster_id = ci.cluster_id
            WHERE ci.id_type = 'md_mik' AND ci.id_value = %s LIMIT 1;
            """,
            (enriched["md_mik"],),
        )
        existing = cur.fetchone()

    if existing:
        cluster_id = existing["cluster_id"]
    else:
        cur.execute("SELECT nextval('master_cluster_id_seq') AS next_id;")
        cluster_id = cur.fetchone()["next_id"]

    is_pw = 1 if data.get("in_partywirks") else 0
    sq_delta = float(data.get("sq_spend_delta") or 0.0)
    wr_delta = float(data.get("wr_spend_delta") or 0.0)
    sq_visit_delta = int(data.get("sq_visit_delta") or 0)
    today_iso = date.today().isoformat()

    vals = {
        "cluster_id": cluster_id,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": f"{first_name} {last_name}".strip(),
        "email": email,
        "phone": phone,
        "source": data.get("source", "Webhook"),
        "sq_id": sq_id,
        "avs_postal_code": (data.get("avs_postal_code") or "")[:5],
        "in_tripleseat": 1 if data.get("in_tripleseat") else 0,
        "in_square": 1 if data.get("in_square") else 0,
        "in_webres": 1 if data.get("in_webres") else 0,
        "in_partywirks": is_pw,
        "sq_spend": sq_delta,
        "wr_spend": wr_delta,
        "true_lifetime_spend": max(sq_delta, wr_delta),
        "webres_online_spend": wr_delta,
        "instore_pos_spend": max(0.0, sq_delta - wr_delta),
        "sq_visits": sq_visit_delta,
        "wr_bookings": int(data.get("wr_booking_delta") or 0),
        "ts_events": int(data.get("ts_event_delta") or 0),
        "pw_bookings": int(data.get("pw_booking_delta") or 0),
        "ts_account": data.get("ts_account") or None,
        "pw_last_package": data.get("pw_last_package"),
        "pw_last_party_date": data.get("pw_last_party_date"),
        "wr_last_reservation_date": data.get("wr_last_reservation_date"),
        "sq_first_visit_date": today_iso if sq_visit_delta else None,
        "sq_last_visit_date": today_iso if sq_visit_delta else None,
        **{c: enriched.get(c) for c in ENRICHMENT_FIELDS},
        "versium_alt_emails": json.dumps(enriched.get("alt_emails") or []),
        "versium_alt_phones": json.dumps(enriched.get("alt_phones") or []),
        "versium_lifestyle_flags": json.dumps(enriched.get("lifestyle_flags") or {}),
        "versium_online_audience": json.dumps(enriched.get("online_audience") or {}),
        "ts_lead_status": data.get("ts_lead_status"),
        "ts_max_guests": int(data.get("ts_max_guests") or 0) or None,
        "ts_last_event_date": data.get("ts_last_event_date"),
        "ts_turned_down_reason": data.get("ts_turned_down_reason"),
    }
    if is_pw:
        vals["md_presence_of_children"] = "Children Present"

    # Last line of defence: a dict or list in a plain column makes psycopg2 fail the whole webhook
    # ("can't adapt type 'dict'"). Handlers convert the fields they know (as_text); anything else is dropped.
    for c, v in vals.items():
        if c not in JSONB_FIELDS and isinstance(v, (dict, list, tuple)):
            log.warning("Dropped a %s value for column %s (%s)", type(v).__name__, c, vals.get("source"))
            vals[c] = None

    cols = list(vals)
    placeholders = [f"%({c})s::jsonb" if c in JSONB_FIELDS else f"%({c})s" for c in cols]
    sql = f"INSERT INTO master_customers ({', '.join(cols)}, updated_at) VALUES ({', '.join(placeholders)}, NOW())"
    if existing:
        sets = [f"{c} = {UPSERT_UPDATE_RULES[c]}" for c in cols if c in UPSERT_UPDATE_RULES]
        sql += f" ON CONFLICT (cluster_id) DO UPDATE SET {', '.join(sets)}, updated_at = NOW()"
    # A brand-new customer is a plain INSERT: if the ID were ever taken, this fails loudly
    # instead of silently merging two different people.
    cur.execute(sql + " RETURNING *;", vals)
    cust = cur.fetchone()
    if plan:
        # Start (or keep) the waiting window; the first webhook's time counts, later ones just add details.
        inputs = {k: v for k, v in {"email": email, "phone": phone, "address": (data.get("address") or "").strip(),
                                    "city": data.get("city"), "state": data.get("state"),
                                    "zip": str(data.get("zip") or "")[:5]}.items() if v}
        if plan == "retry":
            inputs["_retry"] = True
        cur.execute("""UPDATE master_customers
                       SET enrich_after = COALESCE(enrich_after, NOW() + make_interval(mins => %s)),
                           enrich_inputs = COALESCE(enrich_inputs, '{}'::jsonb) || %s::jsonb
                       WHERE cluster_id = %s RETURNING *;""", (ENRICH_DELAY_MINUTES, json.dumps(inputs), cluster_id))
        cust = cur.fetchone()

    # 3. Index all primary & Versium alternate identifiers
    id_pairs = [("email", email), ("phone", phone), ("sq_id", sq_id), ("md_mik", enriched.get("md_mik"))]
    id_pairs += [("email", e) for e in enriched.get("alt_emails") or []]
    id_pairs += [("phone", p) for p in enriched.get("alt_phones") or []]
    for id_type, id_val in id_pairs:
        if id_val:
            cur.execute(
                """
                INSERT INTO customer_identities (cluster_id, id_type, id_value, source_system)
                VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;
                """,
                (cluster_id, id_type, id_val, data.get("source", "Webhook")),
            )

    # 4. Credit any parked Square payments from this Square customer, then rescore
    cust = claim_pending_square_payments(cur, cust, sq_id)
    if not existing:
        cust = attach_square_history(cur, cust, sq_id, exclude_txn=data.get("_txn_id"))
    cust = rescore_customer(cur, cust)
    return cust, needs_enrichment


def save_enrichment(cur, cluster_id: int, enr: dict, source: str, extra: dict = None):
    """Used by the cron backfills: writes every enrichment field without overwriting known values."""
    vals = {c: enr.get(c) for c in ENRICHMENT_FIELDS}
    vals.update(extra or {})
    sets = [f"{c} = COALESCE(%({c})s, {c})" for c in vals]
    alt_e, alt_p = enr.get("alt_emails") or [], enr.get("alt_phones") or []
    flags = enr.get("lifestyle_flags") or {}
    if alt_e:
        sets.append("versium_alt_emails = %(alt_e)s::jsonb")
    if alt_p:
        sets.append("versium_alt_phones = %(alt_p)s::jsonb")
    if flags:
        sets.append("versium_lifestyle_flags = COALESCE(versium_lifestyle_flags, '{}'::jsonb) || %(flags)s::jsonb")
    online = enr.get("online_audience") or {}
    if online:
        sets.append("versium_online_audience = COALESCE(versium_online_audience, '{}'::jsonb) || %(online)s::jsonb")
    vals.update({"alt_e": json.dumps(alt_e), "alt_p": json.dumps(alt_p), "flags": json.dumps(flags),
                 "online": json.dumps(online),
                 "cid": cluster_id})
    cur.execute(f"UPDATE master_customers SET {', '.join(sets)}, updated_at = NOW() "
                f"WHERE cluster_id = %(cid)s RETURNING *;", vals)
    cust = cur.fetchone()
    pairs = [("md_mik", enr.get("md_mik"))] + [("email", e) for e in alt_e] + [("phone", p) for p in alt_p]
    for id_type, id_val in pairs:
        if id_val:
            cur.execute(
                "INSERT INTO customer_identities (cluster_id, id_type, id_value, source_system) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;",
                (cluster_id, id_type, id_val, source),
            )
    return cust


# ---------------------------------------------------------------------------
# Square catalog
# ---------------------------------------------------------------------------

def run_square_catalog_sync() -> dict:
    """Pulls all Square Catalog Categories, Items, and Price Key Variations into square_catalog_items in Neon."""
    if not SQUARE_ACCESS_TOKEN:
        return {"status": "error", "message": "Missing SQUARE_ACCESS_TOKEN"}

    try:
        headers = {
            "Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}",
            "Accept": "application/json",
        }

        all_objects = []
        cursor = None
        while True:
            params = {"types": "ITEM,CATEGORY"}
            if cursor:
                params["cursor"] = cursor
            resp = requests.get(
                "https://connect.squareup.com/v2/catalog/list",
                headers=headers,
                params=params,
                timeout=15,
            ).json()
            if "errors" in resp:
                return {"status": "square_api_error", "errors": resp["errors"]}
            objs = resp.get("objects") or []
            all_objects.extend(objs)
            cursor = resp.get("cursor")
            if not cursor:
                break

        categories = {}
        for obj in all_objects:
            if obj.get("type") == "CATEGORY":
                cat_data = obj.get("category_data") or {}
                categories[obj["id"]] = cat_data.get("name") or ""

        rows_to_upsert = []
        for obj in all_objects:
            if obj.get("type") != "ITEM":
                continue
            item_id = obj.get("id")
            is_deleted = bool(obj.get("is_deleted", False))
            item_data = obj.get("item_data") or {}
            item_name = item_data.get("name") or ""

            cat_id = item_data.get("category_id") or ""
            if not cat_id and isinstance(item_data.get("categories"), list) and item_data["categories"]:
                cat_id = (item_data["categories"][0] or {}).get("id") or ""
            if not cat_id and (item_data.get("reporting_category") or {}).get("id"):
                cat_id = item_data["reporting_category"]["id"]
            cat_name = categories.get(cat_id, "")

            for var in (item_data.get("variations") or []):
                var_id = var.get("id")
                if not var_id:
                    continue
                var_data = var.get("item_variation_data") or {}
                var_name = var_data.get("name") or ""
                sku = var_data.get("sku") or ""
                price_money = var_data.get("price_money") or {}
                price_cents = price_money.get("amount") or 0
                price_dollars = round(float(price_cents) / 100.0, 2)

                combined = f"{cat_name} {item_name} {var_name}".lower()
                if any(k in combined for k in ["party", "birthday", "package", "event", "corporate", "banquet", "deposit"]):
                    dept = "Parties & Events"
                elif any(k in combined for k in ["shoe", "sock"]):
                    dept = "Shoes & Socks"
                elif any(k in combined for k in ["bowl", "lane", "game", "league", "cosmic"]):
                    dept = "Bowling & Lanes"
                elif any(k in combined for k in ["laser", "arcade", "card", "play", "vr", "token"]):
                    dept = "Laser Tag & Arcade"
                elif any(k in combined for k in ["beer", "wine", "liquor", "cocktail", "drink", "bar", "seltzer", "shot", "draft", "pitcher"]):
                    dept = "Bar & Alcohol"
                elif any(k in combined for k in ["pizza", "wing", "burger", "fries", "pretzel", "food", "kitchen", "soda", "pop", "appetizer", "salad", "nacho", "tender"]):
                    dept = "Food & Kitchen"
                else:
                    dept = "General / Other"

                rows_to_upsert.append((
                    var_id, item_id, item_name, var_name, cat_id, cat_name,
                    price_dollars, sku, dept, is_deleted
                ))

        conn = get_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS square_catalog_items (
                            variation_id TEXT PRIMARY KEY,
                            item_id TEXT,
                            item_name TEXT,
                            variation_name TEXT,
                            category_id TEXT,
                            category_name TEXT,
                            price_dollars NUMERIC(10,2),
                            sku TEXT,
                            pg_department TEXT,
                            is_deleted BOOLEAN DEFAULT FALSE,
                            updated_at TIMESTAMPTZ DEFAULT NOW()
                        );
                        """
                    )
                    for r in rows_to_upsert:
                        cur.execute(
                            """
                            INSERT INTO square_catalog_items (
                                variation_id, item_id, item_name, variation_name,
                                category_id, category_name, price_dollars, sku, pg_department, is_deleted, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (variation_id) DO UPDATE SET
                                item_name = EXCLUDED.item_name,
                                variation_name = EXCLUDED.variation_name,
                                category_id = EXCLUDED.category_id,
                                category_name = EXCLUDED.category_name,
                                price_dollars = EXCLUDED.price_dollars,
                                sku = EXCLUDED.sku,
                                pg_department = EXCLUDED.pg_department,
                                is_deleted = EXCLUDED.is_deleted,
                                updated_at = NOW();
                            """,
                            r,
                        )
            return {
                "status": "catalog_synced",
                "total_variations_synced": len(rows_to_upsert),
                "total_categories_found": len(categories),
            }
        finally:
            conn.close()
    except Exception as e:
        return {"status": "exception", "detail": str(e)}


# ---------------------------------------------------------------------------
# Webhook handlers.
# Each route is `async` only to read the raw body; all blocking work (database, Square,
# Melissa, Versium, Meta) runs in a worker thread so one slow request never stalls the others.
# ---------------------------------------------------------------------------

def _identified_sq_customer(cur, sq_id: str) -> bool:
    cur.execute("SELECT 1 FROM customer_identities WHERE id_type = 'sq_id' AND id_value = %s LIMIT 1;", (sq_id,))
    return cur.fetchone() is not None


def handle_square_payment(cur, payment: dict, background_tasks: BackgroundTasks) -> dict:
    if payment.get("status") != "COMPLETED":
        return {"status": "ignored_incomplete_payment"}
    payment_id = payment.get("id")
    if not payment_id:
        return {"status": "ignored_missing_payment_id"}

    # total_money includes tips; refunds are subtracted.
    total_cents = to_int((payment.get("total_money") or payment.get("amount_money") or {}).get("amount"))
    refund_cents = to_int((payment.get("refunded_money") or {}).get("amount"))
    net = round((total_cents - refund_cents) / 100.0, 2)
    txn_id = f"sq_pay_{payment_id}"

    if not claim_transaction(cur, txn_id, "square", net, payment.get("created_at"), payment.get("updated_at")):
        result = reprice_transaction(cur, txn_id, net, "square", payment.get("updated_at"))
        retry_unsent_event(cur, txn_id)
        return result

    sq_id = (payment.get("customer_id") or "").strip()
    # What was bought: read the payment's order so every payment carries its price keys.
    if payment.get("order_id"):
        order = (square_api("GET", f"/v2/orders/{payment['order_id']}", timeout=5) or {}).get("order")
        if order:
            save_order_items(cur, txn_id, order, sq_id)
        else:
            cur.execute("UPDATE unified_transactions SET square_customer_id = NULLIF(%s, '') WHERE transaction_id = %s;",
                        (sq_id, txn_id))
    email = (payment.get("buyer_email_address") or "").strip().lower()
    card = (payment.get("card_details") or {}).get("card") or {}
    card_zip = ((card.get("billing_address") or {}).get("postal_code")
                or (payment.get("billing_address") or {}).get("postal_code") or "")[:5]
    chip_fn, chip_ln = parse_cardholder_name(card.get("cardholder_name") or "")
    has_name = bool(chip_fn and chip_ln)

    data = {
        "sq_id": sq_id, "first_name": chip_fn, "last_name": chip_ln, "email": email,
        "zip": card_zip, "avs_postal_code": card_zip,
        "sq_spend_delta": net, "sq_visit_delta": 1, "in_square": 1, "source": "Square_POS", "_txn_id": txn_id,
    }

    if sq_id and not _identified_sq_customer(cur, sq_id):
        profile = fetch_square_customer_profile(sq_id)
        if profile is None and not (email or has_name):
            # Square is the only way to identify this buyer and it didn't answer: fail so Square retries.
            raise HTTPException(status_code=503, detail="Square customer lookup unavailable; retry")
        profile = profile or {}
        for k in ("first_name", "last_name", "email", "phone", "address", "city", "state", "zip"):
            if profile.get(k) and not data.get(k):
                data[k] = profile[k]
        if not any([data.get("email"), clean_phone(data.get("phone")),
                    (data.get("first_name") and data.get("last_name"))]):
            # Blank Square profile: park the payment; it's credited if this Square customer is identified later.
            cur.execute("UPDATE unified_transactions SET external_customer_id = %s WHERE transaction_id = %s;",
                        (sq_id, txn_id))
            return {"status": "parked_unidentified_square_customer", "payment_id": payment_id}
    elif not sq_id and not (email or has_name):
        return {"status": "skipped_anonymous_payment", "payment_id": payment_id}

    cust, newly_enriched = upsert_and_enrich_customer(cur, data)
    cur.execute("UPDATE unified_transactions SET cluster_id = %s WHERE transaction_id = %s;",
                (cust["cluster_id"], txn_id))
    cust = rescore_customer(cur, cust)  # the score now includes this payment (visit, recency)

    thresh = get_threshold(cur)
    if net > 0 and not is_detroit_excluded(cust.get("md_city"), cust.get("zip5")):
        fire_meta_capi_event(cur, cust, "Purchase", net, thresh, event_id=txn_id, action_source="physical_store",
                             event_time=parse_source_time(payment.get("created_at")))
        if newly_enriched:
            background_tasks.add_task(sync_customer_to_meta_custom_audiences, dict(cust))
    return {"status": "payment_attributed", "cluster_id": cust["cluster_id"], "amount": net}


def handle_square_customer(cur, event_type: str, c_obj: dict, background_tasks: BackgroundTasks) -> dict:
    if event_type == "customer.deleted":
        return {"status": "ignored_deleted_customer"}
    fn = (c_obj.get("given_name") or "").strip()
    ln = (c_obj.get("family_name") or "").strip()
    em = (c_obj.get("email_address") or "").strip()
    ph = clean_phone(c_obj.get("phone_number") or "")
    if not any([em, ph, (len(fn) >= 2 and len(ln) >= 2)]):
        return {"status": "ignored_blank_customer_profile"}

    addr = c_obj.get("address") or {}
    cust, _ = upsert_and_enrich_customer(cur, {
        "sq_id": c_obj.get("id"),
        "first_name": fn,
        "last_name": ln,
        "email": em,
        "phone": ph,
        "address": addr.get("address_line_1"),
        "city": addr.get("locality"),
        "state": addr.get("administrative_district_level_1"),
        "zip": addr.get("postal_code"),
        "in_square": 1,
        "source": "Square_Customer",
    })
    thresh = get_threshold(cur)
    if not is_detroit_excluded(cust.get("md_city"), cust.get("zip5")):
        background_tasks.add_task(sync_customer_to_meta_custom_audiences, dict(cust))
    return {"status": "customer_synced", "cluster_id": cust["cluster_id"]}


def handle_square_event(body: dict, background_tasks: BackgroundTasks) -> dict:
    event_type = body.get("type", "")
    obj = (body.get("data") or {}).get("object") or {}

    if event_type == "catalog.version.updated":
        background_tasks.add_task(run_square_catalog_sync)
        return {"status": "catalog_sync_queued"}
    if not (event_type.startswith("payment.") or event_type.startswith("customer.")):
        return {"status": "ignored"}

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                if event_type.startswith("payment."):
                    return handle_square_payment(cur, obj.get("payment") or {}, background_tasks)
                return handle_square_customer(cur, event_type, obj.get("customer") or {}, background_tasks)
    finally:
        conn.close()


@app.post("/webhooks/square")
async def square_webhook(request: Request, background_tasks: BackgroundTasks):
    """Square POS payments, customers, and catalog updates. Safe to receive the same event many times."""
    raw, body = await read_json(request)
    verify_square_signature(request, raw)
    return await run_in_threadpool(handle_square_event, body, background_tasks)


def handle_webres(data: dict, background_tasks: BackgroundTasks) -> dict:
    amount = to_float(data.get("total_amount") or data.get("amount"))
    res_date = date_only(data.get("reservation_date") or data.get("event_date") or data.get("date")) \
        or date.today().isoformat()
    booking_id = data.get("reservation_id") or data.get("id")
    if not booking_id:
        # No ID in the payload: derive a stable one (hashed, so no contact details leave in event IDs).
        name = f"{data.get('first_name') or ''} {data.get('last_name') or ''}".strip().lower()
        basis = f"{(data.get('email') or '').strip().lower()}|{clean_phone(data.get('phone'))}|{name}|{res_date}|{amount:.2f}"
        booking_id = "h" + hashlib.sha256(basis.encode()).hexdigest()[:16]
    txn_id = f"wr_{booking_id}"
    if not any([data.get("email"), clean_phone(data.get("phone")), data.get("first_name")]):
        return {"status": "ignored_empty_webres_payload"}

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                if not claim_transaction(cur, txn_id, "webres", amount):
                    result = reprice_transaction(cur, txn_id, amount, "webres")
                    retry_unsent_event(cur, txn_id)
                    retry_unsent_event(cur, f"qb_wr_{booking_id}")
                    return result

                cust, _ = upsert_and_enrich_customer(cur, {
                    "first_name": data.get("first_name"),
                    "last_name": data.get("last_name"),
                    "email": data.get("email"),
                    "phone": data.get("phone"),
                    # Only if the Gmail script sends them (it reads name/email/phone from the PDF today).
                    "address": data.get("address") or data.get("street"),
                    "city": data.get("city"),
                    "state": data.get("state"),
                    "zip": data.get("zip") or data.get("postal_code"),
                    "wr_spend_delta": amount,
                    "wr_booking_delta": 1,
                    "wr_last_reservation_date": res_date,
                    "in_webres": 1,
                    "source": "WebRes",
                })
                cur.execute("UPDATE unified_transactions SET cluster_id = %s WHERE transaction_id = %s;",
                            (cust["cluster_id"], txn_id))
                cust = rescore_customer(cur, cust)  # the score now includes this booking
                thresh = get_threshold(cur)

                # When the booking was made: the Gmail script's booking/email time if it sends one (never the
                # reservation date, which is when they bowl). Without one, now: the script posts each confirmation
                # email within ~5 minutes of it arriving.
                booked = parse_source_time(data.get("booked_at") or data.get("created_at")
                                           or data.get("email_received_at"))
                if not is_detroit_excluded(cust.get("md_city"), cust.get("zip5")):
                    if amount > 0:
                        fire_meta_capi_event(cur, cust, "Purchase", amount, thresh, event_id=txn_id, event_time=booked)
                    if cust["bowler_value_score"] >= thresh or cust.get("enrich_after"):
                        fire_meta_capi_event(cur, cust, "Qualified_Bowler", amount, thresh,
                                             event_id=f"qb_wr_{booking_id}", event_time=booked)
                    background_tasks.add_task(sync_customer_to_meta_custom_audiences, dict(cust))

        return {"status": "webres_synced", "cluster_id": cust["cluster_id"], "score": cust["bowler_value_score"]}
    finally:
        conn.close()


@app.post("/webhooks/webres")
async def webres_webhook(request: Request, background_tasks: BackgroundTasks):
    """Online lane reservations (WebRes/BowlNow). Each booking is counted once."""
    require_webhook_key(request)
    _, data = await read_json(request)
    return await run_in_threadpool(handle_webres, data, background_tasks)


def _count_record_once(cur, cust: dict, txn_id: str, source_system: str, counter: str,
                       bump: bool = True) -> tuple:
    """Bumps a booking/event counter only the first time we see this Tripleseat/Partywirks record.
    Returns (customer, first_time_seen). bump=False records the ID without counting it."""
    if not claim_transaction(cur, txn_id, source_system):
        return cust, False
    cur.execute("UPDATE unified_transactions SET cluster_id = %s WHERE transaction_id = %s;",
                (cust["cluster_id"], txn_id))
    if not bump:
        return cust, True
    cur.execute(f"UPDATE master_customers SET {counter} = COALESCE({counter}, 0) + 1 "
                f"WHERE cluster_id = %s RETURNING *;", (cust["cluster_id"],))
    return rescore_customer(cur, cur.fetchone()), True


def tripleseat_record(raw: dict) -> tuple:
    """(kind, object) of a Tripleseat webhook: its lead, event, booking or contact."""
    for key in ("lead", "event", "booking", "contact"):
        if isinstance(raw.get(key), dict):
            return key, raw[key]
    return "record", raw


def tripleseat_record_key(raw) -> Optional[str]:
    """Which Tripleseat record a webhook is about ("event:123"), so a failed copy can be matched to newer ones."""
    if not isinstance(raw, dict):
        return None
    kind, obj = tripleseat_record(raw)
    rid = as_text(obj.get("id") or obj.get("lead_id") or obj.get("event_id"))
    return f"{kind}:{rid}" if rid else None


# Tripleseat event statuses that mean a booked event (CLOSED = definite and already held).
TRIPLESEAT_BOOKED_STATUSES = ("DEFINITE", "CLOSED")


def tripleseat_contract_value(kind: str, obj: dict) -> float:
    """The contracted money of a Tripleseat event (grand_total: incl. taxes, fees, gratuity), or 0 if unknown.
    A booking uses its total_grand_total, else adds up the totals of its events that aren't lost or deleted."""
    def money(v) -> float:
        return max(ts_money(as_text(v)) or 0.0, 0.0)  # ts_money also reads "2,400.00" / "$2400"
    if kind == "booking":
        return money(obj.get("total_grand_total")) or round(sum(
            money(e.get("grand_total")) for e in obj.get("events") or []
            if isinstance(e, dict) and str(e.get("status") or "").upper() != "LOST" and not e.get("deleted_at")), 2)
    return money(obj.get("grand_total"))


def ts_conversion_time(kind: str, obj: dict, status: str) -> Optional[datetime]:
    """When a Tripleseat conversion happened: for a booking, when it moved to its current status
    (status_changes), else the record's created_at. Never the event date (that's when the party happens).
    meta_event_time turns a time older than Meta accepts (a record created weeks ago that became definite
    today) into now, when we learn of it."""
    if kind == "booking":
        changes = [c for c in obj.get("status_changes") or obj.get("booking_status_changes") or []
                   if isinstance(c, dict) and str(c.get("status") or "").upper() == status]
        times = [t for t in (parse_source_time(c.get("created_at")) for c in changes) if t]
        if times:
            return max(times)
    return parse_source_time(obj.get("created_at"))


def ts_event_id(cur, prefix: str, cust: dict, key: str, kind: str, record_id) -> str:
    """The Tripleseat QB/Lead event ID: <prefix>_ts_<cluster>_<date key>. Before bookings were keyed by their
    start_date, a booking's event was logged as <prefix>_ts_<cluster>_booking_<id>; if that one exists, it's
    reused (already sent: nothing more is sent; failed or held: it's the one retried), so a booking re-sent
    after the change never sends a second conversion."""
    eid = f"{prefix}_ts_{cust['cluster_id']}_{key}"
    if kind == "booking" and record_id:
        legacy = f"{prefix}_ts_{cust['cluster_id']}_booking_{record_id}"
        if legacy != eid:
            cur.execute("SELECT 1 FROM capi_events_log WHERE event_id = %s;", (legacy,))
            if cur.fetchone():
                return legacy
    return eid


def handle_tripleseat(raw: dict, background_tasks: BackgroundTasks) -> dict:
    kind, obj = tripleseat_record(raw)
    contact = obj.get("contact") if isinstance(obj.get("contact"), dict) else obj
    account = obj.get("account") or contact.get("account") or {}
    if not isinstance(account, dict):
        account = {}

    # Every value is read through as_text: Tripleseat sends some fields as objects (lost_reason is
    # {"id", "name"} on LOST events), and a dict in a text column used to fail the whole webhook.
    email = as_text(contact.get("email_address") or obj.get("email_address") or obj.get("email")) or ""
    if not email and isinstance(contact.get("email_addresses"), list) and contact["email_addresses"] \
            and isinstance(contact["email_addresses"][0], dict):
        email = as_text(contact["email_addresses"][0].get("address")) or ""

    phone = as_text(contact.get("phone_number") or obj.get("phone_number") or obj.get("phone")) or ""
    if not phone and isinstance(contact.get("phone_numbers"), list) and contact["phone_numbers"] \
            and isinstance(contact["phone_numbers"][0], dict):
        phone = as_text(contact["phone_numbers"][0].get("number")) or ""

    addr_obj = {}
    if isinstance(contact.get("addresses"), list) and contact["addresses"]:
        addr_obj = contact["addresses"][0]
    elif isinstance(obj.get("addresses"), list) and obj["addresses"]:
        addr_obj = obj["addresses"][0]
    if not isinstance(addr_obj, dict):
        addr_obj = {}

    first_name = as_text(contact.get("first_name") or obj.get("first_name")) or ""
    last_name = as_text(contact.get("last_name") or obj.get("last_name")) or ""
    company = as_text(account.get("name") or obj.get("company") or obj.get("account_name")
                      or contact.get("company")) or ""
    status = (as_text(obj.get("status") or obj.get("lead_status")) or "PROSPECT").upper().replace(" ", "_")
    guest_count = to_int(obj.get("guest_count") or obj.get("guaranteed_guest_count") or obj.get("expected_guest_count"))
    # A booking has no event_date, only start_date (a plain YYYY-MM-DD). Reading it gives a booking the same
    # date key as its event, so one Tripleseat party gets one Qualified_Bowler / Lead, not a date-key one plus
    # a booking-key one seconds apart.
    event_date = date_only(as_text(obj.get("event_date") or obj.get("event_start_iso8601") or obj.get("desired_date")
                                   or (obj.get("start_date") if kind == "booking" else None)))
    turned_down_reason = as_text(obj.get("turned_down_reason") or obj.get("lost_reason"))
    record_id = as_text(obj.get("id") or obj.get("lead_id") or obj.get("event_id"))

    if not any([email, phone, first_name, last_name]):
        return {"status": "ignored_empty_tripleseat_payload"}

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cust, _ = upsert_and_enrich_customer(cur, {
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email,
                    "phone": phone,
                    "address": as_text(addr_obj.get("address1") or obj.get("address")),
                    "city": as_text(addr_obj.get("city") or obj.get("city")),
                    "state": as_text(addr_obj.get("state") or obj.get("state")),
                    "zip": as_text(addr_obj.get("zip_code") or obj.get("zip")),
                    "ts_account": company or None,
                    "ts_lead_status": status,
                    "ts_max_guests": guest_count,
                    "ts_last_event_date": event_date,
                    "ts_turned_down_reason": turned_down_reason,
                    "in_tripleseat": 1,
                    "source": "Tripleseat",
                })
                # fbp / fbc / user agent from the lead form's hidden fields (captured when the form was submitted).
                browser = ts_browser_ids(obj)
                if browser:
                    save_browser_ids(cur, cust["cluster_id"], browser, parse_source_time(obj.get("created_at")))
                    cur.execute("SELECT * FROM master_customers WHERE cluster_id = %s;", (cust["cluster_id"],))
                    cust = cur.fetchone()
                if record_id and kind in ("lead", "event", "booking"):
                    # A booking counts once: an event of an already-recorded booking (or a booking one of
                    # whose events is already recorded) is claimed without counting again.
                    related = []
                    if kind == "event" and obj.get("booking_id"):
                        related = [f"ts_booking_{obj['booking_id']}"]
                    elif kind == "booking":
                        related = [f"ts_event_{e['id']}" for e in obj.get("events") or []
                                   if isinstance(e, dict) and e.get("id")]
                    bump = not (related and _ts_known(cur, related))
                    cust, _ = _count_record_once(cur, cust, f"ts_{kind}_{record_id}", "tripleseat", "ts_events",
                                                 bump=bump)

                thresh = get_threshold(cur)
                excluded = is_detroit_excluded(cust.get("md_city"), cust.get("zip5"))
                if not excluded:
                    # Seeds are who the customer is (an organiser stays one after a lost event), not the score.
                    background_tasks.add_task(sync_customer_to_meta_custom_audiences, dict(cust))
                if (
                    not excluded
                    and (cust["bowler_value_score"] >= thresh or cust.get("enrich_after"))
                    and status not in ("LOST", "TURNED_DOWN")
                ):
                    # One event per customer per event date, however many times Tripleseat re-sends the
                    # lead/event/booking (a lead, its event and the event's booking share the date; the record
                    # key is only a fallback for a record with no date at all). Only a definite booking
                    # is a Qualified_Bowler, worth its contracted total (or no value); a prospect or tentative
                    # lead is a Lead with no value. A lead that later becomes definite sends both, once each.
                    key = ts_date(event_date) or event_date or (f"{kind}_{record_id}" if record_id else "undated")
                    if kind in ("event", "booking") and status in TRIPLESEAT_BOOKED_STATUSES:
                        fire_meta_capi_event(cur, cust, "Qualified_Bowler", tripleseat_contract_value(kind, obj),
                                             thresh, event_id=ts_event_id(cur, "qb", cust, key, kind, record_id),
                                             event_time=ts_conversion_time(kind, obj, status))
                    else:
                        fire_meta_capi_event(cur, cust, "Lead", 0.0, thresh,
                                             event_id=ts_event_id(cur, "lead", cust, key, kind, record_id),
                                             event_time=ts_conversion_time(kind, obj, status))

        return {
            "status": "tripleseat_synced",
            "cluster_id": cust["cluster_id"],
            "lead_status": status,
            "score": cust["bowler_value_score"],
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tripleseat OAuth 2 (authorization code). /oauth/tripleseat/login sends a person to Tripleseat to approve
# access; Tripleseat returns to /oauth/tripleseat/callback with a code, which is swapped for tokens stored
# in oauth_tokens (migrations/006). tripleseat_access_token() renews the 2-hour token with the refresh token.
# ---------------------------------------------------------------------------
TRIPLESEAT_STATE_COOKIE = "ts_oauth_state"
TRIPLESEAT_STATE_MAX_AGE = 600  # seconds the person has to finish logging in at Tripleseat
TRIPLESEAT_REFRESH_MARGIN = 300  # renew when the access token has less than this many seconds left


class TripleseatNotConnected(Exception):
    """Tripleseat isn't set up yet (no client secret) or nobody has logged in at /oauth/tripleseat/login."""


def tripleseat_enabled() -> bool:
    return bool(TRIPLESEAT_CLIENT_ID and TRIPLESEAT_CLIENT_SECRET)


def _tripleseat_state_sig(payload: str) -> str:
    return hmac.new(TRIPLESEAT_CLIENT_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()


def make_tripleseat_state(now: Optional[float] = None) -> str:
    payload = f"{int(now if now is not None else time.time())}.{base64.urlsafe_b64encode(os.urandom(18)).decode()}"
    return f"{payload}.{_tripleseat_state_sig(payload)}"


def tripleseat_state_valid(state: str, cookie: str, now: Optional[float] = None) -> bool:
    """The state must be ours (signed), recent, and match the cookie set on the browser that started the login."""
    if not state or not cookie or not hmac.compare_digest(state.encode(), cookie.encode()):
        return False
    try:
        ts, nonce, sig = state.split(".")
        age = (now if now is not None else time.time()) - int(ts)
    except ValueError:
        return False
    return 0 <= age <= TRIPLESEAT_STATE_MAX_AGE and hmac.compare_digest(sig, _tripleseat_state_sig(f"{ts}.{nonce}"))


def tripleseat_authorize_url(state: str) -> str:
    params = {"client_id": TRIPLESEAT_CLIENT_ID, "redirect_uri": TRIPLESEAT_REDIRECT_URI,
              "response_type": "code", "state": state}
    if TRIPLESEAT_SCOPES:
        params["scope"] = TRIPLESEAT_SCOPES
    return requests.Request("GET", TRIPLESEAT_AUTHORIZE_URL, params=params).prepare().url


def _tripleseat_token_request(fields: dict) -> dict:
    body = {"client_id": TRIPLESEAT_CLIENT_ID, "client_secret": TRIPLESEAT_CLIENT_SECRET, **fields}
    r = requests.post(TRIPLESEAT_TOKEN_URL, data=body, headers={"Accept": "application/json"}, timeout=20)
    if r.status_code != 200:
        # Error bodies are OAuth error codes; never log the request (it holds the secret / code).
        log.error("Tripleseat token request (%s) rejected: HTTP %s %s",
                  fields.get("grant_type"), r.status_code, r.text[:300])
        raise TripleseatNotConnected(f"Tripleseat token request failed (HTTP {r.status_code})")
    tok = r.json()
    if not tok.get("access_token"):
        raise TripleseatNotConnected("Tripleseat token response had no access_token")
    return tok


def save_tripleseat_token(cur, tok: dict, previous_refresh: Optional[str] = None):
    expires_in = int(tok.get("expires_in") or 7200)
    cur.execute("""
        INSERT INTO oauth_tokens (provider, access_token, refresh_token, expires_at, scope, updated_at)
        VALUES ('tripleseat', %s, %s, NOW() + make_interval(secs => %s), %s, NOW())
        ON CONFLICT (provider) DO UPDATE SET access_token = EXCLUDED.access_token,
            refresh_token = EXCLUDED.refresh_token, expires_at = EXCLUDED.expires_at,
            scope = COALESCE(EXCLUDED.scope, oauth_tokens.scope), updated_at = NOW()
    """, (tok["access_token"], tok.get("refresh_token") or previous_refresh, expires_in, tok.get("scope")))


def tripleseat_access_token(force_refresh: bool = False) -> str:
    """A valid Tripleseat access token, renewed when close to expiring. The row lock makes concurrent
    callers wait for one renewal instead of each spending the refresh token."""
    if not tripleseat_enabled():
        raise TripleseatNotConnected("TRIPLESEAT_CLIENT_ID / TRIPLESEAT_CLIENT_SECRET are not set")
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""SELECT access_token, refresh_token,
                                  EXTRACT(EPOCH FROM (expires_at - NOW())) AS seconds_left
                           FROM oauth_tokens WHERE provider = 'tripleseat' FOR UPDATE""")
            row = cur.fetchone()
            if not row:
                raise TripleseatNotConnected("Not connected: open /oauth/tripleseat/login")
            if not force_refresh and float(row["seconds_left"]) > TRIPLESEAT_REFRESH_MARGIN:
                return row["access_token"]
            if not row["refresh_token"]:
                raise TripleseatNotConnected("Token expired and no refresh token: open /oauth/tripleseat/login")
            tok = _tripleseat_token_request({"grant_type": "refresh_token", "refresh_token": row["refresh_token"]})
            save_tripleseat_token(cur, tok, previous_refresh=row["refresh_token"])
            log.info("Tripleseat access token renewed")
            return tok["access_token"]
    finally:
        conn.close()


def tripleseat_get(path: str, params: Optional[dict] = None) -> dict:
    """GET a Tripleseat API path (e.g. "/events/search.json"). Retries 429s (limit 10 requests/second) and
    renews the token once on a 401."""
    token = tripleseat_access_token()
    url = f"{TRIPLESEAT_API}/{path.lstrip('/')}"
    renewed = False
    for attempt in range(5):
        r = requests.get(url, params=params, timeout=30,
                         headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
        if r.status_code == 401 and not renewed:
            token, renewed = tripleseat_access_token(force_refresh=True), True
            continue
        if r.status_code == 429:
            time.sleep(min(float(r.headers.get("Retry-After") or 1), 10) * (attempt + 1))
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()
    raise RuntimeError(f"Tripleseat GET {path} kept failing (HTTP {r.status_code})")


def _oauth_page(title: str, message: str, status: int = 200) -> HTMLResponse:
    import html as _html
    return HTMLResponse(status_code=status, content=(
        "<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width'>"
        f"<title>{_html.escape(title)}</title><body style='font-family:system-ui;max-width:36em;margin:3em auto;"
        f"padding:0 1em'><h1>{_html.escape(title)}</h1><p>{_html.escape(message)}</p></body>"))


@app.get("/oauth/tripleseat/login")
def tripleseat_login(request: Request, key: Optional[str] = CRON_KEY_PARAM):
    """Open in a browser (with ?key=CRON_SECRET) to connect the server to Tripleseat."""
    require_cron_key(request)
    if not tripleseat_enabled():
        return _oauth_page("Tripleseat isn't set up yet",
                           "Add TRIPLESEAT_CLIENT_ID and TRIPLESEAT_CLIENT_SECRET in Render, then try again.", 503)
    state = make_tripleseat_state()
    resp = RedirectResponse(tripleseat_authorize_url(state), status_code=302)
    resp.set_cookie(TRIPLESEAT_STATE_COOKIE, state, max_age=TRIPLESEAT_STATE_MAX_AGE, httponly=True,
                    secure=True, samesite="lax", path="/oauth/tripleseat")
    return resp


def _finish_tripleseat_login(code: str):
    tok = _tripleseat_token_request({"grant_type": "authorization_code", "code": code,
                                     "redirect_uri": TRIPLESEAT_REDIRECT_URI})
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            save_tripleseat_token(cur, tok)
    finally:
        conn.close()
    log.info("Tripleseat connected (scope: %s)", tok.get("scope") or "app default")


@app.get("/oauth/tripleseat/callback")
async def tripleseat_callback(request: Request, code: Optional[str] = None, state: Optional[str] = None,
                              error: Optional[str] = None):
    """Tripleseat sends the browser here after the login; not called directly."""
    if not tripleseat_enabled():
        return _oauth_page("Tripleseat isn't set up yet", "TRIPLESEAT_CLIENT_SECRET is not set in Render.", 503)
    if error:
        return _oauth_page("Tripleseat login cancelled", f"Tripleseat answered: {error[:100]}", 400)
    if not tripleseat_state_valid(state or "", request.cookies.get(TRIPLESEAT_STATE_COOKIE, "")):
        return _oauth_page("Login link expired",
                           "Start again from /oauth/tripleseat/login in the same browser, within 10 minutes.", 400)
    if not code:
        return _oauth_page("Tripleseat login failed", "Tripleseat didn't send an authorization code.", 400)
    try:
        await run_in_threadpool(_finish_tripleseat_login, code)
    except TripleseatNotConnected:
        return _oauth_page("Tripleseat login failed", "Tripleseat refused the code. Check the Render logs.", 502)
    resp = _oauth_page("Tripleseat connected", "The server can now read Tripleseat. You can close this tab.")
    resp.delete_cookie(TRIPLESEAT_STATE_COOKIE, path="/oauth/tripleseat")
    return resp


@app.get("/cron/tripleseat-status")
def tripleseat_status(request: Request, check: bool = False, key: Optional[str] = CRON_KEY_PARAM):
    """Whether the server is connected to Tripleseat. check=true also makes one small API call."""
    require_cron_key(request)
    out = {"client_id_set": bool(TRIPLESEAT_CLIENT_ID), "client_secret_set": bool(TRIPLESEAT_CLIENT_SECRET)}
    if not tripleseat_enabled():
        return {**out, "connected": False, "next_step": "add TRIPLESEAT_CLIENT_ID and TRIPLESEAT_CLIENT_SECRET in Render"}
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""SELECT expires_at, scope, updated_at, refresh_token IS NOT NULL AS has_refresh
                           FROM oauth_tokens WHERE provider = 'tripleseat'""")
            row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return {**out, "connected": False, "next_step": "open /oauth/tripleseat/login?key=... in a browser"}
    out.update(connected=True, expires_at=row["expires_at"].isoformat(), scope=row["scope"],
               has_refresh_token=row["has_refresh"], token_saved_at=row["updated_at"].isoformat())
    if check:
        try:
            data = tripleseat_get("/sites.json")
            out["api_check"] = f"ok ({len(data) if isinstance(data, list) else 'data'} returned)"
        except Exception as e:
            out["api_check"] = f"failed: {type(e).__name__}: {str(e)[:200]}"
    return out


# ---------------------------------------------------------------------------
# Tripleseat API sync: bookings (with their events and payments) and leads. Links each to a customer
# (in_tripleseat, ts_* fields) and stores the records in tripleseat_bookings / _events / _leads
# (migrations/007). Never changes lifetime spend and never sends Meta events: Tripleseat money mostly
# settles through Square, and history isn't a live signal.
# ---------------------------------------------------------------------------
TRIPLESEAT_DATE_FORMAT = os.getenv("TRIPLESEAT_DATE_FORMAT", "%m/%d/%Y")  # search filter dates
TRIPLESEAT_PAGE_PAUSE = 0.15  # seconds between API pages (limit is 10 requests/second)


def ts_money(val) -> Optional[float]:
    if val in (None, ""):
        return None
    try:
        return round(float(str(val).replace(",", "").replace("$", "").strip()), 2)
    except ValueError:
        return None


def ts_date(val) -> Optional[str]:
    """ISO 'YYYY-MM-DD' from Tripleseat's ISO timestamps or 'MM/DD/YYYY' dates."""
    if not val or not isinstance(val, str):
        return None
    v = val.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}", v):
        return v[:10]
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", v)
    return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}" if m else None


def ts_timestamp(val) -> Optional[str]:
    return val if isinstance(val, str) and re.match(r"^\d{4}-\d{2}-\d{2}", val.strip()) else None


def ts_contact_identity(contact: dict, account: dict, fallback: dict = None) -> dict:
    """Name / email / phone / address from a Tripleseat contact (falling back to a lead's own fields)."""
    contact = contact if isinstance(contact, dict) else {}
    account = account if isinstance(account, dict) else {}
    fb = fallback or {}
    emails = [e.get("address") for e in contact.get("email_addresses") or [] if isinstance(e, dict) and e.get("address")]
    phones = [p.get("number") for p in contact.get("phone_numbers") or [] if isinstance(p, dict) and p.get("number")]
    addrs = [a for a in contact.get("addresses") or [] if isinstance(a, dict)]
    a = addrs[0] if addrs else {}
    return {
        "first_name": contact.get("first_name") or fb.get("first_name") or "",
        "last_name": contact.get("last_name") or fb.get("last_name") or "",
        "email": (emails[0] if emails else "") or fb.get("email_address") or "",
        "phone": (phones[0] if phones else "") or fb.get("phone_number") or "",
        "address": a.get("address1"), "city": a.get("city"), "state": a.get("state"),
        "zip": a.get("zip_code") or a.get("zip"),
        "ts_account": account.get("name") or fb.get("company") or None,
    }


def ts_payment_summary(payment_sets) -> dict:
    """Totals from a booking's payment_sets. Card payments go through Square (already in spend);
    paid_not_card is what was paid some other way (check, ACH, cash)."""
    if not isinstance(payment_sets, list):
        return {"payments": None, "paid_total": None, "paid_not_card": None, "refunded_total": None,
                "amount_due": None}
    payments, paid = [], 0.0
    not_card = refunded = due = 0.0
    for ps in payment_sets:
        if not isinstance(ps, dict):
            continue
        due += ts_money(ps.get("amount_due")) or 0.0
        for pay in ps.get("payments") or []:
            if not isinstance(pay, dict):
                continue
            amount = ts_money(pay.get("amount")) or 0.0
            back = ts_money(pay.get("amount_refunded")) or 0.0
            state = str(pay.get("state") or "").lower()
            method = str(pay.get("payment_method") or "")
            is_card = bool(pay.get("credit_card_type")) or "card" in method.lower()
            if state in ("paid", "refunded", "partially_refunded") or pay.get("paid_at"):
                net = max(amount - back, 0.0)
                paid += net
                refunded += back
                if not is_card:
                    not_card += net
            # Only what's needed to match payments later; no notes, references or payer names.
            payments.append({"id": pay.get("id"), "amount": amount, "state": state or None,
                             "paid_at": pay.get("paid_at"), "method": method or None, "card": is_card,
                             "refunded": back or None})
    return {"payments": payments, "paid_total": round(paid, 2), "paid_not_card": round(not_card, 2),
            "refunded_total": round(refunded, 2), "amount_due": round(due, 2)}


def _ts_lead_sources(obj: dict) -> list:
    out = []
    for s_ in obj.get("selected_lead_sources") or []:
        if isinstance(s_, dict):
            src = s_.get("lead_source") if isinstance(s_.get("lead_source"), dict) else {}
            name = src.get("name") or s_.get("lead_source_other")
            if name:
                out.append(name)
    return out


def _ts_link_customer(cur, ident: dict, extra: dict, apply: bool) -> tuple:
    """(customer or None, "created"|"matched"|"no_identity"). Dry runs only look the person up."""
    if not any(ident.get(k) for k in ("email", "phone", "first_name", "last_name")):
        return None, "no_identity"
    found = _find_existing(cur, "", (ident["email"] or "").strip().lower(), clean_phone(ident["phone"]),
                           ident["first_name"].strip(), ident["last_name"].strip())
    how = "matched" if found else "created"
    if not apply:
        return found, how
    # Records arrive in update order, not event order: keep the latest event date, never an older one.
    event_date = extra.pop("ts_last_event_date", None)
    cust, _ = upsert_and_enrich_customer(cur, {**ident, **extra, "in_tripleseat": 1, "source": "Tripleseat"})
    if event_date:
        cur.execute("""UPDATE master_customers SET ts_last_event_date = %s
                       WHERE cluster_id = %s AND (ts_last_event_date IS NULL
                             OR ts_last_event_date !~ '^\\d{4}-\\d{2}-\\d{2}' OR ts_last_event_date < %s)
                       RETURNING *;""", (event_date, cust["cluster_id"], event_date))
        updated = cur.fetchone()
        if updated:
            cust = rescore_customer(cur, updated)  # the booking's date counts for recency
    return cust, how


def _ts_known(cur, txn_ids: list) -> set:
    cur.execute("SELECT transaction_id FROM unified_transactions WHERE transaction_id = ANY(%s);", (txn_ids,))
    return {r["transaction_id"] for r in cur.fetchall()}


def ingest_tripleseat_booking(cur, booking: dict, count_it: bool, apply: bool = True) -> dict:
    bid = booking.get("id")
    if not bid:
        return {"status": "skipped_no_id"}
    events = [e for e in booking.get("events") or [] if isinstance(e, dict) and e.get("id")]
    status = str(booking.get("status") or "").upper() or None
    live_events = [e for e in events if str(e.get("status") or "").upper() != "LOST" and not e.get("deleted_at")]
    dates = [d for d in (ts_date(e.get("event_start_iso8601") or e.get("event_date")) for e in live_events) if d]
    guests = max([to_int(e.get("guest_count") or e.get("guaranteed_guest_count")) for e in live_events] or [0])
    lost = next((e.get("lost_reason") for e in events if e.get("lost_reason")), None) or booking.get("lost_reason")
    lost_name = lost.get("name") if isinstance(lost, dict) else lost
    ident = ts_contact_identity(booking.get("contact"), booking.get("account"))
    cust, how = _ts_link_customer(cur, ident, {
        "ts_lead_status": status, "ts_max_guests": guests or None,
        "ts_last_event_date": max(dates) if dates else None, "ts_turned_down_reason": lost_name,
    }, apply)
    money = ts_payment_summary(booking.get("payment_sets"))
    if not apply:
        return {"status": f"would_{how}", "has_payments": money["payments"] is not None}
    cid = cust["cluster_id"] if cust else None
    if cust:
        # One count per booking, and only if no webhook already recorded it or one of its events.
        txn_ids = [f"ts_booking_{bid}"] + [f"ts_event_{e['id']}" for e in events]
        first_time = not _ts_known(cur, txn_ids)
        for t in txn_ids:
            cust, _ = _count_record_once(cur, cust, t, "tripleseat", "ts_events", bump=False)
        if first_time and count_it and status != "LOST" and live_events:
            cur.execute("UPDATE master_customers SET ts_events = COALESCE(ts_events, 0) + 1 WHERE cluster_id = %s;",
                        (cid,))
    cur.execute("""
        INSERT INTO tripleseat_bookings (booking_id, cluster_id, status, name, account_name, start_date, end_date,
            market_segment, lead_sources, payments, paid_total, paid_not_card, refunded_total, amount_due,
            ts_created_at, ts_updated_at, synced_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (booking_id) DO UPDATE SET cluster_id = COALESCE(EXCLUDED.cluster_id, tripleseat_bookings.cluster_id),
            status = EXCLUDED.status, name = EXCLUDED.name, account_name = EXCLUDED.account_name,
            start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date, market_segment = EXCLUDED.market_segment,
            lead_sources = EXCLUDED.lead_sources,
            payments = COALESCE(EXCLUDED.payments, tripleseat_bookings.payments),
            paid_total = COALESCE(EXCLUDED.paid_total, tripleseat_bookings.paid_total),
            paid_not_card = COALESCE(EXCLUDED.paid_not_card, tripleseat_bookings.paid_not_card),
            refunded_total = COALESCE(EXCLUDED.refunded_total, tripleseat_bookings.refunded_total),
            amount_due = COALESCE(EXCLUDED.amount_due, tripleseat_bookings.amount_due),
            ts_created_at = EXCLUDED.ts_created_at, ts_updated_at = EXCLUDED.ts_updated_at, synced_at = NOW()
    """, (bid, cid, status, booking.get("name"), ident["ts_account"], ts_date(booking.get("start_date")),
          ts_date(booking.get("end_date")), booking.get("market_segment"), json.dumps(_ts_lead_sources(booking)),
          json.dumps(money["payments"]) if money["payments"] is not None else None, money["paid_total"],
          money["paid_not_card"], money["refunded_total"], money["amount_due"],
          ts_timestamp(booking.get("created_at")), ts_timestamp(booking.get("updated_at"))))
    for e in events:
        lr = e.get("lost_reason")
        cur.execute("""
            INSERT INTO tripleseat_events (event_id, booking_id, cluster_id, status, name, event_date, guest_count,
                event_type_id, grand_total, actual_amount, amount_due, deposit_amount, lost_reason,
                ts_created_at, ts_updated_at, synced_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (event_id) DO UPDATE SET booking_id = EXCLUDED.booking_id,
                cluster_id = COALESCE(EXCLUDED.cluster_id, tripleseat_events.cluster_id), status = EXCLUDED.status,
                name = EXCLUDED.name, event_date = EXCLUDED.event_date, guest_count = EXCLUDED.guest_count,
                event_type_id = EXCLUDED.event_type_id,
                grand_total = COALESCE(EXCLUDED.grand_total, tripleseat_events.grand_total),
                actual_amount = COALESCE(EXCLUDED.actual_amount, tripleseat_events.actual_amount),
                amount_due = COALESCE(EXCLUDED.amount_due, tripleseat_events.amount_due),
                deposit_amount = COALESCE(EXCLUDED.deposit_amount, tripleseat_events.deposit_amount),
                lost_reason = EXCLUDED.lost_reason, ts_created_at = EXCLUDED.ts_created_at,
                ts_updated_at = EXCLUDED.ts_updated_at, synced_at = NOW()
        """, (e["id"], bid, cid, str(e.get("status") or "").upper() or None, e.get("name"),
              ts_date(e.get("event_start_iso8601") or e.get("event_date")),
              to_int(e.get("guest_count") or e.get("guaranteed_guest_count")) or None, e.get("event_type_id"),
              ts_money(e.get("grand_total")), ts_money(e.get("actual_amount")), ts_money(e.get("amount_due")),
              ts_money(e.get("deposit_amount")), lr.get("name") if isinstance(lr, dict) else lr,
              ts_timestamp(e.get("created_at")), ts_timestamp(e.get("updated_at"))))
    return {"status": how if cust else "stored_without_customer", "has_payments": money["payments"] is not None}


def ingest_tripleseat_lead(cur, lead: dict, apply: bool = True) -> dict:
    lid = lead.get("id")
    if not lid:
        return {"status": "skipped_no_id"}
    ident = ts_contact_identity(lead.get("contact"), lead.get("account"), fallback=lead)
    status = "TURNED_DOWN" if lead.get("turned_down_at") else ("CONVERTED" if lead.get("converted_at") else "PROSPECT")
    event_date = ts_date(lead.get("event_date"))
    cust, how = _ts_link_customer(cur, ident, {
        "ts_max_guests": to_int(lead.get("guest_count")) or None, "ts_last_event_date": event_date,
        # A lead never downgrades a booking's status; only an unknown status is filled in.
        "ts_lead_status": None,
    }, apply)
    if not apply:
        return {"status": f"would_{how}"}
    cid = cust["cluster_id"] if cust else None
    if cust:
        cust, _ = _count_record_once(cur, cust, f"ts_lead_{lid}", "tripleseat", "ts_events", bump=False)
        cur.execute("UPDATE master_customers SET ts_lead_status = COALESCE(ts_lead_status, %s) WHERE cluster_id = %s;",
                    (status, cid))
    booking = lead.get("booking") if isinstance(lead.get("booking"), dict) else {}
    campaign = {k: lead.get(f"campaign_{k}") for k in ("source", "medium", "name", "term", "content")
                if lead.get(f"campaign_{k}")}
    cur.execute("""
        INSERT INTO tripleseat_leads (lead_id, cluster_id, booking_id, company, event_description, event_date,
            guest_count, email_opt_in, campaign, lead_sources, converted_at, turned_down_at, ts_created_at,
            ts_updated_at, synced_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (lead_id) DO UPDATE SET cluster_id = COALESCE(EXCLUDED.cluster_id, tripleseat_leads.cluster_id),
            booking_id = COALESCE(EXCLUDED.booking_id, tripleseat_leads.booking_id), company = EXCLUDED.company,
            event_description = EXCLUDED.event_description, event_date = EXCLUDED.event_date,
            guest_count = EXCLUDED.guest_count, email_opt_in = EXCLUDED.email_opt_in, campaign = EXCLUDED.campaign,
            lead_sources = EXCLUDED.lead_sources, converted_at = EXCLUDED.converted_at,
            turned_down_at = EXCLUDED.turned_down_at, ts_created_at = EXCLUDED.ts_created_at,
            ts_updated_at = EXCLUDED.ts_updated_at, synced_at = NOW()
    """, (lid, cid, booking.get("id"), ident["ts_account"], (lead.get("event_description") or "")[:2000] or None,
          event_date, to_int(lead.get("guest_count")) or None, lead.get("email_opt_in"), json.dumps(campaign),
          json.dumps(_ts_lead_sources(lead)), ts_timestamp(lead.get("converted_at")),
          ts_timestamp(lead.get("turned_down_at")), ts_timestamp(lead.get("created_at")),
          ts_timestamp(lead.get("updated_at"))))
    return {"status": how if cust else "stored_without_customer"}


def tripleseat_pages(path: str, params: dict, max_pages: int = 2000):
    """Yields every result of a paginated Tripleseat search, oldest-updated first where supported."""
    page = 1
    while page <= max_pages:
        body = tripleseat_get(path, {**params, "page": page})
        results = body.get("results") if isinstance(body, dict) else body
        for r in results or []:
            yield r
        total = body.get("total_pages") if isinstance(body, dict) else None
        if not results or not total or page >= int(total):
            return
        page += 1
        time.sleep(TRIPLESEAT_PAGE_PAUSE)


def run_tripleseat_sync(days_back: Optional[int] = 3, count_created_since: Optional[str] = None,
                        apply: bool = True) -> dict:
    """Reads bookings updated in the last `days_back` days (every booking when None) and leads created in
    that window, and stores them. A booking created before `count_created_since` (YYYY-MM-DD) is recorded
    without adding to ts_events; the history import passes today so old bookings aren't counted as new.
    apply=False only looks customers up and reports what it would do."""
    stats = collections.Counter()
    booking_params = {"show_financial": "true", "order": "updated_at", "sort_direction": "asc"}
    lead_params = {"order": "created_at", "sort_direction": "asc"}
    since = None
    if days_back is not None:
        since = date.fromordinal(date.today().toordinal() - max(0, int(days_back)))
        booking_params["booking_updated_start_date"] = since.strftime(TRIPLESEAT_DATE_FORMAT)
        lead_params["created_after"] = since.strftime(TRIPLESEAT_DATE_FORMAT)
    conn = get_conn()
    try:
        for kind, path, params in (("booking", "/bookings/search.json", booking_params),
                                   ("lead", "/leads/search.json", lead_params)):
            for rec in tripleseat_pages(path, params):
                if not isinstance(rec, dict):
                    continue
                stats[f"{kind}s_read"] += 1
                try:
                    with conn:
                        with conn.cursor() as cur:
                            if kind == "booking":
                                created = ts_date(rec.get("created_at")) or ""
                                count_it = not count_created_since or created >= count_created_since
                                res = ingest_tripleseat_booking(cur, rec, count_it, apply=apply)
                                if not res.get("has_payments", True):
                                    stats["bookings_without_payment_data"] += 1
                            else:
                                res = ingest_tripleseat_lead(cur, rec, apply=apply)
                    stats[f"{kind}_{res['status']}"] += 1
                except Exception as e:
                    stats[f"{kind}_failed"] += 1
                    log.error("Tripleseat %s %s not saved: %s: %s", kind, rec.get("id"), type(e).__name__, str(e)[:200])
    finally:
        conn.close()
    out = {"status": "tripleseat_sync_complete" if apply else "tripleseat_sync_dry_run",
           "since": since.isoformat() if since else "all", **stats}
    log.info("Tripleseat sync: %s", out)
    return out


def _run_tripleseat_sync_logged(*args, **kwargs):
    try:
        run_tripleseat_sync(*args, **kwargs)
    except TripleseatNotConnected as e:
        log.error("Tripleseat sync skipped: %s", e)
    except Exception as e:
        log.exception("Tripleseat sync failed: %s", type(e).__name__)


@app.post("/cron/sync-tripleseat")
def sync_tripleseat(request: Request, background_tasks: BackgroundTasks, days_back: int = 3,
                    history: bool = False, apply: bool = True, key: Optional[str] = CRON_KEY_PARAM):
    """Nightly: bookings updated and leads created in the last days_back days. history=true reads every
    booking and lead, without counting old bookings as new events; run it with apply=false first to see how
    many new customers it would create. Runs in the background; the totals appear in the logs as
    "Tripleseat sync: {...}"."""
    require_cron_key(request)
    if not tripleseat_enabled():
        return {"status": "error", "message": "Tripleseat isn't set up (TRIPLESEAT_CLIENT_ID / _SECRET)"}
    if history:
        background_tasks.add_task(_run_tripleseat_sync_logged, None, date.today().isoformat(), apply)
    else:
        background_tasks.add_task(_run_tripleseat_sync_logged, max(0, min(days_back, 400)), None, apply)
    return {"status": "tripleseat_sync_queued", "history": history, "apply": apply,
            "days_back": None if history else days_back}


# ---------------------------------------------------------------------------
# Dead letters (migrations/010): a webhook that fails is stored with its payload and retried by
# /cron/retry-webhooks, so a bug or an outage doesn't lose the record.
# ---------------------------------------------------------------------------
DEAD_LETTER_MAX_ATTEMPTS = 5


def save_dead_letter(source: str, raw, error: Exception, record_key: Optional[str] = None) -> Optional[int]:
    """Keeps a failed webhook's payload. Never raises: the webhook's own error is what gets reported.
    A re-sent identical payload is the same row, moved to now (it's the latest copy of its record)."""
    if not isinstance(raw, (dict, list)):
        return None
    try:
        body = json.dumps(raw, sort_keys=True, default=str)
        digest = hashlib.sha256(body.encode()).hexdigest()
        err = f"{type(error).__name__}: {error}"[:1000]
        conn = get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("""INSERT INTO webhook_dead_letters (source, payload, payload_hash, error, record_key)
                               VALUES (%s, %s::jsonb, %s, %s, %s)
                               ON CONFLICT (source, payload_hash) DO UPDATE
                               SET error = EXCLUDED.error, received_at = NOW(), resolved_at = NULL, result = NULL
                               RETURNING id;""", (source, body, digest, err, record_key))
                dl_id = cur.fetchone()["id"]
        finally:
            conn.close()
        log.error("%s webhook failed (%s); kept as dead letter %s for /cron/retry-webhooks",
                  source, type(error).__name__, dl_id)
        return dl_id
    except Exception as e:
        log.error("Could not keep the failed %s webhook as a dead letter: %s", source, type(e).__name__)
        return None


def supersede_dead_letters(source: str, record_key: Optional[str], before_sql: str, params: tuple = (),
                           cur=None) -> int:
    """Marks open dead letters of this record received before `before_sql` (when the newer copy arrived)
    as superseded: replaying them would overwrite newer data (e.g. bring a LOST event back to PROSPECT)."""
    if not record_key:
        return 0
    sql = f"""UPDATE webhook_dead_letters SET resolved_at = NOW(), result = '{{"status": "superseded"}}'::jsonb
              WHERE source = %s AND record_key = %s AND resolved_at IS NULL AND received_at < {before_sql};"""
    if cur is not None:
        cur.execute(sql, (source, record_key) + params)
        return cur.rowcount
    try:
        conn = get_conn()
        try:
            with conn, conn.cursor() as c:
                c.execute(sql, (source, record_key) + params)
                return c.rowcount
        finally:
            conn.close()
    except Exception as e:  # never fail a webhook that already succeeded
        log.warning("Could not check %s dead letters: %s", source, type(e).__name__)
        return 0


def _run_background_tasks(tasks: BackgroundTasks):
    for t in tasks.tasks:
        try:
            t.func(*t.args, **t.kwargs)
        except Exception as e:
            log.warning("Dead-letter retry follow-up task failed: %s", type(e).__name__)


def retry_dead_letters(limit: int = 50) -> dict:
    """Processes stored failed webhooks again, oldest first. A success is marked resolved; a failure
    counts an attempt and is given up after DEAD_LETTER_MAX_ATTEMPTS (still visible in the table)."""
    handlers = {"tripleseat": handle_tripleseat}
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""SELECT id, source, payload FROM webhook_dead_letters
                           WHERE resolved_at IS NULL AND attempts < %s ORDER BY id LIMIT %s;""",
                        (DEAD_LETTER_MAX_ATTEMPTS, limit))
            rows = cur.fetchall()
    finally:
        conn.close()
    resolved, failed, superseded = [], [], []
    for r in rows:
        # Only the newest open copy of a record is replayed; older ones are superseded by it.
        conn = get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("""SELECT resolved_at IS NULL AS still_open, record_key,
                                      EXISTS (SELECT 1 FROM webhook_dead_letters n
                                              WHERE n.source = d.source AND n.record_key = d.record_key
                                                AND n.id <> d.id AND n.resolved_at IS NULL
                                                AND n.received_at > d.received_at) AS newer_open
                               FROM webhook_dead_letters d WHERE id = %s;""", (r["id"],))
                state = cur.fetchone()
                if state["still_open"] and state["newer_open"]:
                    cur.execute("""UPDATE webhook_dead_letters SET resolved_at = NOW(),
                                   result = '{"status": "superseded"}'::jsonb WHERE id = %s;""", (r["id"],))
        finally:
            conn.close()
        if not state["still_open"] or state["newer_open"]:
            superseded.append(r["id"])
            continue
        handler = handlers.get(r["source"])
        tasks = BackgroundTasks()
        try:
            if handler is None:
                raise ValueError(f"no retry handler for {r['source']}")
            result, err = handler(r["payload"], tasks), None
        except Exception as e:
            result, err = None, f"{type(e).__name__}: {e}"[:1000]
        conn = get_conn()
        try:
            with conn, conn.cursor() as cur:
                if err is None:
                    cur.execute("""UPDATE webhook_dead_letters SET attempts = attempts + 1, last_attempt_at = NOW(),
                                   resolved_at = NOW(), result = %s::jsonb WHERE id = %s;""",
                                (json.dumps(result, default=str), r["id"]))
                    supersede_dead_letters(r["source"], state["record_key"],
                                           "(SELECT received_at FROM webhook_dead_letters WHERE id = %s)",
                                           (r["id"],), cur=cur)
                else:
                    cur.execute("""UPDATE webhook_dead_letters SET attempts = attempts + 1, last_attempt_at = NOW(),
                                   error = %s WHERE id = %s;""", (err, r["id"]))
        finally:
            conn.close()
        if err is None:
            _run_background_tasks(tasks)
            resolved.append(r["id"])
        else:
            log.warning("Dead letter %s (%s) failed again: %s", r["id"], r["source"], err.split(":")[0])
            failed.append(r["id"])
    return {"checked": len(rows), "resolved": resolved, "failed": failed, "superseded": superseded}


# --- Tripleseat webhook signature (X-Signature: t=<timestamp>,v1=<HMAC-SHA256>) ------------------------------
# Tripleseat documents the header shape (t=timestamp,v1=hash), HMAC-SHA256, the endpoint's signing secret, that
# "timestamps are part of the signed payload" and that the raw request body is signed, but NOT the exact signed
# string (which bytes, in what order, with what separators). Until a real delivery has verified, every candidate
# below is tried (check-only) and the one that matched is logged by name; enforcement then accepts only the named
# one (TRIPLESEAT_SIGNATURE_SCHEME), never a guess, and only if it signs the timestamp.
#
# A candidate name is "<payload>:<encoding>", plus "@<key form>" when the secret is used decoded rather than as
# its UTF-8 text: e.g. "t.body:hex", "t.url.body:base64@b64key". The six names checked before (t.body / tbody /
# body x hex / base64, raw key) are unchanged.
#
# Payloads: name -> (builder, signs the timestamp, provenance). A builder gets the delivery context and returns
# the signed bytes, or None when the candidate doesn't apply (no URL given, body isn't JSON).
_TS_URL_CANDIDATES = ("t.url.body", "t.urlnoq.body", "t.path.body", "t.pathq.body", "url.body", "urlnoq.body")


def _ts_json(ctx, rails=False):
    try:
        out = json.dumps(json.loads(ctx["body"]), separators=(",", ":"), ensure_ascii=False)
    except (ValueError, UnicodeDecodeError):
        return None
    if rails:  # ActiveSupport's to_json escapes these three for HTML safety
        out = out.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    try:
        return out.encode("utf-8")
    except UnicodeEncodeError:  # e.g. an escaped unpaired surrogate: not a candidate, never a 500
        return None


def _ts_join(*parts):
    return None if any(p is None for p in parts) else b"".join(parts)


TRIPLESEAT_SIGNATURE_PAYLOADS = {
    # --- checked since PR #35 ---
    "t.body": (lambda c: _ts_join(c["t"], b".", c["body"]), True,
               "convention (Stripe: '<t>.<raw body>'); matches the docs' 'timestamp is part of the signed payload'"),
    "tbody": (lambda c: _ts_join(c["t"], c["body"]), True, "guess: timestamp and raw body, no separator"),
    "body": (lambda c: c["body"], False, "convention (GitHub: raw body only); timestamp unsigned"),
    # --- separators / order ---
    "t:body": (lambda c: _ts_join(c["t"], b":", c["body"]), True, "guess: ':' separator"),
    "t-nl-body": (lambda c: _ts_join(c["t"], b"\n", c["body"]), True, "guess: newline separator"),
    "t-sp-body": (lambda c: _ts_join(c["t"], b" ", c["body"]), True, "guess: space separator"),
    "body.t": (lambda c: _ts_join(c["body"], b".", c["t"]), True, "guess: reversed order"),
    # --- trailing newline ---
    "t.body-rstrip": (lambda c: _ts_join(c["t"], b".", c["body"].rstrip(b"\r\n")), True,
                      "guess: raw body with a trailing newline stripped"),
    "t.body+nl": (lambda c: _ts_join(c["t"], b".", c["body"], b"\n"), True,
                  "guess: raw body with a trailing newline added"),
    "body-rstrip": (lambda c: c["body"].rstrip(b"\r\n"), False, "guess: raw body, trailing newline stripped"),
    # --- the URL in the signed string (Tripleseat's URL today carries ?key=) ---
    "t.url.body": (lambda c: _ts_join(c["t"], b".", c["url"], b".", c["body"]), True,
                   "guess: timestamp, full URL with query (?key=), body"),
    "t.urlnoq.body": (lambda c: _ts_join(c["t"], b".", c["urlnoq"], b".", c["body"]), True,
                      "guess: timestamp, URL without query, body"),
    "t.path.body": (lambda c: _ts_join(c["t"], b".", c["path"], b".", c["body"]), True,
                    "guess: timestamp, path, body"),
    "t.pathq.body": (lambda c: _ts_join(c["t"], b".", c["pathq"], b".", c["body"]), True,
                     "guess: timestamp, path with query, body"),
    "url.body": (lambda c: _ts_join(c["url"], c["body"]), False,
                 "convention (Square: notification URL + raw body); timestamp unsigned"),
    "urlnoq.body": (lambda c: _ts_join(c["urlnoq"], c["body"]), False,
                    "convention (Square-style) without the query; timestamp unsigned"),
    # --- re-serialised JSON instead of the raw bytes (the docs say raw; tried in case the sender signs its own
    #     serialisation and the transport differs) ---
    "t.json": (lambda c: _ts_join(c["t"], b".", _ts_json(c)), True, "guess: compact re-serialised JSON body"),
    "t.json-rails": (lambda c: _ts_join(c["t"], b".", _ts_json(c, rails=True)), True,
                     "guess: compact JSON as Rails to_json escapes it (< > & as \\u003c ...)"),
    # --- double hash: HMAC over the body's SHA-256 hex digest ---
    "t.bodyhash": (lambda c: _ts_join(c["t"], b".", hashlib.sha256(c["body"]).hexdigest().encode()), True,
                   "guess: timestamp + '.' + hex SHA-256 of the body"),
    "bodyhash": (lambda c: hashlib.sha256(c["body"]).hexdigest().encode(), False,
                 "guess: hex SHA-256 of the body; timestamp unsigned"),
}
# Not tried: a timestamp in another form than the header's (a verifier only ever has the header's t), other hash
# algorithms (the docs say HMAC SHA256).
TRIPLESEAT_SIGNATURE_ENCODINGS = ("hex", "base64", "base64url")  # base64url compared with or without '=' padding


def parse_tripleseat_signature(header: str) -> tuple:
    """(timestamp, [v1 signatures]) from 't=1700000000,v1=abc[,v1=def]'; (None, []) when unreadable."""
    t, sigs = None, []
    for part in (header or "").split(","):
        k, _, v = part.strip().partition("=")
        if k == "t" and v.strip():
            t = v.strip()
        elif k == "v1" and v.strip():
            sigs.append(v.strip())
    return t, sigs


def _ts_key_forms(secret: str) -> dict:
    """Key form -> key bytes. "" is the secret's UTF-8 text (the default everywhere, convention). The decoded forms
    are guesses, included only when the secret decodes that way."""
    forms = {"": secret.encode("utf-8")}
    for name, decode in (("b64key", lambda v: base64.b64decode(v, validate=True)),
                         ("b64urlkey", lambda v: base64.urlsafe_b64decode(v.rstrip("=") + "=" * (-len(v.rstrip("=")) % 4))),
                         ("hexkey", bytes.fromhex)):
        try:
            if name == "b64urlkey" and not re.fullmatch(r"[A-Za-z0-9_-]+={0,2}", secret):
                continue
            key = decode(secret)
        except ValueError:
            continue
        if key and key not in forms.values():
            forms[name] = key
    if secret.startswith("whsec_"):  # convention (Svix / Standard Webhooks): base64 after the whsec_ prefix
        try:
            forms["whsec"] = base64.b64decode(secret[len("whsec_"):], validate=True)
        except ValueError:
            pass
    return forms


def _ts_scheme_parts(scheme: str):
    """(payload, encoding, key form) for a valid candidate name, else None."""
    head, _, key_form = (scheme or "").partition("@")
    payload, _, enc = head.rpartition(":")
    if payload in TRIPLESEAT_SIGNATURE_PAYLOADS and enc in TRIPLESEAT_SIGNATURE_ENCODINGS and \
            key_form in ("", "b64key", "b64urlkey", "hexkey", "whsec"):
        return payload, enc, key_form
    return None


def tripleseat_scheme_signs_timestamp(scheme: str) -> bool:
    parts = _ts_scheme_parts(scheme)
    return bool(parts) and TRIPLESEAT_SIGNATURE_PAYLOADS[parts[0]][1]


def _ts_context(t: str, body: bytes, url: Optional[str]) -> dict:
    ctx = {"t": t.encode(), "body": body, "url": None, "urlnoq": None, "path": None, "pathq": None}
    if url:
        noq, _, query = url.partition("?")
        path = "/" + noq.split("://", 1)[-1].partition("/")[2]
        ctx.update(url=url.encode(), urlnoq=noq.encode(), path=path.encode(),
                   pathq=(path + ("?" + query if query else "")).encode())
    return ctx


def _ts_encode(digest: bytes, enc: str) -> str:
    if enc == "hex":
        return digest.hex()
    if enc == "base64":
        return base64.b64encode(digest).decode()
    return base64.urlsafe_b64encode(digest).decode().rstrip("=")


def _ts_matches(sig: str, expected: str, enc: str) -> bool:
    if enc == "hex":
        sig = sig.lower()
    elif enc == "base64url":
        sig = sig.rstrip("=")
    return hmac.compare_digest(sig.encode(), expected.encode())


def tripleseat_candidates_tried(secret: str, url: Optional[str], only: str = None) -> str:
    """A description of the candidate set, for the log: names only, never values."""
    if only:
        return only
    payloads = [p for p in TRIPLESEAT_SIGNATURE_PAYLOADS if url or p not in _TS_URL_CANDIDATES]
    keys = ["raw"] + [k for k in _ts_key_forms(secret) if k]
    return (f"{len(payloads) * len(TRIPLESEAT_SIGNATURE_ENCODINGS) * len(keys)} candidates = payloads "
            f"[{', '.join(payloads)}] x encodings [{', '.join(TRIPLESEAT_SIGNATURE_ENCODINGS)}] x keys "
            f"[{', '.join(keys)}]")


def tripleseat_signature_scheme(body: bytes, header: str, secret: str, only: str = None,
                                url: Optional[str] = None) -> Optional[str]:
    """The name ('t.body:hex', ...) of the first candidate this delivery's signature verifies with, or None.
    url is the URL Tripleseat posted to (query included); the URL candidates are skipped without it."""
    return next(tripleseat_signature_matches(body, header, secret, only, url), None)


def tripleseat_signature_matches(body: bytes, header: str, secret: str, only: str = None,
                                 url: Optional[str] = None):
    """Every candidate name this delivery's signature verifies with, in candidate order. More than one means the
    delivery can't tell them apart (e.g. a base64 signature with none of + / - _ is valid base64 and base64url)."""
    t, sigs = parse_tripleseat_signature(header)
    if not t or not sigs:
        return
    if only and not _ts_scheme_parts(only):
        return
    ctx = _ts_context(t, body, url)
    for key_form, key in _ts_key_forms(secret).items():
        for name, (build, _signs_t, _why) in TRIPLESEAT_SIGNATURE_PAYLOADS.items():
            msg = build(ctx)
            if msg is None:
                continue
            digest = hmac.new(key, msg, hashlib.sha256).digest()
            for enc in TRIPLESEAT_SIGNATURE_ENCODINGS:
                scheme = f"{name}:{enc}" + (f"@{key_form}" if key_form else "")
                if only and scheme != only:
                    continue
                expected = _ts_encode(digest, enc)
                if any(_ts_matches(sig, expected, enc) for sig in sigs):
                    yield scheme


def tripleseat_v1_shape(sig: str) -> str:
    """Length and alphabet of a received v1 value, never the value: tells whether it can be a SHA-256 HMAC at all
    (64 hex characters, 44 base64 characters with '=' padding, or 43 base64url characters)."""
    n = len(sig)
    if re.fullmatch(r"[0-9a-fA-F]+", sig):
        cls, sha256 = "hex", n == 64
    elif re.fullmatch(r"[A-Za-z0-9+/]+=*", sig):
        cls, sha256 = "base64", n == 44 or n == 43
    elif re.fullmatch(r"[A-Za-z0-9_-]+=*", sig):
        cls, sha256 = "base64url", n in (43, 44)
    else:
        cls, sha256 = "other", False
    return f"{n} chars {cls}" + (" (SHA-256 size)" if sha256 else " (not SHA-256 size)")


def tripleseat_secret_shape(secret: Optional[str], as_set: str) -> str:
    """Length and character class of the signing secret, and whether whitespace was trimmed from what was set in
    Render. Never the value, any part of it, or a hash of it."""
    if not secret:
        return "not set"
    if re.fullmatch(r"[0-9a-fA-F]+", secret):
        cls = "hex"
    elif re.fullmatch(r"[A-Za-z0-9]+", secret):
        cls = "alphanumeric"
    elif re.fullmatch(r"[A-Za-z0-9_-]+", secret):
        cls = "URL-safe (letters, digits, - _)"
    elif re.fullmatch(r"[A-Za-z0-9+/]+=*", secret):
        cls = "base64"
    else:
        cls = "other (contains characters outside letters, digits, - _ + / =)"
    trimmed = "yes" if as_set != secret else "no"
    return f"{len(secret)} characters, {cls}, whitespace trimmed from the Render value: {trimmed}"


def _tripleseat_public_url(request: Request) -> str:
    """The URL Tripleseat posted to, query included (Render terminates TLS, so the scheme comes from the proxy)."""
    proto = request.headers.get("x-forwarded-proto", request.url.scheme).split(",")[0].strip() or "https"
    host = request.headers.get("host") or request.url.netloc
    query = request.url.query
    return f"{proto}://{host}{request.url.path}" + (f"?{query}" if query else "")


def _signature_skew(t: Optional[str]) -> Optional[int]:
    """Seconds between the signed timestamp and our clock, or None when it isn't a usable number (nan, inf,
    1e999, text), so a malformed header follows the normal MISMATCH / 401 path instead of a 500."""
    try:
        ts = float(t)
        if not math.isfinite(ts):
            return None
        if ts > 1e12:  # milliseconds
            ts /= 1000.0
        return int(time.time() - ts)
    except (TypeError, ValueError, OverflowError):
        return None


def check_tripleseat_signature(request: Request, body: bytes) -> bool:
    """True when the delivery is authenticated by its signature (enforcement on and the signature valid).
    Otherwise logs the result (check-only) and returns False, so the shared key is still required; with
    enforcement on, a missing/invalid/stale signature is a 401. Never logs the header value or the secret."""
    if not TRIPLESEAT_WEBHOOK_SECRET:
        if TRIPLESEAT_SIGNATURE_ENFORCE:
            log.error("TRIPLESEAT_SIGNATURE_ENFORCE is on but TRIPLESEAT_WEBHOOK_SECRET is not set: refusing")
            raise HTTPException(status_code=401, detail="Unauthorized")
        return False
    # Only a construction that signs the timestamp can replace the shared key: with a body-only signature a captured
    # delivery could be replayed forever. Body-only candidates stay available for diagnostics (check-only).
    pinned_ok = tripleseat_scheme_signs_timestamp(TRIPLESEAT_SIGNATURE_SCHEME)
    enforce = TRIPLESEAT_SIGNATURE_ENFORCE and pinned_ok
    if TRIPLESEAT_SIGNATURE_ENFORCE and not pinned_ok:
        log.error("TRIPLESEAT_SIGNATURE_ENFORCE is on but TRIPLESEAT_SIGNATURE_SCHEME is %s: still check-only",
                  "not set" if not TRIPLESEAT_SIGNATURE_SCHEME else "not a timestamp-signed construction")
    header = request.headers.get("x-signature", "")
    t, sigs = parse_tripleseat_signature(header)
    skew = _signature_skew(t)
    url = _tripleseat_public_url(request)
    only = TRIPLESEAT_SIGNATURE_SCHEME if enforce else None
    matches = list(tripleseat_signature_matches(body, header, TRIPLESEAT_WEBHOOK_SECRET, only=only, url=url))
    scheme = matches[0] if matches else None
    ambiguous = ("; AMBIGUOUS, also matches %s: don't pin until a delivery matches only one" % ", ".join(matches[1:])
                 if len(matches) > 1 else "")
    fresh = skew is not None and abs(skew) <= TRIPLESEAT_SIGNATURE_MAX_SKEW_SECONDS
    if scheme and not tripleseat_scheme_signs_timestamp(scheme):
        log.info("Tripleseat signature verified (scheme %s: the timestamp is not signed, so it can't replace the "
                 "shared key%s); not enforced", scheme, ambiguous)
        return False
    if scheme and fresh:
        log.info("Tripleseat signature verified (scheme %s, timestamp skew %ss%s)%s", scheme, skew, ambiguous,
                 "" if enforce else "; not enforced yet")
        return enforce
    if not header:
        why = "MISSING (no X-Signature header; signature-like headers: %s)" % (
            ", ".join(sorted(h for h in request.headers.keys() if "sig" in h.lower())) or "none")
    elif not t or not sigs:
        why = "UNREADABLE (header present without t= and v1=)"
    elif scheme:
        why = f"STALE (scheme {scheme} matched but timestamp skew {skew}s exceeds {TRIPLESEAT_SIGNATURE_MAX_SKEW_SECONDS}s)"
    else:
        why = (f"MISMATCH (t present, {len(sigs)} v1 value(s) [{'; '.join(tripleseat_v1_shape(v) for v in sigs)}], "
               f"skew {skew}s, body {len(body)} bytes; none matched; "
               f"tried {tripleseat_candidates_tried(TRIPLESEAT_WEBHOOK_SECRET, url, only)})")
    log.warning("Tripleseat signature %s; %s", why, "rejected" if enforce else "not enforced yet")
    if enforce:
        raise HTTPException(status_code=401, detail="Invalid Tripleseat signature")
    return False


@app.post("/webhooks/tripleseat")
async def tripleseat_webhook(request: Request, background_tasks: BackgroundTasks):
    """Tripleseat Lead, Event, Booking, and Contact webhooks. Authenticated by Tripleseat's X-Signature once
    enforced; until then by the shared key (and the signature is checked and logged)."""
    body, raw = await read_json(request)
    if not check_tripleseat_signature(request, body):
        require_webhook_key(request)
    record_key = tripleseat_record_key(raw)
    arrived = datetime.now(timezone.utc)  # before processing: a slower request mustn't outrank a newer one
    try:
        result = await run_in_threadpool(handle_tripleseat, raw, background_tasks)
    except Exception as e:
        # Still a 500 (Tripleseat sees the failure), but the payload is kept for /cron/retry-webhooks.
        await run_in_threadpool(save_dead_letter, "tripleseat", raw, e, record_key)
        raise
    # This copy is newer than the failed copies of the same record saved before it arrived: those must not
    # be replayed. A copy that failed while this one was being processed arrived later, so it is kept.
    await run_in_threadpool(supersede_dead_letters, "tripleseat", record_key, "%s::timestamptz", (arrived,))
    return result


# ---------------------------------------------------------------------------
# Partywirks. With PARTYWIRKS_API_KEY set, a webhook is only a notification: the reservation and the
# parent's details are always read back from the Partywirks API, so a forged webhook can't create data
# and the field names are the documented ones. Without the key (or if the API is down) the webhook
# body itself is used, since Partywirks sends the same structure.
# Children's names, ages and form answers (customer_responses, event_name) are never stored.
# ---------------------------------------------------------------------------

def partywirks_api(path: str, params: dict = None):
    """Full JSON body from the Partywirks API; {} when the record doesn't exist; None when the API
    can't be used right now (no key, unreachable, error). Waits out rate limits (60 requests/min)."""
    if not PARTYWIRKS_API_KEY:
        return None
    for _ in range(3):
        try:
            r = requests.get(f"{PARTYWIRKS_API}{path}", params=params, timeout=15,
                             headers={"Authorization": f"Bearer {PARTYWIRKS_API_KEY}", "Accept": "application/json"})
        except Exception as e:
            log.warning("Partywirks API %s unreachable: %s", path.split("?")[0], type(e).__name__)
            return None
        if r.status_code == 429:
            time.sleep(min(max(to_int(r.headers.get("Retry-After"), 5), 1), 60))
            continue
        if r.status_code == 404:
            return {}
        if r.status_code != 200:
            log.warning("Partywirks API %s returned HTTP %s", path.split("?")[0], r.status_code)
            return None
        try:
            body = r.json()
        except ValueError:
            return None
        return body if isinstance(body, dict) else None
    log.warning("Partywirks API rate limit not clearing")
    return None


def _find_reservation(raw: dict) -> dict:
    for cand in (raw, raw.get("data"), raw.get("reservation"), raw.get("order")):
        if isinstance(cand, dict) and ("order_id" in cand or "event_date" in cand or "customer" in cand):
            return cand
    return raw if isinstance(raw, dict) else {}


def partywirks_record(res: dict, customer: dict = None) -> dict:
    """Normalises a Partywirks reservation (+ optional full customer record) into what we store."""
    res = res or {}
    brief = res.get("customer") if isinstance(res.get("customer"), dict) else {}
    full = customer or {}
    if isinstance(full.get("address"), dict):        # API customer record: nested address
        a = full["address"]
        addr = {"street": a.get("street_1"), "city": a.get("city"), "state": a.get("state"), "zip": a.get("zip_code")}
    else:                                              # webhook body: flat fields on the customer
        addr = {"street": brief.get("address") if isinstance(brief.get("address"), str) else None,
                "city": brief.get("city"), "state": brief.get("state"), "zip": brief.get("zip_code")}
    first = (full.get("first_name") or brief.get("first_name") or "").strip()
    last = (full.get("last_name") or brief.get("last_name") or "").strip()
    if not (first or last):
        name = (brief.get("name") or full.get("full_name") or "").strip()
        parts = name.split(None, 1)
        first, last = (parts[0] if parts else ""), (parts[1] if len(parts) > 1 else "")
    phone = clean_phone(full.get("phone") or brief.get("phone")) or clean_phone(full.get("alternate_phone"))
    activity = res.get("activity") if isinstance(res.get("activity"), dict) else {}
    return {
        "order_id": str(res.get("order_id") or res.get("id") or "").strip(),
        "customer_id": res.get("customer_id") or brief.get("id") or full.get("id"),
        "status": str(res.get("status") or "").strip().lower(),
        "email": (full.get("email") or brief.get("email") or "").strip().lower(),
        "phone": phone,
        "first_name": first,
        "last_name": last,
        "address": addr.get("street") or "",
        "city": addr.get("city") or "",
        "state": addr.get("state") or "",
        "zip": str(addr.get("zip") or "")[:5],
        "package": (activity.get("name") or "BIRTHDAY PARTY")[:100],
        "party_date": date_only(res.get("event_date")),
        "created_at": res.get("created_at"),
        "total": _partywirks_total(res),
    }


def _partywirks_total(res: dict) -> float:
    """The reservation's real order total (totals.total, incl. tax), or 0 when Partywirks didn't give one."""
    totals = res.get("totals") if isinstance(res.get("totals"), dict) else {}
    try:
        return max(float(totals.get("total") or 0), 0.0)
    except (TypeError, ValueError):
        return 0.0


def fetch_partywirks_record(order_id: str):
    """Reservation + parent from the API. {} if Partywirks has no such reservation, None if unavailable."""
    body = partywirks_api(f"/reservations/{order_id}")
    if body is None or body == {}:
        return body
    res = body.get("data") or {}
    if not res:
        return {}
    customer, complete = None, True
    if res.get("customer_id"):
        cbody = partywirks_api(f"/customers/{res['customer_id']}")
        customer = (cbody or {}).get("data") if cbody else None
        complete = cbody is not None  # None = API trouble (a 404 just means no extra details)
    rec = partywirks_record(res, customer)
    rec["customer_complete"] = complete
    return rec


def record_partywirks_spend(cur, cluster_id, txn_id: str, total: float) -> Optional[dict]:
    """Puts a Partywirks order's real total on its ledger row and adds it to the customer's pw_spend, once per
    order (only while the row is still at $0). true_lifetime_spend becomes the max of the per-system totals,
    never the sum. Returns the updated customer, or None when nothing changed (no total, already recorded)."""
    total = round(float(total or 0), 2)
    if total <= 0 or not cluster_id:
        return None
    cur.execute("UPDATE unified_transactions SET total_amount = %s WHERE transaction_id = %s AND cluster_id = %s "
                "AND source_system = 'partywirks' AND total_amount = 0 RETURNING transaction_id;",
                (total, txn_id, cluster_id))
    if not cur.fetchone():
        return None
    return _set_partywirks_spend(cur, cluster_id, total)


def reverse_partywirks_spend(cur, txn_id: str) -> Optional[dict]:
    """A cancelled order's total comes off the customer's pw_spend (its ledger row goes back to $0)."""
    cur.execute("SELECT cluster_id, total_amount FROM unified_transactions WHERE transaction_id = %s "
                "AND source_system = 'partywirks' AND total_amount > 0 FOR UPDATE;", (txn_id,))
    row = cur.fetchone()
    if not row or not row["cluster_id"]:
        return None
    cur.execute("UPDATE unified_transactions SET total_amount = 0 WHERE transaction_id = %s;", (txn_id,))
    return _set_partywirks_spend(cur, row["cluster_id"], -float(row["total_amount"]))


def _set_partywirks_spend(cur, cluster_id, delta: float) -> dict:
    cur.execute("""UPDATE master_customers SET
                       pw_spend = GREATEST(0, COALESCE(pw_spend, 0) + %(d)s),
                       true_lifetime_spend = GREATEST(COALESCE(sq_spend, 0), COALESCE(wr_spend, 0),
                                                      GREATEST(0, COALESCE(pw_spend, 0) + %(d)s)),
                       updated_at = NOW()
                   WHERE cluster_id = %(cid)s RETURNING *;""", {"d": delta, "cid": cluster_id})
    return cur.fetchone()


def pw_party_date(party_date) -> Optional[str]:
    """A Partywirks party date as ISO YYYY-MM-DD, or None when it's missing or unreadable."""
    return ts_date(str(party_date)) if party_date else None


def set_pw_party_date(cur, txn_id: str, party_date) -> bool:
    """Puts the party date on a Partywirks ledger row, or moves it when the party was rescheduled. An unknown
    date never overwrites a known one. Doesn't count the booking again or resend its Qualified_Bowler."""
    d = pw_party_date(party_date)
    if not d:
        return False
    cur.execute("UPDATE unified_transactions SET party_date = %s::date WHERE transaction_id = %s "
                "AND source_system = 'partywirks' AND party_date IS DISTINCT FROM %s::date RETURNING 1;",
                (d, txn_id, d))
    return cur.fetchone() is not None


def ingest_partywirks(cur, rec: dict, background_tasks: BackgroundTasks = None, count_booking: bool = True) -> dict:
    """Saves one Partywirks reservation. A booking is counted (and its Qualified_Bowler sent) once per
    order; cancelled reservations update contact details only. count_booking=False marks the order as
    seen without counting it (used when backfilling bookings the original import already counted)."""
    if not any([rec.get("email"), rec.get("phone"), rec.get("first_name")]):
        return {"status": "ignored_empty_partywirks_reservation"}
    cancelled = rec.get("status") == "cancelled"
    cust, _ = upsert_and_enrich_customer(cur, {
        "first_name": rec.get("first_name"),
        "last_name": rec.get("last_name"),
        "email": rec.get("email"),
        "phone": rec.get("phone"),
        "address": rec.get("address"),
        "city": rec.get("city"),
        "state": rec.get("state"),
        "zip": rec.get("zip"),
        "in_partywirks": 1,
        "pw_last_package": None if cancelled else rec.get("package"),
        "pw_last_party_date": None if cancelled else rec.get("party_date"),
        "source": "Partywirks",
    })
    if cancelled:
        if rec.get("order_id") and reverse_partywirks_spend(cur, f"pw_{rec['order_id']}"):
            rescore_customer(cur, cust)
        return {"status": "partywirks_cancelled_reservation", "cluster_id": cust["cluster_id"]}

    key = rec.get("order_id") or f"c{cust['cluster_id']}_{rec.get('party_date') or 'undated'}"
    cust, is_new = _count_record_once(cur, cust, f"pw_{key}", "partywirks", "pw_bookings", bump=count_booking)
    # The ledger row carries the party date (kept current when the party is rescheduled; NULL while unknown), so
    # a Tripleseat re-entry of the same party can be matched to it (party_qb_twin).
    set_pw_party_date(cur, f"pw_{key}", rec.get("party_date"))
    # The order's real total is the booker's spend (often their only spend: the Square deposit is usually not
    # linked to them). Recorded once per order, including bookings first seen before totals were stored.
    spent = record_partywirks_spend(cur, cust["cluster_id"], f"pw_{key}", rec.get("total"))
    if spent:
        cust = rescore_customer(cur, spent)

    thresh = get_threshold(cur)
    if not is_new:
        retry_unsent_event(cur, f"qb_pw_{key}")  # a Meta failure last time gets another try
    elif count_booking and not is_detroit_excluded(cust.get("md_city"), cust.get("zip5")):
        if cust["bowler_value_score"] >= thresh or cust.get("enrich_after"):
            # The real order total, or no value. Not a Purchase: the deposit/balance arrive as Square payments,
            # so a Partywirks Purchase would need the same twin check as WebRes (purchase_twin) first.
            # When the order was placed (Partywirks local time), never the party date, which is usually ahead.
            fire_meta_capi_event(cur, cust, "Qualified_Bowler", rec.get("total") or 0.0, thresh,
                                 event_id=f"qb_pw_{key}",
                                 event_time=parse_source_time(rec.get("created_at"), naive_tz=VENUE_TZ))
        if background_tasks is not None:
            background_tasks.add_task(sync_customer_to_meta_custom_audiences, dict(cust))
        else:
            sync_customer_to_meta_custom_audiences(dict(cust))  # catch-up/backfill: no request to defer to
    return {
        "status": "partywirks_synced" if is_new else "partywirks_already_recorded",
        "cluster_id": cust["cluster_id"],
        "package": rec.get("package"),
        "score": cust["bowler_value_score"],
    }


def handle_partywirks(raw: dict, background_tasks: BackgroundTasks) -> dict:
    payload_rec = partywirks_record(_find_reservation(raw))
    rec = None
    if PARTYWIRKS_API_KEY and payload_rec["order_id"]:
        rec = fetch_partywirks_record(payload_rec["order_id"])
        if rec == {}:
            # Partywirks has no such reservation: not a real booking, so don't store anything.
            return {"status": "ignored_unknown_partywirks_reservation"}
    if not rec:
        # No API key, or the API is down: use the webhook body, but only for a real order ID, so a
        # Partywirks test/sample delivery ("John Doe", example.com) is never saved.
        if not REAL_PARTYWIRKS_ORDER_ID.match(payload_rec["order_id"]):
            log.warning("Partywirks webhook with non-standard order ID ignored (test delivery?)")
            return {"status": "ignored_partywirks_test_or_unknown_order"}
        rec = payload_rec

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                return ingest_partywirks(cur, rec, background_tasks)
    finally:
        conn.close()


@app.post("/webhooks/partywirks")
async def partywirks_webhook(request: Request, background_tasks: BackgroundTasks):
    """Partywirks reservation webhooks."""
    require_webhook_key(request)
    body, raw = await read_json(request)
    check_partywirks_signature(request, body)
    return await run_in_threadpool(handle_partywirks, raw, background_tasks)


def partywirks_signature_matches(body: bytes, provided: str, secret: str) -> bool:
    """HMAC-SHA256 of the raw body with the signature key. Partywirks documents the algorithm and
    header but not the encoding, so hex or base64 (optionally prefixed 'sha256=') are accepted."""
    provided = (provided or "").strip()
    if provided.lower().startswith("sha256="):
        provided = provided[7:]
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).digest()
    for expected in (digest.hex(), base64.b64encode(digest).decode()):
        if hmac.compare_digest(provided.encode(), expected.encode()):
            return True
    return hmac.compare_digest(provided.lower().encode(), digest.hex().encode())


def check_partywirks_signature(request: Request, body: bytes):
    if not PARTYWIRKS_WEBHOOK_SECRET:
        return
    provided = request.headers.get("x-partywirks-signature", "")
    ok = bool(provided) and partywirks_signature_matches(body, provided, PARTYWIRKS_WEBHOOK_SECRET)
    if ok:
        log.info("Partywirks signature verified")
        return
    log.warning("Partywirks signature %s (header %s, %s)", "MISMATCH" if provided else "MISSING",
                "present" if provided else "absent", "rejected" if PARTYWIRKS_SIGNATURE_ENFORCE else "not enforced yet")
    if PARTYWIRKS_SIGNATURE_ENFORCE:
        raise HTTPException(status_code=401, detail="Invalid Partywirks signature")


def run_partywirks_sync(days_back: int = 3, count_bookings_created_since: str = None, apply: bool = True,
                        pause_seconds: float = 1.1) -> dict:
    """Catch-up: lists reservations with a party date from `days_back` days ago onward and saves any
    we haven't recorded yet (missed webhooks, outages). Bookings created before
    `count_bookings_created_since` ('YYYY-MM-DD HH:MM:SS', Partywirks' own clock) are recorded without
    being counted. With apply=False nothing is fetched beyond the reservation list or written."""
    if not PARTYWIRKS_API_KEY:
        return {"status": "error", "message": "Missing PARTYWIRKS_API_KEY"}
    date_from = date.fromordinal(date.today().toordinal() - max(0, int(days_back))).isoformat()
    reservations, page = [], 1
    while True:
        body = partywirks_api("/reservations", {"date_from": date_from, "per_page": 100, "page": page})
        if not body:
            return {"status": "error", "message": f"Partywirks reservation list unavailable (page {page})"}
        reservations += body.get("data") or []
        if not (body.get("pagination") or {}).get("has_more"):
            break
        page += 1

    conn = get_conn()
    stats = collections.Counter()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT transaction_id FROM unified_transactions WHERE source_system = 'partywirks';")
            known = {r["transaction_id"] for r in cur.fetchall()}
        conn.commit()
        for res in reservations:
            oid = str(res.get("order_id") or "")
            stats["listed"] += 1
            if not oid or f"pw_{oid}" in known:
                stats["already_recorded"] += 1
                continue
            if str(res.get("status") or "").lower() == "cancelled":
                stats["cancelled_skipped"] += 1
                continue
            count_it = not count_bookings_created_since or str(res.get("created_at") or "") >= count_bookings_created_since
            stats["new_counted" if count_it else "new_recorded_without_counting"] += 1
            if not apply:
                continue
            rec = fetch_partywirks_record(oid)
            time.sleep(pause_seconds)  # two API calls per reservation; stay under 60/min
            if not rec or not rec.get("customer_complete", True):
                stats["api_lookup_failed_retry_next_run"] += 1  # not recorded, so the next run tries again
                continue
            with conn:
                with conn.cursor() as cur:
                    result = ingest_partywirks(cur, rec, None, count_booking=count_it)
            stats[result["status"]] += 1
    finally:
        conn.close()
    out = {"status": "partywirks_sync_complete" if apply else "partywirks_sync_dry_run",
           "date_from": date_from, **stats}
    log.info("Partywirks sync: %s", out)
    return out


# ---------------------------------------------------------------------------
# Deferred enrichment worker (runs inside the server every minute; see ENRICH_DELAY_MINUTES)
# ---------------------------------------------------------------------------
_VERSIUM_MARKERS = ("_versium_checked", "_versium_contact_done", "_versium_demo_done")


def versium_queryable(cust: dict) -> bool:
    """Whether there's anything to look up: the same test run_melissa_and_versium_waterfall applies before
    calling a provider (an email, a phone, a street, or a full name with a ZIP)."""
    c = cust or {}
    return any([str(c.get("email") or "").strip(), clean_phone(c.get("phone")), str(c.get("md_address1") or "").strip(),
                c.get("first_name") and c.get("last_name") and c.get("zip5")])


def enrichment_complete(cust: dict) -> bool:
    """True when the customer's score can be trusted for a drop: Versium has answered (match or no match, live or
    the CSV import), or Versium doesn't apply (an excluded area, nothing to look up, or a deployment without
    Versium). Nothing to look up is final: re-queueing such a customer would never change the answer."""
    if versium_checked(cust) or not VERSIUM_REQUIRED or not versium_queryable(cust):
        return True
    return is_detroit_excluded(cust.get("md_city"), cust.get("zip5"))


def release_deferred_events(cur, cust: dict, exhausted: bool = False) -> collections.Counter:
    """Sends the Meta events held while the customer waited for enrichment, now that the address and score are
    known: Purchase if not in an excluded area; Qualified_Bowler and Lead also need the score threshold.

    A Qualified_Bowler / Lead is only dropped as below_threshold once enrichment finished (enrichment_complete):
    a score missing Versium's data is understated. If enrichment hasn't finished, it stays held and the customer
    is queued for enrichment again; after the worker's last attempt (exhausted) it's dropped as
    enrichment_exhausted, which the Versium catch-up re-evaluates once Versium answers (reevaluate_exhausted)."""
    out = collections.Counter()
    cur.execute("""SELECT * FROM capi_events_log WHERE cluster_id = %s AND NOT sent_to_meta
                   AND meta_response_status = 'DEFERRED_ENRICHMENT' ORDER BY sent_at FOR UPDATE;""",
                (cust["cluster_id"],))
    rows = cur.fetchall()
    if not rows:
        return out
    thresh = get_threshold(cur)
    excluded = is_detroit_excluded(cust.get("md_city"), cust.get("zip5"))
    score = int(cust.get("bowler_value_score") or 0)
    cur.execute("UPDATE capi_events_log SET bowler_value_score = %s, threshold_at_send = %s "
                "WHERE event_id = ANY(%s);", (score, thresh, [r["event_id"] for r in rows]))
    complete = enrichment_complete(cust)
    requeue = False
    for r in rows:
        reason = "excluded_area" if excluded else (
            "below_threshold" if r["event_name"] in ("Qualified_Bowler", "Lead") and score < thresh else None)
        if reason == "below_threshold" and not complete:
            if not exhausted:
                requeue = True
                out["held_enrichment_incomplete"] += 1
                continue
            reason = "enrichment_exhausted"
        if reason:
            cur.execute("UPDATE capi_events_log SET meta_response_status = 'DROPPED_AFTER_ENRICHMENT', "
                        "meta_response_body = %s WHERE event_id = %s;", (reason, r["event_id"]))
            out[f"dropped_{reason}"] += 1
            continue
        fire_meta_capi_event(cur, cust, r["event_name"], float(r["event_value"] or 0), thresh, r["event_id"],
                             action_source=r.get("meta_response_body") or _action_source_for(r["event_id"]))
        out["sent"] += 1
    if requeue:
        # Never enriched (or enrichment didn't finish): enrich again; the worker releases the events afterwards.
        cur.execute("UPDATE master_customers SET enrich_after = NOW() + INTERVAL '1 minute' "
                    "WHERE cluster_id = %s AND enrich_after IS NULL;", (cust["cluster_id"],))
    return out


def reevaluate_exhausted(cur, cust: dict) -> collections.Counter:
    """After a later enrichment (the Versium catch-up) finished for this customer, events dropped as
    enrichment_exhausted that Meta still accepts are judged again on the now-complete score: sent, or dropped
    as below_threshold / excluded_area."""
    if not enrichment_complete(cust):
        return collections.Counter()
    cur.execute("""UPDATE capi_events_log SET meta_response_status = 'DEFERRED_ENRICHMENT', meta_response_body = NULL
                   WHERE cluster_id = %s AND NOT sent_to_meta AND meta_response_status = 'DROPPED_AFTER_ENRICHMENT'
                     AND meta_response_body = 'enrichment_exhausted'
                     AND COALESCE(event_time, sent_at) > NOW() - make_interval(days => %s)
                   RETURNING event_id;""", (cust["cluster_id"], RETRY_MAX_AGE_DAYS))
    if not cur.fetchall():
        return collections.Counter()
    return release_deferred_events(cur, cust)


def record_enrichment_outcome(cur, cluster_id, enr: dict, attempt: int, final: str):
    """Which providers answered or failed for this customer, after which attempt, and how it ended, readable
    from the database (master_customers.enrich_outcome, migrations/015)."""
    rec = {"at": datetime.now(timezone.utc).isoformat(), "attempt": attempt, "final": final,
           **{k: v for k, v in (enr.get("_outcome") or {}).items() if v}}
    cur.execute("UPDATE master_customers SET enrich_outcome = %s::jsonb WHERE cluster_id = %s;",
                (json.dumps(rec), cluster_id))


def _claim_pending(conn):
    """Takes the customer whose waiting window ended first; the 10-minute lease stops a second worker."""
    with conn, conn.cursor() as cur:
        cur.execute("""UPDATE master_customers
                       SET enrich_after = NOW() + INTERVAL '10 minutes', enrich_attempts = enrich_attempts + 1
                       WHERE cluster_id = (SELECT cluster_id FROM master_customers
                                           WHERE enrich_after IS NOT NULL AND enrich_after <= NOW()
                                             AND NOT (COALESCE(versium_lifestyle_flags, '{}'::jsonb) ? '_versium_pending'
                                                  AND (versium_lifestyle_flags->>'_versium_pending')::float
                                                      > extract(epoch FROM now()) - 3600)
                                           ORDER BY enrich_after LIMIT 1 FOR UPDATE SKIP LOCKED)
                       RETURNING *;""")
        rec = cur.fetchone()
        if rec and (rec.get("enrich_inputs") or {}).get("_retry"):
            # The one retry with new details: forget the old "already looked up" markers, once.
            cur.execute(f"""UPDATE master_customers
                            SET versium_lifestyle_flags = (COALESCE(versium_lifestyle_flags, '{{}}'::jsonb)
                                    - ARRAY{list(_VERSIUM_MARKERS)}::text[])
                                    || jsonb_build_object('_versium_retried', %s::text),
                                enrich_inputs = enrich_inputs - '_retry'
                            WHERE cluster_id = %s RETURNING *;""", (date.today().isoformat(), rec["cluster_id"]))
            rec = cur.fetchone()
        return rec


def enrich_pending_customer(conn, rec: dict) -> str:
    inputs = rec.get("enrich_inputs") or {}
    pick = lambda col, key: str(rec.get(col) or inputs.get(key) or "").strip()
    retried = bool((rec.get("versium_lifestyle_flags") or {}).get("_versium_retried"))
    # A retry is about the new details, so they go first; otherwise the merged record's own values do.
    email = str(inputs.get("email") or rec.get("email") or "") if retried else pick("email", "email")
    phone = str(inputs.get("phone") or rec.get("phone") or "") if retried else pick("phone", "phone")
    try:
        enr = run_melissa_and_versium_waterfall(
            rec.get("first_name") or "", rec.get("last_name") or "", email, phone,
            pick("md_address1", "address"), pick("md_city", "city"), pick("md_state", "state"),
            pick("zip5", "zip"), known=rec, unhurried=True)
    except Exception as e:
        log.exception("Deferred enrichment of a customer failed: %s", type(e).__name__)
        enr = {"_versium_failed": True}
    failed = bool(enr.get("_versium_failed"))
    with conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM master_customers WHERE cluster_id = %s FOR UPDATE;", (rec["cluster_id"],))
        cust = save_enrichment(cur, rec["cluster_id"], enr, "enrichment")  # keeps whatever was paid for
        if failed and rec["enrich_attempts"] < ENRICH_MAX_ATTEMPTS:
            record_enrichment_outcome(cur, rec["cluster_id"], enr, rec["enrich_attempts"], "retry_later")
            cur.execute("UPDATE master_customers SET enrich_after = NOW() + make_interval(mins => %s) "
                        "WHERE cluster_id = %s;", (15 * rec["enrich_attempts"], rec["cluster_id"]))
            return "retry_later"
        cur.execute("SELECT enrich_inputs, versium_lifestyle_flags FROM master_customers WHERE cluster_id = %s;",
                    (rec["cluster_id"],))
        now_row = cur.fetchone()
        strip = lambda d: {k: v for k, v in (d or {}).items() if k != "_retry"}
        if strip(now_row["enrich_inputs"]) != strip(inputs):
            # A webhook added details while we were looking up. If Versium found nothing, look again with
            # them (once, as a retry); if it matched, there's nothing more to buy.
            unmatched = not versium_matched({"versium_lifestyle_flags": now_row["versium_lifestyle_flags"]})
            if unmatched and not (now_row["versium_lifestyle_flags"] or {}).get("_versium_retried"):
                cur.execute("""UPDATE master_customers SET enrich_after = NOW() + INTERVAL '1 minute', enrich_attempts = 0,
                                   enrich_inputs = enrich_inputs || '{"_retry": true}'::jsonb
                               WHERE cluster_id = %s;""", (rec["cluster_id"],))
                return "rerun_new_details"
        record_enrichment_outcome(cur, rec["cluster_id"], enr, rec["enrich_attempts"],
                                  "gave_up_versium" if failed else "enriched")
        cur.execute("UPDATE master_customers SET enrich_after = NULL, enrich_inputs = NULL, enrich_attempts = 0 "
                    "WHERE cluster_id = %s RETURNING *;", (rec["cluster_id"],))
        cust = rescore_customer(cur, cur.fetchone(), exact=True)
        # After the last attempt (failed): an event that would be dropped on this understated score is dropped as
        # enrichment_exhausted, which the Versium catch-up re-evaluates.
        released = release_deferred_events(cur, cust, exhausted=failed)
        if not failed:
            # Events dropped as enrichment_exhausted on an earlier run are judged again now that it finished
            # (the Versium catch-up does the same for customers the worker doesn't see again).
            released.update(reevaluate_exhausted(cur, cust))
        qualified = not is_detroit_excluded(cust.get("md_city"), cust.get("zip5"))  # seeds don't need the score
    if qualified:
        try:
            sync_customer_to_meta_custom_audiences(dict(cust))
        except Exception as e:
            log.warning("Audience sync after enrichment failed: %s", type(e).__name__)
    if released:
        log.info("Deferred Meta events for a customer: %s", dict(released))
    return "gave_up_versium" if failed else "enriched"


def process_pending_enrichment(limit: int = ENRICH_BATCH) -> dict:
    """Enriches up to `limit` customers whose waiting window has ended, then releases any held events left
    for customers no longer waiting (a webhook that raced the worker)."""
    stats = collections.Counter()
    conn = get_conn()
    try:
        for _ in range(limit):
            rec = _claim_pending(conn)
            if not rec:
                break
            stats[enrich_pending_customer(conn, rec)] += 1
        with conn, conn.cursor() as cur:
            cur.execute("""SELECT DISTINCT l.cluster_id FROM capi_events_log l
                           JOIN master_customers m ON m.cluster_id = l.cluster_id
                           WHERE NOT l.sent_to_meta AND l.meta_response_status = 'DEFERRED_ENRICHMENT'
                             AND m.enrich_after IS NULL LIMIT 50;""")
            for r in cur.fetchall():
                cur.execute("SELECT * FROM master_customers WHERE cluster_id = %s;", (r["cluster_id"],))
                stats.update(release_deferred_events(cur, cur.fetchone()))
    finally:
        conn.close()
    if stats:
        log.info("Deferred enrichment: %s", dict(stats))
    return dict(stats)


# ---------------------------------------------------------------------------
# Cron endpoints (plain `def`, so FastAPI runs them in a worker thread)
# ---------------------------------------------------------------------------

@app.post("/cron/enrich-partywirks")
def enrich_partywirks_customers(request: Request, key: Optional[str] = CRON_KEY_PARAM):
    """Runs un-enriched Partywirks parents through Melissa (+Versium) and recalculates persona & score."""
    require_cron_key(request)
    if not MELISSA_API_KEY:
        return {"status": "error", "message": "Missing MELISSA_API_KEY"}

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM master_customers
                    WHERE in_partywirks = 1 AND md_address1 IS NULL AND md_results IS NULL
                    ORDER BY cluster_id;
                    """
                )
                rows = cur.fetchall()

        processed = address_matched = pg2_count = lookup_failures = 0
        for r in rows:
            enr = run_melissa_and_versium_waterfall(
                r.get("first_name") or "", r.get("last_name") or "",
                r.get("email") or "", r.get("phone") or "", known=r,
            )
            if enr.get("md_results") is None:
                lookup_failures += 1  # Melissa didn't answer; leave it for the next run
                with conn, conn.cursor() as cur:
                    save_online_audience(cur, r["cluster_id"], enr.get("online_audience"))  # already paid for
                continue
            processed += 1
            if enr.get("md_address1"):
                address_matched += 1
            with conn:
                with conn.cursor() as cur:
                    cust = save_enrichment(cur, r["cluster_id"], enr, "Partywirks",
                                           extra={"md_presence_of_children": "Children Present"})
                    cust = rescore_customer(cur, cust, exact=True)
                    if (cust.get("pg_mosaic_persona") or "").startswith("PG-2"):
                        pg2_count += 1

        return {
            "status": "partywirks_melissa_enriched",
            "records_processed": processed,
            "street_addresses_found": address_matched,
            "upgraded_to_pg2_affluent_families": pg2_count,
            "lookup_failures_retry_later": lookup_failures,
        }
    finally:
        conn.close()


@app.post("/cron/recover-square-avs-zips")
def recover_square_avs_zips(request: Request, limit: int = 150, scan_payment_pages: int = 15,
                            key: Optional[str] = CRON_KEY_PARAM):
    """Finds card billing ZIPs for email-less Square profiles, then enriches via Melissa."""
    require_cron_key(request)
    if not SQUARE_ACCESS_TOKEN:
        return {"status": "error", "message": "Missing SQUARE_ACCESS_TOKEN"}
    limit = max(1, min(limit, 500))
    scan_payment_pages = max(1, min(scan_payment_pages, 50))

    headers = {"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}", "Accept": "application/json"}
    customer_zip_map = {}
    cursor = None
    for _ in range(scan_payment_pages):
        try:
            params = {"limit": 100}
            if cursor:
                params["cursor"] = cursor
            p_resp = requests.get("https://connect.squareup.com/v2/payments", headers=headers,
                                  params=params, timeout=10).json()
            for pay in (p_resp.get("payments") or []):
                cid = pay.get("customer_id")
                if not cid:
                    continue
                p_zip = (
                    (((pay.get("card_details") or {}).get("card") or {}).get("billing_address") or {}).get("postal_code")
                    or (pay.get("billing_address") or {}).get("postal_code")
                    or ""
                ).strip()[:5]
                if len(p_zip) == 5 and p_zip.isdigit():
                    customer_zip_map.setdefault(cid, p_zip)  # payments are newest first: keep the newest
            cursor = p_resp.get("cursor")
            if not cursor:
                break
        except Exception:
            break

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM master_customers
                    WHERE md_address1 IS NULL
                      AND md_results IS NULL
                      AND sq_id IS NOT NULL AND sq_id <> ''
                    ORDER BY CASE WHEN phone IS NOT NULL AND phone != '' THEN 0 ELSE 1 END,
                             true_lifetime_spend DESC
                    LIMIT %s;
                    """,
                    (limit,),
                )
                rows = cur.fetchall()

        checked = zips_found = addresses_unlocked = lookup_failures = 0
        for r in rows:
            checked += 1
            sq_id = r["sq_id"]
            phone = clean_phone(r.get("phone") or "")
            avs_zip = customer_zip_map.get(sq_id, "")

            if not avs_zip or not phone:
                prof = fetch_square_customer_profile(sq_id)
                if prof is None and not (avs_zip or phone):
                    lookup_failures += 1  # Square didn't answer; try this one next run
                    continue
                prof = prof or {}
                avs_zip = avs_zip or prof.get("zip") or ""
                phone = phone or clean_phone(prof.get("phone"))

            if avs_zip:
                zips_found += 1

            with conn:
                with conn.cursor() as cur:
                    if not (phone or avs_zip):
                        cur.execute("UPDATE master_customers SET md_results = 'NO_AVS_ZIP_ON_CARD' "
                                    "WHERE cluster_id = %s;", (r["cluster_id"],))
                        continue
                    enr = run_melissa_and_versium_waterfall(
                        r.get("first_name") or "", r.get("last_name") or "",
                        r.get("email") or "", phone, zip_code=avs_zip, known=r,
                    )
                    if enr.get("md_results") is None and MELISSA_API_KEY and (
                            phone or (r.get("first_name") and r.get("last_name"))):
                        lookup_failures += 1  # Melissa didn't answer; try again next run
                        save_online_audience(cur, r["cluster_id"], enr.get("online_audience"))  # already paid for
                        continue
                    if enr.get("md_address1"):
                        addresses_unlocked += 1
                    if enr.get("md_results") is None:
                        enr["md_results"] = "AVS_CHECKED"
                    cust = save_enrichment(cur, r["cluster_id"], enr, "Square",
                                           extra={"phone": phone or None, "avs_postal_code": avs_zip or None})
                    if phone:
                        cur.execute("INSERT INTO customer_identities (cluster_id, id_type, id_value, source_system) "
                                    "VALUES (%s, 'phone', %s, 'Square') ON CONFLICT DO NOTHING;",
                                    (r["cluster_id"], phone))
                    rescore_customer(cur, cust, exact=True)

        return {
            "status": "square_avs_recovery_complete",
            "profiles_checked": checked,
            "payment_zips_cached": len(customer_zip_map),
            "avs_zips_found_for_batch": zips_found,
            "street_addresses_unlocked_via_melissa": addresses_unlocked,
            "lookup_failures_retry_later": lookup_failures,
        }
    finally:
        conn.close()


@app.post("/cron/census-geocode")
def census_geocode_catch_up(request: Request, limit: int = 200, key: Optional[str] = CRON_KEY_PARAM):
    """Free US Census Geocoder for customers with a street address but no census tract or no coordinates:
    census tract/block, county, school districts, and coordinates/distance when Melissa had none. Then
    rescores them. Each address is asked once (census_checked_at); a Census outage retries next run."""
    require_cron_key(request)
    limit = max(1, min(int(limit), 1000))
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT cluster_id FROM master_customers
                WHERE census_checked_at IS NULL AND (md_census_tract IS NULL OR md_latitude IS NULL)
                  AND COALESCE(TRIM(md_address1), '') <> ''
                  AND (COALESCE(TRIM(zip5), '') <> '' OR (COALESCE(TRIM(md_city), '') <> ''
                                                         AND COALESCE(TRIM(md_state), '') <> ''))
                ORDER BY bowler_value_score DESC NULLS LAST, true_lifetime_spend DESC NULLS LAST, cluster_id
                LIMIT %s;
                """, (limit,))
            ids = [r["cluster_id"] for r in cur.fetchall()]
        matched = coords_added = no_match = failures = 0
        for cid in ids:
            with conn, conn.cursor() as cur:
                cur.execute("SELECT * FROM master_customers WHERE cluster_id = %s AND census_checked_at IS NULL "
                            "FOR UPDATE SKIP LOCKED;", (cid,))
                r = cur.fetchone()
                if not r:
                    continue
                census = census_geocode(r.get("md_address1"), r.get("md_city"), r.get("md_state"), r.get("zip5"))
                if census is None:
                    failures += 1
                    continue
                enr = apply_census({"md_latitude": r.get("md_latitude")}, census)
                enr["census_checked_at"] = datetime.now(timezone.utc).isoformat()
                if census:
                    matched += 1
                    coords_added += int(not r.get("md_latitude") and bool(census.get("md_latitude")))
                else:
                    no_match += 1
                cust = save_enrichment(cur, cid, enr, "Census")
                rescore_customer(cur, cust)
        return {"status": "census_geocode", "matched": matched, "coordinates_added": coords_added,
                "no_match": no_match, "failed_retry_later": failures}
    finally:
        conn.close()


@app.post("/cron/melissa-geocode")
def melissa_geocode_catch_up(request: Request, limit: int = 100, key: Optional[str] = CRON_KEY_PARAM):
    """Runs customers who have a street address + ZIP but no Melissa address work yet (no coordinates or no
    carrier route, e.g. addresses that came from a Versium CSV) through Melissa: verified address,
    coordinates/distance, carrier route, household key. Highest scores first; `limit` paid lookups per call
    (max 500). Each customer is tried once (marked GEO_TRIED); a Melissa rejection leaves them for the next run."""
    require_cron_key(request)
    if not MELISSA_API_KEY:
        return {"status": "error", "message": "Missing MELISSA_API_KEY"}
    limit = max(1, min(int(limit), 500))
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM master_customers
                WHERE (md_latitude IS NULL OR md_carrier_route IS NULL)
                  AND COALESCE(TRIM(md_address1), '') <> '' AND COALESCE(TRIM(zip5), '') <> ''
                  AND COALESCE(md_results, '') NOT LIKE '%%GEO_TRIED%%'
                ORDER BY bowler_value_score DESC NULLS LAST, true_lifetime_spend DESC NULLS LAST, cluster_id
                LIMIT %s;
                """, (limit,))
            rows = cur.fetchall()
        geocoded = no_coordinates = failures = newly_excluded = skipped = consecutive_failures = 0
        stopped_early = False
        for cand in rows:
            # Lock the row for the lookup and recheck it, so overlapping runs never pay twice for one
            # customer (the lock is released on commit/rollback, so a crashed run leaves nothing behind).
            with conn, conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM master_customers
                    WHERE cluster_id = %s AND (md_latitude IS NULL OR md_carrier_route IS NULL)
                      AND COALESCE(md_results, '') NOT LIKE '%%GEO_TRIED%%'
                    FOR UPDATE SKIP LOCKED;
                    """, (cand["cluster_id"],))
                r = cur.fetchone()
                if not r:
                    skipped += 1  # another run has it, or it was geocoded meanwhile
                    continue
                enr = melissa_lookup(r.get("first_name") or "", r.get("last_name") or "", r.get("email") or "",
                                     r.get("phone") or "", r.get("md_address1") or "", r.get("md_city") or "",
                                     r.get("md_state") or "", r.get("zip5") or "")
                if enr.get("md_results") is None:
                    failures += 1  # Melissa didn't answer; try again next run
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        # Melissa is failing (outage, rate limit, credits): stop instead of failing the whole
                        # batch; the reason is in the logs (HTTP status + body).
                        stopped_early = True
                        break
                    continue
                consecutive_failures = 0
                enr["md_results"] = f"{enr['md_results'].rstrip(',')},GEO_TRIED"
                if enr.get("md_latitude"):
                    geocoded += 1
                else:
                    no_coordinates += 1
                was = is_detroit_excluded(r.get("md_city"), r.get("zip5"))
                cust = save_enrichment(cur, r["cluster_id"], enr, "Melissa")
                cust = rescore_customer(cur, cust)
            if not was and is_detroit_excluded(cust.get("md_city"), cust.get("zip5")):
                newly_excluded += 1
        return {"status": "melissa_geocode", "looked_up": geocoded + no_coordinates, "geocoded": geocoded,
                "no_coordinates": no_coordinates, "now_in_excluded_area": newly_excluded,
                "failed_retry_later": failures, "skipped_already_handled": skipped, "stopped_early": stopped_early}
    finally:
        conn.close()


# Customers the catch-up may buy: nothing stored yet, or only a claim older than an hour (a run that died).
OA_UNCLAIMED = ("(COALESCE(versium_online_audience, '{}'::jsonb) = '{}'::jsonb OR "
                "(versium_online_audience - '_pending' = '{}'::jsonb AND "
                "(versium_online_audience->>'_pending')::float < extract(epoch FROM now()) - 3600))")


VERSIUM_UNCHECKED = ("enrich_after IS NULL AND "  # customers waiting for deferred enrichment belong to the worker
                     "(versium_lifestyle_flags IS NULL OR (NOT versium_lifestyle_flags ? '_source' AND "
                     "NOT versium_lifestyle_flags ? '_versium_checked' AND "
                     "(NOT versium_lifestyle_flags ? '_versium_pending' OR "
                     "(versium_lifestyle_flags->>'_versium_pending')::float < extract(epoch FROM now()) - 3600)))")


@app.post("/cron/versium-enrich")
def versium_enrich_catch_up(request: Request, limit: int = 100, key: Optional[str] = CRON_KEY_PARAM):
    """Versium contact + demographics (and the address when there's no street, then the free Census lookup)
    for advertisable customers Versium has never answered for. Highest scores first; about 2 paid lookups
    per customer (3 when the online audience append is also missing); `limit` customers per call (max 500).
    Stops early after 3 rejections in a row (credits, auth) so a bad batch doesn't keep spending."""
    require_cron_key(request)
    if not VERSIUM_API_KEY:
        return {"status": "error", "message": "Missing VERSIUM_API_KEY"}
    limit = max(1, min(int(limit), 500))
    conn = get_conn()
    stats = collections.Counter()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"""
                SELECT * FROM master_customers
                WHERE {VERSIUM_UNCHECKED} AND (COALESCE(email, '') <> '' OR COALESCE(phone, '') <> '')
                ORDER BY bowler_value_score DESC NULLS LAST, true_lifetime_spend DESC NULLS LAST, cluster_id;""")
            rows = [r for r in cur.fetchall() if not is_detroit_excluded(r["md_city"], r["zip5"])][:limit]
        consecutive_failures = 0
        for r in rows:
            with conn, conn.cursor() as cur:  # claim first so overlapping runs never pay twice
                cur.execute(f"""UPDATE master_customers SET versium_lifestyle_flags =
                                    COALESCE(versium_lifestyle_flags, '{{}}'::jsonb)
                                    || jsonb_build_object('_versium_pending', extract(epoch FROM now()))
                                WHERE cluster_id = %s AND {VERSIUM_UNCHECKED} RETURNING cluster_id;""",
                            (r["cluster_id"],))
                if not cur.fetchone():
                    continue
            enr = {"alt_emails": [], "alt_phones": [], "lifestyle_flags": {}, "online_audience": {},
                   "md_city": r["md_city"], "md_state": r["md_state"], "zip5": r["zip5"]}
            run_versium_enrichment(enr, r["first_name"] or "", r["last_name"] or "", r["email"] or "",
                                   r["phone"] or "", r["md_address1"] or "", r["md_city"] or "",
                                   r["md_state"] or "", r["zip5"] or "", r, time.monotonic(),
                                   online_budget=float("inf"))
            stats["checked"] += 1
            if enr.get("versium_address"):
                stats["address_found"] += 1
                census = census_geocode(enr["md_address1"], enr.get("md_city") or "", enr.get("md_state") or "",
                                        enr.get("zip5") or "")
                apply_census(enr, census or {})
                if census is not None:
                    enr["census_checked_at"] = datetime.now(timezone.utc).isoformat()
                    stats["census_matched" if census else "census_no_match"] += 1
            with conn, conn.cursor() as cur:
                cur.execute("UPDATE master_customers SET versium_lifestyle_flags = versium_lifestyle_flags "
                            "- '_versium_pending' WHERE cluster_id = %s;", (r["cluster_id"],))
                if enr.get("_versium_contact_failed") and enr.get("_versium_demo_failed") \
                        and not enr.get("online_audience"):
                    stats["failed_retry_later"] += 1
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        break
                    continue
                consecutive_failures = 0
                if enr.get("_versium_failed"):
                    # Keep what was paid for; no checked marker, so the failed part is retried.
                    stats["partly_failed_retry_later"] += 1
                elif set(enr.get("lifestyle_flags") or {}) - {"_versium_checked", "_versium_contact_done", "_versium_demo_done"}:
                    stats["demographics_found"] += 1
                else:
                    stats["no_demographic_match"] += 1
                stats["alt_contacts_found"] += int(bool(enr["alt_emails"] or enr["alt_phones"]))
                if not MELISSA_LIVE and r.get("md_results") is None and not enr.get("_versium_failed"):
                    enr["md_results"] = MELISSA_OFF  # finished: webhooks won't look this customer up again
                cust = save_enrichment(cur, r["cluster_id"], enr, "versium")
                cust = rescore_customer(cur, cust, exact=True)
                # Events dropped because enrichment never finished are judged again now that it has.
                stats.update({f"reevaluated_{k}": v for k, v in reevaluate_exhausted(cur, cust).items()})
        out = {"status": "versium_enrich", "eligible_in_batch": len(rows), **stats,
               "stopped_early": consecutive_failures >= 3}
        log.info("Versium enrich catch-up: %s", out)
        return out
    finally:
        conn.close()


@app.post("/cron/versium-online-audience")
def versium_online_audience_catch_up(request: Request, limit: int = 100,
                                     key: Optional[str] = CRON_KEY_PARAM):
    """Online audience append for eligible customers that don't have it yet (never checked, or the live
    call was skipped or failed). Highest scores first; `limit` paid lookups per call (max 500).
    Customers Versium has no match for are marked checked so they aren't bought again."""
    require_cron_key(request)
    if not VERSIUM_API_KEY:
        return {"status": "error", "message": "Missing VERSIUM_API_KEY"}
    limit = max(1, min(int(limit), 500))
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT cluster_id, first_name, last_name, email, phone, md_address1, md_city, md_state, zip5
                FROM master_customers
                WHERE {OA_UNCLAIMED}
                  AND (COALESCE(email, '') <> '' OR COALESCE(phone, '') <> '')
                ORDER BY bowler_value_score DESC NULLS LAST, true_lifetime_spend DESC NULLS LAST, cluster_id;
                """
            )
            rows = [r for r in cur.fetchall()
                    if versium_online_append_eligible(r["md_city"], r["md_state"], r["zip5"])][:limit]
        headers = {"x-versium-api-key": VERSIUM_API_KEY, "Accept": "application/json"}
        matched = no_match = failures = consecutive_failures = 0
        for r in rows:
            # Claim the customer first, so overlapping runs never buy the same lookup twice.
            with conn, conn.cursor() as cur:
                cur.execute(f"UPDATE master_customers SET versium_online_audience = "
                            f"jsonb_build_object('_pending', extract(epoch FROM now())) "
                            f"WHERE cluster_id = %s AND {OA_UNCLAIMED} RETURNING cluster_id;", (r["cluster_id"],))
                if not cur.fetchone():
                    continue  # another run or a live webhook got there first
            v_input = {k: v for k, v in {
                "first": r["first_name"], "last": r["last_name"], "email": r["email"], "phone": r["phone"],
                "address": r["md_address1"], "city": r["md_city"], "state": r["md_state"], "zip": r["zip5"],
                "country": "US"}.items() if v}
            try:
                oa = fetch_versium_online_audience(headers, v_input)
            except Exception as e:
                log.warning("Versium online audience lookup failed: %s", type(e).__name__)
                oa = None
            if oa is None:
                with conn, conn.cursor() as cur:  # release the claim so a later run retries
                    cur.execute("UPDATE master_customers SET versium_online_audience = "
                                "versium_online_audience - '_pending' WHERE cluster_id = %s;", (r["cluster_id"],))
                failures += 1
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    break  # Versium is rejecting everything (credits, auth); stop spending the batch
                continue
            consecutive_failures = 0
            if set(oa) - {"_checked"}:
                matched += 1
            else:
                no_match += 1
            with conn, conn.cursor() as cur:
                save_online_audience(cur, r["cluster_id"], oa)
        return {"status": "versium_online_audience", "eligible_in_batch": len(rows), "matched": matched,
                "no_match": no_match, "failed_retry_later": failures,
                "stopped_early": consecutive_failures >= 3}
    finally:
        conn.close()


@app.post("/cron/adjust-thermostat")
def adjust_thermostat(request: Request, key: Optional[str] = CRON_KEY_PARAM):
    require_cron_key(request)
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # First re-send anything Meta didn't accept, so the thermostat counts it.
                retried = retry_unsent_events(cur)
                # The function is versioned in migrations/012_thermostat.sql (an exact copy of production's).
                cur.execute("SELECT adjust_meta_capi_thermostat() AS new_threshold;")
                res = cur.fetchone()
        return {"status": "adjusted", "new_threshold": res["new_threshold"], "meta_retry": retried}
    finally:
        conn.close()


@app.post("/cron/sync-partywirks")
def sync_partywirks(request: Request, background_tasks: BackgroundTasks, days_back: int = 3,
                    key: Optional[str] = CRON_KEY_PARAM):
    """Nightly catch-up for Partywirks reservations a webhook missed. Runs in the background
    (it paces itself under the API rate limit) and logs its totals."""
    require_cron_key(request)
    if not PARTYWIRKS_API_KEY:
        return {"status": "error", "message": "Missing PARTYWIRKS_API_KEY"}
    background_tasks.add_task(run_partywirks_sync, max(0, min(days_back, 60)))
    return {"status": "partywirks_sync_queued", "days_back": days_back}


@app.post("/cron/sync-square-catalog")
def sync_square_catalog(request: Request, key: Optional[str] = CRON_KEY_PARAM):
    """Manual or scheduled trigger to sync Square Catalog into Neon."""
    require_cron_key(request)
    return run_square_catalog_sync()


# ---------------------------------------------------------------------------
# Manual Melissa.com uploads (migrations/009)
# ---------------------------------------------------------------------------

# Waiting for a Melissa.com upload: a name and an email or phone, Versium answered but found no demographics,
# no Melissa demographics, not still waiting for deferred enrichment, never uploaded (or marked 'prior').
MELISSA_UPLOAD_ELIGIBLE = """
    melissa_upload_status IS NULL AND enrich_after IS NULL
    AND COALESCE(TRIM(first_name), '') <> '' AND COALESCE(TRIM(last_name), '') <> ''
    AND (COALESCE(TRIM(email), '') <> '' OR COALESCE(TRIM(phone), '') <> '')
    AND COALESCE(versium_lifestyle_flags ? '_versium_checked' OR versium_lifestyle_flags ? '_source', FALSE)
    AND NOT EXISTS (SELECT 1 FROM jsonb_object_keys(COALESCE(versium_lifestyle_flags, '{}'::jsonb)) AS k
                    WHERE left(k, 1) <> '_')  -- any Versium field counts as a match (see versium_matched)
    AND COALESCE(TRIM(md_household_income), '') = '' AND COALESCE(TRIM(md_date_of_birth), '') = ''
    AND COALESCE(TRIM(md_demographic_results), '') = ''
"""
MELISSA_BATCH_SIZE = max(1, int(os.getenv("MELISSA_BATCH_SIZE", "100")))
MELISSA_CSV_COLUMNS = ["RecordID", "First Name", "Last Name", "Address", "City", "State", "Zip", "Email", "Phone"]


def melissa_batch_csv(cur, batch_id: int) -> str:
    """The batch as a CSV for Melissa.com. RecordID is our customer number, so results can be matched back."""
    cur.execute("""SELECT cluster_id, first_name, last_name, md_address1, md_city, md_state, zip5, email, phone
                   FROM master_customers WHERE melissa_batch_id = %s ORDER BY cluster_id;""", (batch_id,))
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\r\n")
    w.writerow(MELISSA_CSV_COLUMNS)
    for r in cur.fetchall():
        digits = re.sub(r"\D", "", str(r.get("phone") or ""))
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        w.writerow([r["cluster_id"]] + [str(r.get(k) or "").strip() for k in
                    ("first_name", "last_name", "md_address1", "md_city", "md_state", "zip5", "email")] + [digits])
    return out.getvalue()


def _melissa_batch_payload(cur, batch: dict) -> dict:
    created = batch["created_at"]
    return {"ready": True, "batch_id": batch["batch_id"], "customers": batch["customer_count"],
            "filename": f"melissa-batch-{batch['batch_id']:03d}-{created:%Y-%m-%d}.csv",
            "csv": melissa_batch_csv(cur, batch["batch_id"])}


@app.post("/cron/retry-webhooks")
def retry_webhooks(request: Request, key: Optional[str] = CRON_KEY_PARAM,
                   limit: int = Query(50, ge=1, le=500)):
    """Processes failed webhooks again (webhook_dead_letters, migrations/010). Run hourly."""
    require_cron_key(request)
    return retry_dead_letters(limit)


@app.post("/cron/melissa-batch")
def melissa_batch(request: Request, key: Optional[str] = CRON_KEY_PARAM):
    """For the Gmail Apps Script: once MELISSA_BATCH_SIZE customers are waiting for a Melissa.com upload, makes
    the next batch and returns it as a CSV to email. A batch not yet confirmed as emailed
    (/cron/melissa-batch/{id}/emailed) is returned again, so a failed email never loses one."""
    require_cron_key(request)
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext('melissa_batch'));")  # one batch at a time
            cur.execute("SELECT * FROM melissa_batches WHERE emailed_at IS NULL ORDER BY batch_id LIMIT 1;")
            pending = cur.fetchone()
            if pending:
                return _melissa_batch_payload(cur, pending)
            cur.execute(f"SELECT count(*) AS n FROM master_customers WHERE {MELISSA_UPLOAD_ELIGIBLE};")
            waiting = cur.fetchone()["n"]
            if waiting < MELISSA_BATCH_SIZE:
                return {"ready": False, "waiting": waiting, "batch_size": MELISSA_BATCH_SIZE}
            cur.execute("INSERT INTO melissa_batches (customer_count) VALUES (0) RETURNING *;")
            batch = cur.fetchone()
            cur.execute(f"""UPDATE master_customers SET melissa_upload_status = 'sent', melissa_batch_id = %s
                            WHERE cluster_id IN (SELECT cluster_id FROM master_customers
                                                 WHERE {MELISSA_UPLOAD_ELIGIBLE}
                                                 ORDER BY cluster_id LIMIT %s FOR UPDATE SKIP LOCKED);""",
                        (batch["batch_id"], MELISSA_BATCH_SIZE))
            batch["customer_count"] = cur.rowcount
            cur.execute("UPDATE melissa_batches SET customer_count = %s WHERE batch_id = %s;",
                        (batch["customer_count"], batch["batch_id"]))
            log.info("Melissa upload batch %s made with %s customers", batch["batch_id"], batch["customer_count"])
            return _melissa_batch_payload(cur, batch)
    finally:
        conn.close()


@app.post("/cron/melissa-batch/{batch_id}/emailed")
def melissa_batch_emailed(batch_id: int, request: Request, key: Optional[str] = CRON_KEY_PARAM):
    """The Gmail Apps Script confirms it emailed the batch, so the next call can make a new one."""
    require_cron_key(request)
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("UPDATE melissa_batches SET emailed_at = COALESCE(emailed_at, NOW()) WHERE batch_id = %s "
                        "RETURNING batch_id, emailed_at;", (batch_id,))
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No such batch")
        return {"status": "emailed", "batch_id": row["batch_id"], "emailed_at": row["emailed_at"].isoformat()}
    finally:
        conn.close()


@app.get("/health")
def health():
    """Liveness check. Also shows which authentication checks are switched on (no data exposed)."""
    return {
        "status": "ok",
        "square_signature_check": bool(SQUARE_WEBHOOK_SIGNATURE_KEY),
        "webhook_key_check": bool(WEBHOOK_SECRET),
        "webhook_key_rotation_window": bool(WEBHOOK_SECRET_PREVIOUS),
        "tripleseat_signature_check": ("enforced" if TRIPLESEAT_SIGNATURE_ENFORCE and TRIPLESEAT_SIGNATURE_SCHEME
                                       else "logging only") if TRIPLESEAT_WEBHOOK_SECRET else False,
        "cron_key_check": bool(CRON_SECRET),
        "partywirks_signature_check": ("enforced" if PARTYWIRKS_SIGNATURE_ENFORCE else "logging only")
                                      if PARTYWIRKS_WEBHOOK_SECRET else False,
        # Enrichment can't finish without it (VERSIUM_REQUIRED): held events wait and customers are retried.
        "versium_key_set": bool(VERSIUM_API_KEY),
    }


def sync_cluster_sequence():
    """Keeps the customer-number sequence ahead of every existing customer number."""
    if not DATABASE_URL:
        return
    try:
        conn = get_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT setval('master_cluster_id_seq', GREATEST(
                            (SELECT COALESCE(MAX(cluster_id), 24220) FROM master_customers),
                            (SELECT last_value FROM master_cluster_id_seq)));
                        """
                    )
        finally:
            conn.close()
    except Exception as e:
        log.error("Could not sync master_cluster_id_seq (has the migration been run?): %s", e)
