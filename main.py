import os
import re
import hmac
import math
import time
import json
import base64
import hashlib
import logging
import collections
from datetime import date, datetime, timezone
from typing import Optional

import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from contextlib import asynccontextmanager

log = logging.getLogger("pg_cdp")
logging.basicConfig(level=logging.INFO)



@asynccontextmanager
async def lifespan(_app):
    await run_in_threadpool(sync_cluster_sequence)
    yield


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
# WebRes / Tripleseat / Partywirks: send as header `X-CDP-Key` or query param `?key=`.
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
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
# Real Partywirks order IDs are 13-14 digits; their sample/test payload uses "PW-YYYYMM-NNNN".
REAL_PARTYWIRKS_ORDER_ID = re.compile(r"^\d{10,20}$")

# Your 4 Live Non-Detroit Meta Custom Audience IDs
AUDIENCE_PG1_CORP = os.getenv("AUDIENCE_PG1_CORP", "120246555016470358")
AUDIENCE_PG2_FAMILIES = os.getenv("AUDIENCE_PG2_FAMILIES", "120246555006040358")
AUDIENCE_PRIME_SUBURBS = os.getenv("AUDIENCE_PRIME_SUBURBS", "120246555019320358")
AUDIENCE_ALL_NONDETROIT = os.getenv("AUDIENCE_ALL_NONDETROIT", "120246555027130358")

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


def fetch_versium_online_audience(v_headers: dict, v_input: dict, timeout: float = 4):
    """Returns the parsed IDs plus `_checked` (today), `{"_checked": today}` when Versium has no match,
    or None when the call failed or was rejected (bad parameter, no credits, auth), which is logged
    with Versium's reason and left for /cron/versium-online-audience to retry."""
    resp = requests.get(
        "https://api.versium.com/v2/onlineAudience",
        headers=v_headers,
        params={**v_input, "rcfg_oa_services[]": list(VERSIUM_ONLINE_AUDIENCE_SERVICES)},
        timeout=timeout,
    )
    try:
        body = resp.json()
    except ValueError:
        body = None
    if resp.status_code == 200 and not isinstance(body, dict):
        log.error("Versium online audience returned an unreadable answer (HTTP 200)")
        return None  # stays retryable; never recorded as a no-match
    if resp.status_code != 200:
        body = body if isinstance(body, dict) else {}
        err = body.get("errors") or body.get("error") or body.get("message") if isinstance(body, dict) else None
        log.error("Versium online audience rejected (HTTP %s): %s", resp.status_code, str(err)[:300])
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


def _require_key(request: Request, expected: Optional[str], name: str):
    if not expected:
        log.warning("%s is not set; %s is accepting unauthenticated requests", name, request.url.path)
        return
    if not hmac.compare_digest(_provided_key(request).encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Unauthorized")


def require_webhook_key(request: Request):
    _require_key(request, WEBHOOK_SECRET, "WEBHOOK_SECRET")


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
# Recorded in md_results when there was no street address to check (Check needs one); a later street
# address triggers a lookup again.
MELISSA_NO_STREET = "NO_STREET_TO_CHECK"
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


def melissa_lookup(first_name: str, last_name: str, email: str, phone: str,
                   address: str = "", city: str = "", state: str = "", zip_code: str = "") -> dict:
    """Melissa Personator: verified address, geocode + distance, census/school fields, household demographics.
    Returns {} when Melissa isn't configured or didn't answer (so `md_results` stays None)."""
    if not MELISSA_API_KEY:
        return {}
    zip_code = (zip_code or "")[:5]
    if "Append" not in MELISSA_ACT and not (address or "").strip():
        return {"md_results": MELISSA_NO_STREET}  # nothing Melissa can check; don't spend a credit
    try:
        m_resp = requests.get(
            "https://personator.melissadata.net/v3/WEB/ContactVerify/doContactVerify",
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
            timeout=5,
        )
        m_resp.raise_for_status()
        body = m_resp.json()
        rec = (body.get("Records") or [{}])[0]
    except Exception as e:
        log.warning("Melissa lookup failed: %s", type(e).__name__)
        return {}
    # Melissa pads missing values with spaces; never let a blank overwrite real data.
    rec = {k: (v.strip() if isinstance(v, str) else v) for k, v in (rec or {}).items()}
    rec = {k: v for k, v in rec.items() if v not in ("", None)}
    transmission = str((body or {}).get("TransmissionResults") or "").strip()
    if transmission:
        # Melissa rejected the request itself (bad parameter, license, credits): not an answer; retry later.
        log.error("Melissa rejected the request (TransmissionResults=%s)", transmission[:100])
        return {}
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


def run_melissa_and_versium_waterfall(first_name: str, last_name: str, email: str, phone: str,
                                      address: str = "", city: str = "", state: str = "", zip_code: str = "",
                                      known: dict = None) -> dict:
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

    # --- STEP 1: MELISSA PERSONATOR ---
    result.update(melissa_lookup(first_name, last_name, email, phone, address, city, state, zip_code))

    # --- STEP 1b: US CENSUS GEOCODER (free): census/school fields, coordinates when Melissa had none ---
    street = result.get("md_address1") or address
    if street and not result.get("md_census_tract") and time.monotonic() - started < CENSUS_BUDGET_SECONDS:
        census = census_geocode(street, result.get("md_city") or city,
                                result.get("md_state") or state, result.get("zip5") or zip_code)
        apply_census(result, census or {})
        if census is not None:
            result["census_checked_at"] = datetime.now(timezone.utc).isoformat()

    # --- STEP 2: VERSIUM REACH (When API Key is active) ---
    known = known or {}
    gate_zip = result.get("zip5") or known.get("zip5")
    gate_city = result.get("md_city") or known.get("md_city")
    gate_state = result.get("md_state") or known.get("md_state")
    if VERSIUM_API_KEY and not is_detroit_excluded(gate_city, gate_zip):
        v_headers = {"x-versium-api-key": VERSIUM_API_KEY, "Accept": "application/json"}
        v_input = {
            "first": first_name,
            "last": last_name,
            "email": email,
            "phone": phone,
            "address": result.get("md_address1") or address,
            "city": result.get("md_city") or city,
            "state": result.get("md_state") or state,
            "zip": result.get("zip5") or zip_code,
            "country": "US",
        }
        try:
            vc = requests.get(
                "https://api.versium.com/v2/contact",
                headers=v_headers,
                params={**v_input, "output[]": ["email_multiple", "phone_multiple"], "rcfg_max_emails": 5},
                timeout=5,
            ).json()
            v_res = (vc.get("versium", {}).get("results") or [{}])[0]
            result["alt_emails"] = [e.strip().lower() for e in [v_res.get(f"email_{i}") for i in range(1, 6)] if e]
            result["alt_phones"] = [clean_phone(p) for p in [v_res.get(f"phone_{i}") for i in range(1, 6)] if clean_phone(p)]

            vd = requests.get(
                "https://api.versium.com/v2/demographic",
                headers=v_headers,
                params={**v_input, "output[]": ["basic_demographic", "financial_household_auto", "lifestyle_interests"]},
                timeout=5,
            ).json()
            vd_res = (vd.get("versium", {}).get("results") or [{}])[0]
            result["versium_net_worth"] = vd_res.get("net_worth") or None
            result["versium_home_market_value"] = vd_res.get("home_market_value") or None
            result["versium_auto_make_model"] = f"{vd_res.get('auto_make', '')} {vd_res.get('auto_model', '')}".strip() or None
            result["lifestyle_flags"] = strip_blocked_versium_fields(vd_res)
        except Exception as e:
            log.warning("Versium lookup failed: %s", type(e).__name__)

        # Online audience append (Meta / Google / LinkedIn / TikTok IDs): local, advertisable customers only.
        # Enrichment runs inside the webhook request, so it is skipped when the earlier lookups were
        # already slow. Skipped or failed calls leave the column empty for /cron/versium-online-audience.
        # Already bought (or checked with no match) for this customer: never pay again.
        if not known.get("versium_online_audience") and \
                versium_online_append_eligible(gate_city, gate_state, gate_zip) and \
                time.monotonic() - started < VERSIUM_ONLINE_BUDGET_SECONDS:
            try:
                result["online_audience"] = fetch_versium_online_audience(v_headers, v_input) or {}
            except Exception as e:
                log.warning("Versium online audience lookup failed: %s", type(e).__name__)

    return result


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def score_and_assign_persona(cust: dict) -> tuple:
    kids_raw = str(cust.get("md_presence_of_children") or "").strip().upper()
    has_kids = (
        kids_raw.startswith("Y")
        or (kids_raw.startswith("CHILDREN") and "NO" not in kids_raw)
        or int(cust.get("in_partywirks") or 0) == 1
    )

    income_str = str(cust.get("md_household_income") or "")
    high_income = any(k in income_str for k in ["100,", "125,", "150,", "200,", "250,"])
    mid_income = any(k in income_str for k in ["60,", "75,", "80,", "90,"])

    dist = cust.get("distance_miles")
    total_spend = float(cust.get("true_lifetime_spend") or 0)
    guest_count = int(cust.get("ts_max_guests") or 0)
    excluded_geo = is_detroit_excluded(cust.get("md_city"), cust.get("zip5"))

    is_corp = (
        bool(cust.get("ts_account"))
        or bool(cust.get("versium_b2b_company"))
        or cust.get("md_delivery_indicator") == "B"
        or guest_count >= 25
    )

    if is_corp:
        persona = "PG-1: Corporate & Org Event Planners"
    elif excluded_geo:
        persona = "PG-5: Excluded Geo (Detroit / Southfield / Downriver)"
    elif has_kids and high_income:
        persona = "PG-2: Affluent Suburban Party Families"
    elif has_kids:
        persona = "PG-3: Core Suburban Family Bowlers"
    elif cust.get("md_address1"):
        persona = "PG-4: Adult Social, Date Night & League"
    else:
        persona = "PG-6: Digital-Only (Need Versium Append)"

    if excluded_geo and not is_corp:
        return persona, 25

    score = 30
    if persona == "PG-1: Corporate & Org Event Planners":
        score += 35
    elif persona == "PG-2: Affluent Suburban Party Families":
        score += 25
    elif has_kids:
        score += 15

    if high_income:
        score += 15
    elif mid_income:
        score += 10

    if dist is not None and float(dist) <= 12.0 and not excluded_geo:
        score += 15
    if total_spend >= 400:
        score += 15

    return persona, min(100, score)


def rescore_customer(cur, cust: dict, exact: bool = False) -> dict:
    """Recalculates persona & score. Scores only ratchet upward unless `exact` (used by repairs)
    or the customer is in an excluded area (always pinned to 25)."""
    persona, score = score_and_assign_persona(cust)
    cur.execute(
        """
        UPDATE master_customers
        SET pg_mosaic_persona = %(persona)s,
            bowler_value_score = CASE
                WHEN %(exact)s OR %(score)s = 25 THEN %(score)s
                ELSE GREATEST(COALESCE(bowler_value_score, 0), %(score)s)
            END
        WHERE cluster_id = %(cid)s RETURNING *;
        """,
        {"persona": persona, "score": score, "exact": exact, "cid": cust["cluster_id"]},
    )
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
    return {AUDIENCE_ALL_NONDETROIT: "All Non-Detroit", AUDIENCE_PG1_CORP: "PG-1 Corporate",
            AUDIENCE_PG2_FAMILIES: "PG-2 Families", AUDIENCE_PRIME_SUBURBS: "Prime Suburbs"}


def audience_targets(cust: dict) -> tuple:
    """Which of the 4 audiences a customer belongs in, plus the LTV sent as LOOKALIKE_VALUE.
    Excluded-area customers belong in none."""
    if is_detroit_excluded(cust.get("md_city"), cust.get("zip5")):
        return [], 0.0
    persona = cust.get("pg_mosaic_persona") or ""
    ltv = max(
        float(cust.get("true_lifetime_spend") or 0),
        450.0 if (cust.get("in_tripleseat") or cust.get("in_partywirks")) else 0.0,
    )
    targets = [AUDIENCE_ALL_NONDETROIT]
    if persona.startswith("PG-1") or cust.get("in_tripleseat") == 1 or ltv >= 600:
        targets.append(AUDIENCE_PG1_CORP)
    if persona.startswith("PG-2") or cust.get("in_partywirks") == 1:
        targets.append(AUDIENCE_PG2_FAMILIES)
    if (cust.get("zip5") or "") in PRIME_SUBURB_ZIPS:
        targets.append(AUDIENCE_PRIME_SUBURBS)
    return [a for a in dict.fromkeys(targets) if a], ltv


def audience_row(cust: dict, ltv: float) -> list:
    return [
        sha256_hash(cust.get("email")),
        sha256_hash(meta_phone(cust.get("phone"))),
        sha256_hash(cust.get("first_name")),
        sha256_hash(cust.get("last_name")),
        sha256_hash(meta_city(cust.get("md_city"))),
        sha256_hash(meta_state(cust.get("md_state"))),
        sha256_hash(cust.get("zip5")),
        sha256_hash("us"),
        round(ltv, 2),
    ]


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


def sync_customer_to_meta_custom_audiences(cust: dict):
    """Pushes one qualified non-excluded customer into the Meta value-based Custom Audiences they belong in."""
    if not META_ACCESS_TOKEN:
        return
    targets, ltv = audience_targets(cust)
    row = audience_row(cust, ltv)
    for aud_id in targets:
        upload_audience_rows(aud_id, [row])


def fire_meta_capi_event(cur, cust: dict, event_name: str, value: float, threshold: int,
                         event_id: str, action_source: str = "system_generated"):
    """Sends one CAPI event, at most once per event_id.

    A Meta error or timeout is logged with sent_to_meta = FALSE and never raises, so it can't roll back
    the customer update. If the same event comes through again later, it's retried.
    """
    if not META_PIXEL_ID or not META_ACCESS_TOKEN:
        return
    cur.execute("SELECT sent_to_meta FROM capi_events_log WHERE event_id = %s;", (event_id,))
    prior = cur.fetchone()
    if prior and prior["sent_to_meta"]:
        return

    cluster_id = cust["cluster_id"]
    score = int(cust.get("bowler_value_score") or 0)
    payload = {
        "data": [{
            "event_name": event_name,
            "event_time": int(time.time()),
            "event_id": event_id,
            "action_source": action_source,
            "user_data": build_meta_user_identifiers(cust),
            "custom_data": {
                "currency": "USD",
                "value": round(float(value), 2),
                "pg_persona": cust.get("pg_mosaic_persona"),
                "bowler_value_score": score,
            },
        }]
    }
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
            threshold_at_send, sent_to_meta, meta_response_status, meta_response_body
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO UPDATE SET
            sent_at = NOW(),
            sent_to_meta = EXCLUDED.sent_to_meta,
            meta_response_status = EXCLUDED.meta_response_status,
            meta_response_body = EXCLUDED.meta_response_body
        WHERE capi_events_log.sent_to_meta = FALSE;
        """,
        (event_id, cluster_id, event_name, value, score, threshold, ok, status, body),
    )


def _action_source_for(event_id: str) -> str:
    return "physical_store" if event_id.startswith("sq_pay_") else "system_generated"


def retry_unsent_event(cur, event_id: str) -> bool:
    """Re-sends a logged event Meta didn't accept. Safe to call for any ID: does nothing otherwise."""
    cur.execute("SELECT * FROM capi_events_log WHERE event_id = %s AND NOT sent_to_meta FOR UPDATE SKIP LOCKED;",
                (event_id,))
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


def retry_unsent_events(cur, max_age_days: int = 6) -> dict:
    """Re-sends every event Meta hasn't accepted yet. Meta only takes events up to 7 days old."""
    cur.execute("SELECT event_id FROM capi_events_log WHERE NOT sent_to_meta "
                "AND sent_at > NOW() - make_interval(days => %s) ORDER BY sent_at;", (max_age_days,))
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
                                           GREATEST(0, COALESCE(wr_spend, 0) + %(wr)s)),
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
    "true_lifetime_spend": f"GREATEST({_SQ_NEW}, {_WR_NEW})",
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
    "versium_lifestyle_flags": _jsonb("versium_lifestyle_flags", "{}"),
    "versium_online_audience": f"COALESCE({_MC}.versium_online_audience, '{{}}'::jsonb) || EXCLUDED.versium_online_audience",
    "ts_lead_status": _keep("ts_lead_status"),
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

    # 2. Progressive enrichment
    needs_enrichment = _needs_enrichment(existing, email, phone, data)
    enriched = {}
    if needs_enrichment:
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
    elif data.get("zip") and not (existing or {}).get("zip5"):
        # No paid lookup needed, but keep the ZIP we were handed so the geo gate can use it.
        enriched = {"zip5": str(data["zip"])[:5], "md_city": data.get("city") or None,
                    "md_state": data.get("state") or None}

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
        sets.append("versium_lifestyle_flags = %(flags)s::jsonb")
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

    thresh = get_threshold(cur)
    if net > 0 and not is_detroit_excluded(cust.get("md_city"), cust.get("zip5")):
        fire_meta_capi_event(cur, cust, "Purchase", net, thresh, event_id=txn_id, action_source="physical_store")
        if cust["bowler_value_score"] >= thresh and newly_enriched:
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
    if not is_detroit_excluded(cust.get("md_city"), cust.get("zip5")) and cust["bowler_value_score"] >= thresh:
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
                    "wr_spend_delta": amount,
                    "wr_booking_delta": 1,
                    "wr_last_reservation_date": res_date,
                    "in_webres": 1,
                    "source": "WebRes",
                })
                cur.execute("UPDATE unified_transactions SET cluster_id = %s WHERE transaction_id = %s;",
                            (cust["cluster_id"], txn_id))
                thresh = get_threshold(cur)

                if not is_detroit_excluded(cust.get("md_city"), cust.get("zip5")):
                    if amount > 0:
                        fire_meta_capi_event(cur, cust, "Purchase", amount, thresh, event_id=txn_id)
                    if cust["bowler_value_score"] >= thresh:
                        fire_meta_capi_event(cur, cust, "Qualified_Bowler", max(amount, 250.0), thresh,
                                             event_id=f"qb_wr_{booking_id}")
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
    return cur.fetchone(), True


def handle_tripleseat(raw: dict, background_tasks: BackgroundTasks) -> dict:
    kind, obj = "record", raw
    for key in ("lead", "event", "booking", "contact"):
        if isinstance(raw.get(key), dict):
            kind, obj = key, raw[key]
            break
    contact = obj.get("contact") if isinstance(obj.get("contact"), dict) else obj
    account = obj.get("account") or contact.get("account") or {}
    if not isinstance(account, dict):
        account = {}

    email = contact.get("email_address") or obj.get("email_address") or obj.get("email") or ""
    if not email and isinstance(contact.get("email_addresses"), list) and contact["email_addresses"]:
        email = contact["email_addresses"][0].get("address", "")

    phone = contact.get("phone_number") or obj.get("phone_number") or obj.get("phone") or ""
    if not phone and isinstance(contact.get("phone_numbers"), list) and contact["phone_numbers"]:
        phone = contact["phone_numbers"][0].get("number", "")

    addr_obj = {}
    if isinstance(contact.get("addresses"), list) and contact["addresses"]:
        addr_obj = contact["addresses"][0]
    elif isinstance(obj.get("addresses"), list) and obj["addresses"]:
        addr_obj = obj["addresses"][0]

    first_name = contact.get("first_name") or obj.get("first_name") or ""
    last_name = contact.get("last_name") or obj.get("last_name") or ""
    company = account.get("name") or obj.get("company") or obj.get("account_name") or contact.get("company") or ""
    status = str(obj.get("status") or obj.get("lead_status") or "PROSPECT").upper().replace(" ", "_")
    guest_count = to_int(obj.get("guest_count") or obj.get("guaranteed_guest_count") or obj.get("expected_guest_count"))
    event_date = date_only(obj.get("event_date") or obj.get("event_start_iso8601") or obj.get("desired_date"))
    turned_down_reason = obj.get("turned_down_reason") or obj.get("lost_reason") or None
    record_id = obj.get("id") or obj.get("lead_id") or obj.get("event_id")

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
                    "address": addr_obj.get("address1") or obj.get("address"),
                    "city": addr_obj.get("city") or obj.get("city"),
                    "state": addr_obj.get("state") or obj.get("state"),
                    "zip": addr_obj.get("zip_code") or obj.get("zip"),
                    "ts_account": company or None,
                    "ts_lead_status": status,
                    "ts_max_guests": guest_count,
                    "ts_last_event_date": event_date,
                    "ts_turned_down_reason": turned_down_reason,
                    "in_tripleseat": 1,
                    "source": "Tripleseat",
                })
                if record_id and kind in ("lead", "event", "booking"):
                    cust, _ = _count_record_once(cur, cust, f"ts_{kind}_{record_id}", "tripleseat", "ts_events")

                thresh = get_threshold(cur)
                if (
                    not is_detroit_excluded(cust.get("md_city"), cust.get("zip5"))
                    and cust["bowler_value_score"] >= thresh
                    and status not in ("LOST", "TURNED_DOWN")
                ):
                    # One Qualified_Bowler per customer per event date, however many times Tripleseat
                    # re-sends the lead/event (a lead and the event it turns into share the date).
                    key = event_date or (f"{kind}_{record_id}" if record_id else "undated")
                    synthetic_val = 1000.0 if (cust.get("pg_mosaic_persona") or "").startswith("PG-1") else 450.0
                    fire_meta_capi_event(cur, cust, "Qualified_Bowler", synthetic_val, thresh,
                                         event_id=f"qb_ts_{cust['cluster_id']}_{key}")
                    background_tasks.add_task(sync_customer_to_meta_custom_audiences, dict(cust))

        return {
            "status": "tripleseat_synced",
            "cluster_id": cust["cluster_id"],
            "lead_status": status,
            "score": cust["bowler_value_score"],
        }
    finally:
        conn.close()


@app.post("/webhooks/tripleseat")
async def tripleseat_webhook(request: Request, background_tasks: BackgroundTasks):
    """Tripleseat Lead, Event, Booking, and Contact webhooks."""
    require_webhook_key(request)
    _, raw = await read_json(request)
    return await run_in_threadpool(handle_tripleseat, raw, background_tasks)


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
    }


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
        return {"status": "partywirks_cancelled_reservation", "cluster_id": cust["cluster_id"]}

    key = rec.get("order_id") or f"c{cust['cluster_id']}_{rec.get('party_date') or 'undated'}"
    cust, is_new = _count_record_once(cur, cust, f"pw_{key}", "partywirks", "pw_bookings", bump=count_booking)

    thresh = get_threshold(cur)
    if not is_new:
        retry_unsent_event(cur, f"qb_pw_{key}")  # a Meta failure last time gets another try
    elif (count_booking and not is_detroit_excluded(cust.get("md_city"), cust.get("zip5"))
            and cust["bowler_value_score"] >= thresh):
        fire_meta_capi_event(cur, cust, "Qualified_Bowler", 450.0, thresh, event_id=f"qb_pw_{key}")
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
        geocoded = no_coordinates = failures = newly_excluded = skipped = 0
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
                    continue
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
                "failed_retry_later": failures, "skipped_already_handled": skipped}
    finally:
        conn.close()


# Customers the catch-up may buy: nothing stored yet, or only a claim older than an hour (a run that died).
OA_UNCLAIMED = ("(COALESCE(versium_online_audience, '{}'::jsonb) = '{}'::jsonb OR "
                "(versium_online_audience - '_pending' = '{}'::jsonb AND "
                "(versium_online_audience->>'_pending')::float < extract(epoch FROM now()) - 3600))")


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


@app.get("/health")
def health():
    """Liveness check. Also shows which authentication checks are switched on (no data exposed)."""
    return {
        "status": "ok",
        "square_signature_check": bool(SQUARE_WEBHOOK_SIGNATURE_KEY),
        "webhook_key_check": bool(WEBHOOK_SECRET),
        "cron_key_check": bool(CRON_SECRET),
        "partywirks_signature_check": ("enforced" if PARTYWIRKS_SIGNATURE_ENFORCE else "logging only")
                                      if PARTYWIRKS_WEBHOOK_SECRET else False,
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
