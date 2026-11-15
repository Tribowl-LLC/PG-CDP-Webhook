import os
import math
import time
import json
import hashlib
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Request, BackgroundTasks

app = FastAPI(title="Perfect Game CDP, Enrichment & Meta CAPI Engine")

# Environment Variables (Configure in Render Dashboard)
DATABASE_URL = os.getenv("DATABASE_URL")
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN")
MELISSA_API_KEY = os.getenv("MELISSA_API_KEY")
VERSIUM_API_KEY = os.getenv("VERSIUM_API_KEY")
META_PIXEL_ID = os.getenv("META_PIXEL_ID")
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")

# Your 4 Live Non-Detroit Meta Custom Audience IDs
AUDIENCE_PG1_CORP = os.getenv("AUDIENCE_PG1_CORP", "120246555016470358")
AUDIENCE_PG2_FAMILIES = os.getenv("AUDIENCE_PG2_FAMILIES", "120246555006040358")
AUDIENCE_PRIME_SUBURBS = os.getenv("AUDIENCE_PRIME_SUBURBS", "120246555019320358")
AUDIENCE_ALL_NONDETROIT = os.getenv("AUDIENCE_ALL_NONDETROIT", "120246555027130358")

ALLEY_LAT, ALLEY_LON = 42.4705, -83.3932
EXCLUDED_CITIES = {
    "detroit", "pontiac", "redford", "inkster",
    "highland park", "hamtramck", "southfield", "lathrup village"
}
EXCLUDED_ZIPS = {"48033", "48034", "48037", "48075", "48076"}
PRIME_SUBURB_ZIPS = {
    "48331", "48334", "48335", "48336", "48374", "48375", "48377",
    "48150", "48152", "48154", "48167", "48168", "48322", "48323",
    "48324", "48301", "48302", "48304", "48009", "48025", "48170",
    "48187", "48188", "48382", "48390", "48393", "48178"
}


def sha256_hash(val: str) -> str:
    if not val:
        return ""
    return hashlib.sha256(str(val).strip().lower().encode("utf-8")).hexdigest()


def clean_phone(phone: str) -> str:
    digits = "".join(c for c in str(phone or "") if c.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) == 10 else ""


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return round(2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a)), 2)


def is_detroit_excluded(city: str, zip5: str) -> bool:
    c = (city or "").strip().lower()
    z = (zip5 or "").strip()[:5]
    return c in EXCLUDED_CITIES or z.startswith("482") or z in EXCLUDED_ZIPS


def fetch_square_customer_profile(sq_id: str) -> dict:
    """Pulls full Name, Email, Phone, and Address from Square API when a POS swipe only passes customer_id."""
    if not SQUARE_ACCESS_TOKEN or not sq_id:
        return {}
    try:
        resp = requests.get(
            f"https://connect.squareup.com/v2/customers/{sq_id}",
            headers={"Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}", "Accept": "application/json"},
            timeout=4,
        ).json()
        cust = resp.get("customer", {})
        addr = cust.get("address", {})
        return {
            "first_name": cust.get("given_name", ""),
            "last_name": cust.get("family_name", ""),
            "email": cust.get("email_address", ""),
            "phone": cust.get("phone_number", ""),
            "address": addr.get("address_line_1", ""),
            "city": addr.get("locality", ""),
            "state": addr.get("administrative_district_level_1", ""),
            "zip": addr.get("postal_code", ""),
        }
    except Exception:
        return {}


def run_melissa_and_versium_waterfall(first_name: str, last_name: str, email: str, phone: str, address: str = "", city: str = "", state: str = "", zip_code: str = "") -> dict:
    """Step 1: Melissa Personator (Address, Lat/Long, Demographics). Step 2: Versium REACH (Alt Emails/Phones, Wealth, B2B)."""
    result = {
        "alt_emails": [],
        "alt_phones": [],
        "lifestyle_flags": {},
    }
    if not any([email, phone, address]):
        return result

    # --- STEP 1: MELISSA PERSONATOR ---
    if MELISSA_API_KEY:
        try:
            m_resp = requests.get(
                "https://personator.melissadata.net/v3/WEB/ContactVerify/doContactVerify",
                params={
                    "id": MELISSA_API_KEY,
                    "act": "Check,Verify,Append",
                    "cols": "GrpAddressDetails,GrpGeo,GrpDemographicBasic",
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
            ).json()
            rec = m_resp.get("Records", [{}])[0]
            has_street = bool((rec.get("AddressLine1") or address or "").strip())
            lat = float(rec.get("Latitude") or 0) if has_street else 0.0
            lon = float(rec.get("Longitude") or 0) if has_street else 0.0
            dist = haversine_miles(ALLEY_LAT, ALLEY_LON, lat, lon) if (lat and lon) else None
            result.update({
                "md_address1": rec.get("AddressLine1") or address or None,
                # Only trust Melissa's city/zip if a street address was matched (prevents 248 phone-switch 'Detroit' fallback)
                "md_city": (rec.get("City") if has_street else None) or city or None,
                "md_state": (rec.get("State") if has_street else None) or state or None,
                "zip5": ((rec.get("PostalCode") or zip_code)[:5] if has_street else None) or (zip_code[:5] if zip_code else None),
                "md_plus4": rec.get("Plus4") if has_street else None,
                "md_carrier_
