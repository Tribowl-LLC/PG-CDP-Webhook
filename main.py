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
                "md_city": (rec.get("City") if has_street else None) or city or None,
                "md_state": (rec.get("State") if has_street else None) or state or None,
                "zip5": ((rec.get("PostalCode") or zip_code)[:5] if has_street else None) or (zip_code[:5] if zip_code else None),
                "md_plus4": rec.get("Plus4") if has_street else None,
                "md_carrier_route": rec.get("CarrierRoute") if has_street else None,
                "md_latitude": lat if lat else None,
                "md_longitude": lon if lon else None,
                "distance_miles": dist,
                "md_household_income": rec.get("HouseholdIncome"),
                "md_presence_of_children": rec.get("PresenceOfChildren"),
                "md_children_age_range": rec.get("ChildrenAgeRange"),
                "md_own_rent": rec.get("HomeOwnerStatus"),
                "md_marital_status": rec.get("MaritalStatus"),
                "md_household_size": rec.get("HouseholdSize"),
                "md_length_of_residence": rec.get("LengthOfResidence"),
                "md_education": rec.get("Education"),
                "md_occupation": rec.get("Occupation"),
                "md_date_of_birth": rec.get("DateOfBirth"),
                "md_gender": rec.get("Gender"),
                "md_delivery_indicator": rec.get("DeliveryIndicator"),
                "md_mik": rec.get("MelissaIdentityKey"),
                "md_mak": rec.get("MelissaAddressKey"),
                "md_results": rec.get("Results"),
            })
        except Exception:
            pass

    # --- STEP 2: VERSIUM REACH (When API Key is active) ---
    if VERSIUM_API_KEY and not is_detroit_excluded(result.get("md_city"), result.get("zip5")):
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
            v_res = vc.get("versium", {}).get("results", [{}])[0]
            result["alt_emails"] = [e for e in [v_res.get(f"email_{i}") for i in range(1, 6)] if e]
            result["alt_phones"] = [clean_phone(p) for p in [v_res.get(f"phone_{i}") for i in range(1, 6)] if clean_phone(p)]

            vd = requests.get(
                "https://api.versium.com/v2/demographic",
                headers=v_headers,
                params={**v_input, "output[]": ["basic_demographic", "financial_household_auto", "lifestyle_interests"]},
                timeout=5,
            ).json()
            vd_res = vd.get("versium", {}).get("results", [{}])[0]
            result["versium_net_worth"] = vd_res.get("net_worth")
            result["versium_home_market_value"] = vd_res.get("home_market_value")
            result["versium_auto_make_model"] = f"{vd_res.get('auto_make', '')} {vd_res.get('auto_model', '')}".strip()
            result["lifestyle_flags"] = vd_res
        except Exception:
            pass

    return result


def score_and_assign_persona(cust: dict) -> tuple[str, int]:
    kids_raw = str(cust.get("md_presence_of_children") or "").strip().upper()
    has_kids = kids_raw.startswith("Y") or (kids_raw.startswith("CHILDREN") and "NO" not in kids_raw)

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


def build_meta_user_identifiers(cust: dict) -> dict:
    """Combines primary + Versium alternate emails & phones so online bookings and in-store POS swipes stitch to the same Meta profile."""
    emails = [cust.get("email")] + (cust.get("versium_alt_emails") or [])
    phones = [cust.get("phone")] + (cust.get("versium_alt_phones") or [])
    hashed_emails = list({sha256_hash(e) for e in emails if e})
    hashed_phones = list({sha256_hash(p) for p in phones if p})

    user_data = {
        "em": hashed_emails,
        "ph": hashed_phones,
        "fn": [sha256_hash(cust.get("first_name"))] if cust.get("first_name") else [],
        "ln": [sha256_hash(cust.get("last_name"))] if cust.get("last_name") else [],
        "ct": [sha256_hash(cust.get("md_city"))] if cust.get("md_city") else [],
        "st": [sha256_hash(cust.get("md_state") or "mi")],
        "zp": [sha256_hash(cust.get("zip5"))] if cust.get("zip5") else [],
        "country": [sha256_hash("us")],
        "external_id": [sha256_hash(str(cust["cluster_id"]))],
    }
    return {k: v for k, v in user_data.items() if v}


def sync_customer_to_meta_custom_audiences(cust: dict):
    """Pushes qualified Non-Detroit/Non-Southfield customers directly into your 4 Meta Value-Based Custom Audiences."""
    if not META_ACCESS_TOKEN or is_detroit_excluded(cust.get("md_city"), cust.get("zip5")):
        return

    persona = cust.get("pg_mosaic_persona") or ""
    ltv = max(float(cust.get("true_lifetime_spend") or 0), 450.0 if cust.get("in_tripleseat") else 0.0)
    target_audiences = [AUDIENCE_ALL_NONDETROIT]

    if persona.startswith("PG-1") or cust.get("in_tripleseat") == 1 or ltv >= 600:
        target_audiences.append(AUDIENCE_PG1_CORP)
    if persona.startswith("PG-2"):
        target_audiences.append(AUDIENCE_PG2_FAMILIES)
    if (cust.get("zip5") or "") in PRIME_SUBURB_ZIPS:
        target_audiences.append(AUDIENCE_PRIME_SUBURBS)

    schema = ["EMAIL", "PHONE", "FN", "LN", "CT", "ST", "ZIP", "COUNTRY", "LOOKALIKE_VALUE"]
    row = [
        sha256_hash(cust.get("email", "")),
        sha256_hash(cust.get("phone", "")),
        sha256_hash(cust.get("first_name", "")),
        sha256_hash(cust.get("last_name", "")),
        sha256_hash(cust.get("md_city", "")),
        sha256_hash(cust.get("md_state") or "mi"),
        sha256_hash(cust.get("zip5", "")),
        sha256_hash("us"),
        round(ltv, 2),
    ]

    for aud_id in set(target_audiences):
        if aud_id:
            try:
                requests.post(
                    f"https://graph.facebook.com/v21.0/{aud_id}/users",
                    params={"access_token": META_ACCESS_TOKEN},
                    json={"payload": {"schema": schema, "data": [row]}},
                    timeout=5,
                )
            except Exception:
                pass


def fire_meta_capi_event(cur, cust: dict, event_name: str, value: float, threshold: int, event_id_override: str = None):
    if not META_PIXEL_ID or not META_ACCESS_TOKEN:
        return
    cluster_id = cust["cluster_id"]
    score = int(cust.get("bowler_value_score") or 0)
    event_id = event_id_override or f"{event_name}_{cluster_id}_{int(time.time())}"

    payload = {
        "data": [{
            "event_name": event_name,
            "event_time": int(time.time()),
            "event_id": event_id,
            "action_source": "system_generated",
            "user_data": build_meta_user_identifiers(cust),
            "custom_data": {
                "currency": "USD",
                "value": round(float(value), 2),
                "pg_persona": cust.get("pg_mosaic_persona"),
                "bowler_value_score": score,
            },
        }]
    }
    resp = requests.post(
        f"https://graph.facebook.com/v21.0/{META_PIXEL_ID}/events?access_token={META_ACCESS_TOKEN}",
        json=payload,
        timeout=5,
    )
    cur.execute(
        """
        INSERT INTO capi_events_log (
            event_id, cluster_id, event_name, event_value, bowler_value_score,
            threshold_at_send, sent_to_meta, meta_response_status, meta_response_body
        ) VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s, %s)
        ON CONFLICT (event_id) DO NOTHING;
        """,
        (event_id, cluster_id, event_name, value, score, threshold, str(resp.status_code), resp.text[:1000]),
    )


def upsert_and_enrich_customer(cur, data: dict) -> tuple[dict, bool]:
    """Resolves identity across Square/WebRes/Tripleseat, progressively enriches via Melissa+Versium if missing address, and updates master_customers."""
    sq_id = (data.get("sq_id") or "").strip()
    if sq_id and not (data.get("email") or data.get("phone")):
        sq_profile = fetch_square_customer_profile(sq_id)
        for k, v in sq_profile.items():
            if v and not data.get(k):
                data[k] = v

    email = (data.get("email") or "").strip().lower()
    phone = clean_phone(data.get("phone") or "")
    first_name = (data.get("first_name") or "").strip()
    last_name = (data.get("last_name") or "").strip()

    # 1. Identity Lookup
    existing = None
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
            existing = cur.fetchone()
            if existing:
                break

    # 2. Progressive Enrichment Trigger:
    needs_enrichment = (existing is None) or (not existing.get("md_address1") and (email or phone))
    enriched = {}
    if needs_enrichment and (email or phone or data.get("address")):
        enriched = run_melissa_and_versium_waterfall(
            first_name or (existing or {}).get("first_name", ""),
            last_name or (existing or {}).get("last_name", ""),
            email or (existing or {}).get("email", ""),
            phone or (existing or {}).get("phone", ""),
            data.get("address", ""),
            data.get("city", ""),
            data.get("state", ""),
            data.get("zip", ""),
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

    if existing:
        cluster_id = existing["cluster_id"]
    else:
        cur.execute("SELECT COALESCE(MAX(cluster_id), 24220) + 1 AS next_id FROM master_customers;")
        cluster_id = cur.fetchone()["next_id"]

    sq_delta = float(data.get("sq_spend_delta") or 0.0)
    wr_delta = float(data.get("wr_spend_delta") or 0.0)
    is_ts = 1 if data.get("in_tripleseat") else 0
    is_sq = 1 if data.get("in_square") else 0
    is_wr = 1 if data.get("in_webres") else 0
    guest_count = int(data.get("ts_max_guests") or 0)

    # 3. Upsert into master_customers
    cur.execute(
        """
        INSERT INTO master_customers (
            cluster_id, first_name, last_name, full_name, email, phone, source,
            in_tripleseat, in_square, in_webres,
            sq_spend, wr_spend, true_lifetime_spend, webres_online_spend, instore_pos_spend,
            sq_visits, wr_bookings, ts_events, ts_account, sq_id,
            md_address1, md_city, md_state, zip5, md_plus4, md_carrier_route,
            distance_miles, md_latitude, md_longitude, md_household_income,
            md_presence_of_children, md_children_age_range, md_own_rent, md_marital_status,
            md_household_size, md_length_of_residence, md_education, md_occupation,
            md_date_of_birth, md_gender, md_delivery_indicator, md_mik, md_mak, md_results,
            versium_net_worth, versium_home_market_value, versium_auto_make_model,
            versium_alt_emails, versium_alt_phones, versium_lifestyle_flags,
            ts_lead_status, ts_max_guests, ts_last_event_date, ts_turned_down_reason, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, GREATEST(%s, %s), %s, GREATEST(0, %s - %s),
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s::jsonb, %s::jsonb, %s::jsonb,
            %s, %s, %s, %s, NOW()
        )
        ON CONFLICT (cluster_id) DO UPDATE SET
            first_name = COALESCE(NULLIF(EXCLUDED.first_name, ''), master_customers.first_name),
            last_name = COALESCE(NULLIF(EXCLUDED.last_name, ''), master_customers.last_name),
            email = COALESCE(NULLIF(EXCLUDED.email, ''), master_customers.email),
            phone = COALESCE(NULLIF(EXCLUDED.phone, ''), master_customers.phone),
            in_tripleseat = GREATEST(master_customers.in_tripleseat, EXCLUDED.in_tripleseat),
            in_square = GREATEST(master_customers.in_square, EXCLUDED.in_square),
            in_webres = GREATEST(master_customers.in_webres, EXCLUDED.in_webres),
            sq_spend = master_customers.sq_spend + EXCLUDED.sq_spend,
            wr_spend = master_customers.wr_spend + EXCLUDED.wr_spend,
            true_lifetime_spend = GREATEST(master_customers.sq_spend + EXCLUDED.sq_spend, master_customers.wr_spend + EXCLUDED.wr_spend),
            webres_online_spend = master_customers.wr_spend + EXCLUDED.wr_spend,
            instore_pos_spend = GREATEST(0, (master_customers.sq_spend + EXCLUDED.sq_spend) - (master_customers.wr_spend + EXCLUDED.wr_spend)),
            sq_visits = master_customers.sq_visits + EXCLUDED.sq_visits,
            wr_bookings = master_customers.wr_bookings + EXCLUDED.wr_bookings,
            ts_events = master_customers.ts_events + EXCLUDED.ts_events,
            ts_account = COALESCE(EXCLUDED.ts_account, master_customers.ts_account),
            sq_id = COALESCE(NULLIF(EXCLUDED.sq_id, ''), master_customers.sq_id),
            md_address1 = COALESCE(EXCLUDED.md_address1, master_customers.md_address1),
            md_city = COALESCE(EXCLUDED.md_city, master_customers.md_city),
            md_state = COALESCE(EXCLUDED.md_state, master_customers.md_state),
            zip5 = COALESCE(EXCLUDED.zip5, master_customers.zip5),
            distance_miles = COALESCE(EXCLUDED.distance_miles, master_customers.distance_miles),
            md_household_income = COALESCE(EXCLUDED.md_household_income, master_customers.md_household_income),
            md_presence_of_children = COALESCE(EXCLUDED.md_presence_of_children, master_customers.md_presence_of_children),
            md_children_age_range = COALESCE(EXCLUDED.md_children_age_range, master_customers.md_children_age_range),
            md_own_rent = COALESCE(EXCLUDED.md_own_rent, master_customers.md_own_rent),
            md_delivery_indicator = COALESCE(EXCLUDED.md_delivery_indicator, master_customers.md_delivery_indicator),
            md_mik = COALESCE(EXCLUDED.md_mik, master_customers.md_mik),
            md_mak = COALESCE(EXCLUDED.md_mak, master_customers.md_mak),
            versium_net_worth = COALESCE(EXCLUDED.versium_net_worth, master_customers.versium_net_worth),
            versium_home_market_value = COALESCE(EXCLUDED.versium_home_market_value, master_customers.versium_home_market_value),
            versium_alt_emails = CASE WHEN jsonb_array_length(EXCLUDED.versium_alt_emails) > 0 THEN EXCLUDED.versium_alt_emails ELSE master_customers.versium_alt_emails END,
            versium_alt_phones = CASE WHEN jsonb_array_length(EXCLUDED.versium_alt_phones) > 0 THEN EXCLUDED.versium_alt_phones ELSE master_customers.versium_alt_phones END,
            ts_lead_status = COALESCE(EXCLUDED.ts_lead_status, master_customers.ts_lead_status),
            ts_max_guests = GREATEST(COALESCE(master_customers.ts_max_guests, 0), COALESCE(EXCLUDED.ts_max_guests, 0)),
            ts_last_event_date = COALESCE(EXCLUDED.ts_last_event_date, master_customers.ts_last_event_date),
            ts_turned_down_reason = COALESCE(EXCLUDED.ts_turned_down_reason, master_customers.ts_turned_down_reason),
            updated_at = NOW()
        RETURNING *;
        """,
        (
            cluster_id, first_name, last_name, f"{first_name} {last_name}".strip(), email, phone, data.get("source", "Webhook"),
            is_ts, is_sq, is_wr,
            sq_delta, wr_delta, sq_delta, wr_delta, wr_delta, sq_delta, wr_delta,
            1 if is_sq else 0, 1 if is_wr else 0, 1 if is_ts else 0, data.get("ts_account"), sq_id,
            enriched.get("md_address1"), enriched.get("md_city"), enriched.get("md_state"), enriched.get("zip5"),
            enriched.get("md_plus4"), enriched.get("md_carrier_route"), enriched.get("distance_miles"),
            enriched.get("md_latitude"), enriched.get("md_longitude"), enriched.get("md_household_income"),
            enriched.get("md_presence_of_children"), enriched.get("md_children_age_range"), enriched.get("md_own_rent"),
            enriched.get("md_marital_status"), enriched.get("md_household_size"), enriched.get("md_length_of_residence"),
            enriched.get("md_education"), enriched.get("md_occupation"), enriched.get("md_date_of_birth"),
            enriched.get("md_gender"), enriched.get("md_delivery_indicator"), enriched.get("md_mik"),
            enriched.get("md_mak"), enriched.get("md_results"),
            enriched.get("versium_net_worth"), enriched.get("versium_home_market_value"), enriched.get("versium_auto_make_model"),
            json.dumps(enriched.get("alt_emails", [])), json.dumps(enriched.get("alt_phones", [])), json.dumps(enriched.get("lifestyle_flags", {})),
            data.get("ts_lead_status"), guest_count or None, data.get("ts_last_event_date"), data.get("ts_turned_down_reason")
        ),
    )
    cust = cur.fetchone()

    # 4. Recalculate Persona & Bowler Value Score with combined data
    persona, score = score_and_assign_persona(cust)
    cur.execute(
        """
        UPDATE master_customers
        SET pg_mosaic_persona = %s,
            bowler_value_score = CASE
                WHEN %s = 25 THEN 25
                ELSE GREATEST(COALESCE(bowler_value_score, 0), %s)
            END
        WHERE cluster_id = %s RETURNING *;
        """,
        (persona, score, score, cluster_id),
    )
    cust = cur.fetchone()

    # 5. Index all primary & Versium alternate identifiers into customer_identities
    id_pairs = [("email", email), ("phone", phone), ("sq_id", sq_id), ("md_mik", enriched.get("md_mik"))]
    for alt_e in enriched.get("alt_emails", []):
        id_pairs.append(("email", alt_e.strip().lower()))
    for alt_p in enriched.get("alt_phones", []):
        id_pairs.append(("phone", alt_p))

    for id_type, id_val in id_pairs:
        if id_val:
            cur.execute(
                """
                INSERT INTO customer_identities (cluster_id, id_type, id_value, source_system)
                VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;
                """,
                (cluster_id, id_type, id_val, data.get("source", "Webhook")),
            )

    return cust, needs_enrichment


@app.post("/webhooks/square")
async def square_webhook(request: Request, background_tasks: BackgroundTasks):
    """Handles both Square POS in-store payments (payment.updated) and new Square profiles (customer.created)."""
    body = await request.json()
    event_type = body.get("type", "")
    obj = body.get("data", {}).get("object", {})

    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    try:
        with conn:
            with conn.cursor() as cur:
                if event_type.startswith("payment."):
                    payment = obj.get("payment", {})
                    if payment.get("status") != "COMPLETED":
                        return {"status": "ignored_incomplete_payment"}

                    payment_id = payment.get("id")
                    amount_dollars = float(payment.get("amount_money", {}).get("amount", 0)) / 100.0
                    sq_id = payment.get("customer_id") or ""
                    buyer_email = payment.get("buyer_email_address") or ""

                    cust, newly_enriched = upsert_and_enrich_customer(cur, {
                        "sq_id": sq_id,
                        "email": buyer_email,
                        "sq_spend_delta": amount_dollars,
                        "in_square": 1,
                        "source": "Square_POS",
                    })

                    cur.execute("SELECT current_score_threshold FROM cdp_thermostat_config WHERE id = 1;")
                    thresh = cur.fetchone()["current_score_threshold"]

                    if not is_detroit_excluded(cust.get("md_city"), cust.get("zip5")):
                        fire_meta_capi_event(cur, cust, "Purchase", amount_dollars, thresh, event_id_override=f"sq_pay_{payment_id}")
                        if cust["bowler_value_score"] >= thresh and newly_enriched:
                            background_tasks.add_task(sync_customer_to_meta_custom_audiences, dict(cust))

                    return {"status": "payment_attributed", "cluster_id": cust["cluster_id"], "amount": amount_dollars}

                elif event_type.startswith("customer."):
                    c_obj = obj.get("customer", {})
                    addr = c_obj.get("address", {})
                    cust, newly_enriched = upsert_and_enrich_customer(cur, {
                        "sq_id": c_obj.get("id"),
                        "first_name": c_obj.get("given_name"),
                        "last_name": c_obj.get("family_name"),
                        "email": c_obj.get("email_address"),
                        "phone": c_obj.get("phone_number"),
                        "address": addr.get("address_line_1"),
                        "city": addr.get("locality"),
                        "state": addr.get("administrative_district_level_1"),
                        "zip": addr.get("postal_code"),
                        "in_square": 1,
                        "source": "Square_Customer",
                    })
                    cur.execute("SELECT current_score_threshold FROM cdp_thermostat_config WHERE id = 1;")
                    thresh = cur.fetchone()["current_score_threshold"]
                    if not is_detroit_excluded(cust.get("md_city"), cust.get("zip5")) and cust["bowler_value_score"] >= thresh:
                        background_tasks.add_task(sync_customer_to_meta_custom_audiences, dict(cust))
                    return {"status": "customer_synced", "cluster_id": cust["cluster_id"]}
    finally:
        conn.close()
    return {"status": "ignored"}


@app.post("/webhooks/webres")
async def webres_webhook(request: Request, background_tasks: BackgroundTasks):
    """Receives online lane reservations (WebRes/BowlNow), enriches contact data, updates audiences, and fires Purchase CAPI."""
    data = await request.json()
    amount = float(data.get("total_amount") or data.get("amount") or 0.0)
    booking_id = data.get("reservation_id") or data.get("id") or str(int(time.time()))

    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    try:
        with conn:
            with conn.cursor() as cur:
                cust, _ = upsert_and_enrich_customer(cur, {
                    "first_name": data.get("first_name"),
                    "last_name": data.get("last_name"),
                    "email": data.get("email"),
                    "phone": data.get("phone"),
                    "wr_spend_delta": amount,
                    "in_webres": 1,
                    "source": "WebRes",
                })
                cur.execute("SELECT current_score_threshold FROM cdp_thermostat_config WHERE id = 1;")
                thresh = cur.fetchone()["current_score_threshold"]

                if not is_detroit_excluded(cust.get("md_city"), cust.get("zip5")):
                    if amount > 0:
                        fire_meta_capi_event(cur, cust, "Purchase", amount, thresh, event_id_override=f"wr_{booking_id}")
                    if cust["bowler_value_score"] >= thresh:
                        fire_meta_capi_event(cur, cust, "Qualified_Bowler", max(amount, 250.0), thresh, event_id_override=f"qb_wr_{booking_id}")
                        background_tasks.add_task(sync_customer_to_meta_custom_audiences, dict(cust))

        return {"status": "webres_synced", "cluster_id": cust["cluster_id"], "score": cust["bowler_value_score"]}
    finally:
        conn.close()


@app.post("/webhooks/tripleseat")
async def tripleseat_webhook(request: Request, background_tasks: BackgroundTasks):
    """Unwraps native Tripleseat Lead, Event, Booking, and Contact webhooks, enriches via Melissa+Versium, and fires Qualified_Bowler CAPI."""
    raw = await request.json()

    obj = (
        raw.get("lead")
        or raw.get("event")
        or raw.get("booking")
        or raw.get("contact")
        or raw
    )
    contact = obj.get("contact") or obj
    account = obj.get("account") or contact.get("account") or {}

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
    company = (
        account.get("name")
        or obj.get("company")
        or obj.get("account_name")
        or contact.get("company")
        or ""
    )
    status = (obj.get("status") or obj.get("lead_status") or "PROSPECT").upper()
    guest_count = int(
        obj.get("guest_count")
        or obj.get("guaranteed_guest_count")
        or obj.get("expected_guest_count")
        or 0
    )
    event_date = (
        obj.get("event_date")
        or obj.get("event_start_iso8601")
        or obj.get("desired_date")
        or None
    )
    if event_date and isinstance(event_date, str) and "T" in event_date:
        event_date = event_date.split("T")[0]

    turned_down_reason = (
        obj.get("turned_down_reason")
        or obj.get("lost_reason")
        or None
    )

    if not any([email, phone, first_name, last_name]):
        return {"status": "ignored_empty_tripleseat_payload"}

    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
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
                cur.execute("SELECT current_score_threshold FROM cdp_thermostat_config WHERE id = 1;")
                thresh = cur.fetchone()["current_score_threshold"]

                if (
                    not is_detroit_excluded(cust.get("md_city"), cust.get("zip5"))
                    and cust["bowler_value_score"] >= thresh
                    and status not in ("LOST", "TURNED_DOWN")
                ):
                    synthetic_val = 1000.0 if (cust.get("pg_mosaic_persona") or "").startswith("PG-1") else 450.0
                    fire_meta_capi_event(cur, cust, "Qualified_Bowler", synthetic_val, thresh)
                    background_tasks.add_task(sync_customer_to_meta_custom_audiences, dict(cust))

        return {
            "status": "tripleseat_synced",
            "cluster_id": cust["cluster_id"],
            "lead_status": status,
            "score": cust["bowler_value_score"],
        }
    finally:
        conn.close()


@app.post("/cron/adjust-thermostat")
async def adjust_thermostat():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT adjust_meta_capi_thermostat() AS new_threshold;")
                res = cur.fetchone()
        return {"status": "adjusted", "new_threshold": res["new_threshold"]}
    finally:
        conn.close()


@app.post("/cron/sync-square-catalog")
async def sync_square_catalog():
    """Pulls all Square Catalog Categories, Items, and Price Key Variations into square_catalog_items in Neon."""
    if not SQUARE_ACCESS_TOKEN:
        return {"status": "error", "message": "Missing SQUARE_ACCESS_TOKEN"}

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
        objs = resp.get("objects", [])
        all_objects.extend(objs)
        cursor = resp.get("cursor")
        if not cursor:
            break

    categories = {}
    for obj in all_objects:
        if obj.get("type") == "CATEGORY":
            cat_name = obj.get("category_data", {}).get("name", "")
            categories[obj["id"]] = cat_name

    rows_to_upsert = []
    for obj in all_objects:
        if obj.get("type") != "ITEM":
            continue
        item_id = obj.get("id")
        is_deleted = bool(obj.get("is_deleted", False))
        item_data = obj.get("item_data", {})
        item_name = item_data.get("name", "")

        cat_id = item_data.get("category_id") or ""
        if not cat_id and isinstance(item_data.get("categories"), list) and item_data["categories"]:
            cat_id = item_data["categories"][0].get("id", "")
        if not cat_id and item_data.get("reporting_category", {}).get("id"):
            cat_id = item_data["reporting_category"]["id"]
        cat_name = categories.get(cat_id, "")

        for var in item_data.get("variations", []):
            var_id = var.get("id")
            var_data = var.get("item_variation_data", {})
            var_name = var_data.get("name", "")
            sku = var_data.get("sku", "")
            price_cents = var_data.get("price_money", {}).get("amount", 0)
            price_dollars = round(float(price_cents or 0) / 100.0, 2)

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

    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    try:
        with conn:
            with conn.cursor() as cur:
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
