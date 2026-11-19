import json
import main


def test_grosse_pointe_is_allowed_but_detroit_is_not():
    for city, z in [("Grosse Pointe", "48230"), ("Grosse Pointe Park", "48230"), ("Grosse Pointe Farms", "48236"),
                    ("Grosse Pointe Woods", "48236"), ("Grosse Pointe Shores", "48236"), (None, "48230")]:
        assert not main.is_detroit_excluded(city, z), (city, z)
    for city, z in [("Detroit", "48226"), (None, "48224"), (None, "48201"), ("Harper Woods", "48236"),
                    ("Detroit", "48236"), ("Hamtramck", "48212"), ("Southfield", "48075"), (None, "48341"),
                    ("Redford Twp", "48239"), ("  oak   park ", "48237")]:
        assert main.is_detroit_excluded(city, z), (city, z)
    assert not main.is_detroit_excluded("Farmington Hills", "48335")


def test_meta_formats():
    assert main.meta_phone("(248) 555-0100") == "12485550100"
    assert main.meta_phone("+1 248-555-0100") == "12485550100"
    assert main.meta_phone("555-0100") == ""
    assert main.meta_city("Farmington Hills") == "farmingtonhills"
    assert main.meta_city("St. Clair Shores") == "stclairshores"
    assert main.meta_state("MI") == "mi"
    assert main.meta_state("Michigan") == "mi"
    assert main.meta_state(None) == "mi"
    assert main.meta_state("Ohio") == ""


def test_user_identifiers_hash_formatted_values():
    ud = main.build_meta_user_identifiers({
        "cluster_id": 1, "email": "A@B.com", "phone": "2485550100", "md_city": "Farmington Hills",
        "md_state": "MI", "zip5": "48335", "versium_alt_phones": ["2485550199"],
    })
    assert main.sha256_hash("12485550100") in ud["ph"]
    assert main.sha256_hash("12485550199") in ud["ph"]
    assert ud["ct"] == [main.sha256_hash("farmingtonhills")]
    assert ud["em"] == [main.sha256_hash("a@b.com")]


def test_square_signature_matches_square_algorithm():
    # Square: base64(HMAC-SHA256(key, notification_url + raw_body))
    import base64, hashlib, hmac
    body = b'{"type":"payment.updated"}'
    url = "https://example.test/webhooks/square"
    expected = base64.b64encode(hmac.new(b"k", url.encode() + body, hashlib.sha256).digest()).decode()
    assert main.square_signature(body, "k", url) == expected


def test_cardholder_parsing():
    assert main.parse_cardholder_name("SMITH/JOHN M") == ("John", "Smith")
    assert main.parse_cardholder_name("JANE DOE") == ("Jane", "Doe")
    assert main.parse_cardholder_name("VALUED CUSTOMER") == ("", "")


def test_scoring_unchanged_for_known_case():
    persona, score = main.score_and_assign_persona({
        "in_partywirks": 1, "md_household_income": "$125,000-$149,999", "distance_miles": 5,
        "md_city": "Novi", "zip5": "48375", "true_lifetime_spend": 500,
    })
    assert persona.startswith("PG-2") and score == 100


def test_audience_targets():
    t, ltv = main.audience_targets({"pg_mosaic_persona": "PG-2: x", "zip5": "48335", "md_city": "Farmington Hills",
                                    "in_partywirks": 1, "true_lifetime_spend": 100})
    assert t == [main.AUDIENCE_ALL_NONDETROIT, main.AUDIENCE_PG2_FAMILIES, main.AUDIENCE_PRIME_SUBURBS]
    assert ltv == 450.0
    assert main.audience_targets({"md_city": "Detroit", "zip5": "48226", "pg_mosaic_persona": "PG-1"}) == ([], 0.0)
    assert main.audience_targets({"zip5": "48230", "md_city": "Grosse Pointe"})[0] == [main.AUDIENCE_ALL_NONDETROIT]


def test_rejected_audience_upload_is_logged(fake, caplog):
    fake.meta_mode = "500"
    with caplog.at_level("ERROR", logger="pg_cdp"):
        res = main.upload_audience_rows(main.AUDIENCE_PG1_CORP, [["x"] * 9])
    assert res["ok"] is False
    assert "PG-1 Corporate rejected (HTTP 500)" in caplog.text


def test_accepted_audience_upload_reports_counts(fake, monkeypatch):
    from conftest import FakeResponse
    monkeypatch.setattr(main.requests, "post",
                        lambda url, **kw: FakeResponse(200, {"num_received": 2, "num_invalid_entries": 0}))
    assert main.upload_audience_rows(main.AUDIENCE_PG1_CORP, [[], []]) == {"ok": True, "received": 2, "invalid": 0}


def test_versium_online_append_only_for_local_advertisable_customers():
    assert main.versium_online_append_eligible("Novi", "MI", "48375")
    assert main.versium_online_append_eligible("Novi", "mi", "48375")
    assert main.versium_online_append_eligible(None, None, None)          # unknown location: allowed
    assert main.versium_online_append_eligible("Grosse Pointe", "MI", "48230")
    assert not main.versium_online_append_eligible("Detroit", "MI", "48226")
    assert not main.versium_online_append_eligible("Southfield", "MI", "48075")
    assert not main.versium_online_append_eligible(None, None, "48224")    # Detroit ZIP, no city/state
    assert not main.versium_online_append_eligible("Toledo", "OH", "43604")
    assert not main.versium_online_append_eligible("Austin", "TX", "")


def test_blocked_versium_fields_are_stripped():
    raw = {"net_worth": "$100,000-249,999", "presence_of_children": "Yes", "Credit Rating": "750-799",
           "number_of_credit_lines": "2", "ethnic_group": "x", "Religion": "x", "party_affiliation": "x",
           "donor_political": "Yes", "donor_conservative_politics": "Yes", "donor_liberal_politics": "Yes",
           "credit_card_holder_bank": "Yes", "donor_childrens_causes": "Yes"}
    kept = main.strip_blocked_versium_fields(raw)
    assert set(kept) == {"net_worth", "presence_of_children", "credit_card_holder_bank", "donor_childrens_causes"}
    assert main.strip_blocked_versium_fields(None) == {}


H1, H2, H3 = "a" * 64, "b" * 64, "c" * 64
OA_BODY = {"versium": {"results": [{
    "facebook": [{"email": [H1, H2], "phone": [H3], "zip": ["48335"]}, {"email": [H2.upper()]}],
    "linkedin": [{"email": [H1], "jobtitle": ["Office Manager"], "employeecompany": ["Acme"]}],
    "tiktok": [{"email": [H1], "phone": [H3]}],
    "google": [{"Email": [H1]}],
    "adroll": [{"email": ["d41d8cd98f00b204e9800998ecf8427e"]}],
}]}}


def test_online_audience_parse_keeps_only_meta_google_linkedin_tiktok():
    oa = main.parse_versium_online_audience(OA_BODY)
    assert set(oa) == {"facebook", "google", "linkedin", "tiktok"}
    assert oa["google"]["email"] == [H1]                     # Versium capitalizes Google's keys
    assert oa["facebook"]["email"] == [H1, H2, H2.upper()]
    assert oa["linkedin"]["jobtitle"] == ["Office Manager"]
    assert main.parse_versium_online_audience({}) == {}
    assert main.parse_versium_online_audience(None) == {}


def test_versium_facebook_hashes_join_capi_identifiers():
    cust = {"cluster_id": 1, "email": "a@b.com", "phone": "2485550100",
            "versium_online_audience": main.parse_versium_online_audience(OA_BODY)}
    cust["versium_online_audience"]["facebook"]["phone"].append("not-a-hash")
    ud = main.build_meta_user_identifiers(cust)
    assert {H1, H2, main.sha256_hash("a@b.com")} <= set(ud["em"]) and len(ud["em"]) == 3
    assert H3 in ud["ph"] and "not-a-hash" not in ud["ph"]


def _fake_versium(monkeypatch):
    from conftest import FakeResponse
    calls = []

    def get(url, **kw):
        calls.append(url)
        if "onlineAudience" in url:
            return FakeResponse(200, OA_BODY)
        return FakeResponse(200, {"versium": {"results": [{}]}})
    monkeypatch.setattr(main, "VERSIUM_API_KEY", "test-versium-key")
    monkeypatch.setattr(main, "MELISSA_API_KEY", None)
    monkeypatch.setattr(main.requests, "get", get)
    return calls


def test_waterfall_calls_online_audience_for_michigan(monkeypatch):
    calls = _fake_versium(monkeypatch)
    r = main.run_melissa_and_versium_waterfall("A", "B", "a@b.com", "", "1 Main", "Novi", "MI", "48375")
    assert any("onlineAudience" in u for u in calls)
    assert r["online_audience"]["facebook"]["email"][0] == H1


def test_waterfall_skips_online_audience_out_of_state_and_excluded(monkeypatch):
    calls = _fake_versium(monkeypatch)
    r = main.run_melissa_and_versium_waterfall("A", "B", "a@b.com", "", "1 Main", "Toledo", "OH", "43604")
    assert not any("onlineAudience" in u for u in calls) and r["online_audience"] == {}
    assert main.fetch_versium_online_audience({}, {})["_checked"]                # no-match/match marked checked
    assert any("/v2/contact" in u for u in calls)          # regular Versium append still runs
    calls.clear()
    main.run_melissa_and_versium_waterfall("A", "B", "a@b.com", "", "1 Main", "Detroit", "MI", "48226")
    assert not any("versium.com" in c for c in calls)        # excluded: no Versium at all


def test_stored_geography_gates_versium_when_webhook_has_no_address(monkeypatch):
    calls = _fake_versium(monkeypatch)
    main.run_melissa_and_versium_waterfall("A", "B", "a@b.com", "", known={"md_state": "OH", "zip5": "43604"})
    assert any("/v2/contact" in u for u in calls) and not any("onlineAudience" in u for u in calls)
    calls.clear()
    main.run_melissa_and_versium_waterfall("A", "B", "a@b.com", "", known={"md_city": "Detroit", "zip5": "48226"})
    assert calls == []
    calls.clear()
    # a fresh Michigan address wins over an old out-of-state one
    main.run_melissa_and_versium_waterfall("A", "B", "a@b.com", "", "1 Main", "Novi", "MI", "48375",
                                           known={"md_state": "OH", "zip5": "43604"})
    assert any("onlineAudience" in u for u in calls)


def test_rejected_online_audience_call_is_logged(monkeypatch, caplog):
    from conftest import FakeResponse
    monkeypatch.setattr(main.requests, "get",
                        lambda url, **kw: FakeResponse(400, {"errors": ["Invalid rcfg_oa_services value"]}))
    with caplog.at_level("ERROR", logger="pg_cdp"):
        assert main.fetch_versium_online_audience({}, {"email": "a@b.com"}) is None
    assert "online audience rejected (HTTP 400)" in caplog.text and "a@b.com" not in caplog.text


def test_slow_lookups_skip_online_audience(monkeypatch):
    calls = _fake_versium(monkeypatch)
    monkeypatch.setattr(main, "VERSIUM_ONLINE_BUDGET_SECONDS", -1)
    main.run_melissa_and_versium_waterfall("A", "B", "a@b.com", "", "1 Main", "Novi", "MI", "48375")
    assert any("/v2/contact" in u for u in calls) and not any("onlineAudience" in u for u in calls)


def test_online_audience_not_bought_again_for_checked_customer(monkeypatch):
    calls = _fake_versium(monkeypatch)
    for stored in ({"_checked": "2026-11-18"}, {"facebook": {"email": [H1]}, "_checked": "2026-11-18"}):
        calls.clear()
        main.run_melissa_and_versium_waterfall("A", "B", "a@b.com", "", "1 Main", "Novi", "MI", "48375",
                                               known={"versium_online_audience": stored})
        assert any("/v2/contact" in u for u in calls) and not any("onlineAudience" in u for u in calls)


def test_unreadable_online_audience_answer_stays_retryable(monkeypatch):
    class Bad:
        status_code = 200
        text = "<html>"

        def json(self):
            raise ValueError("not json")
    monkeypatch.setattr(main.requests, "get", lambda url, **kw: Bad())
    assert main.fetch_versium_online_audience({}, {}) is None
    from conftest import FakeResponse
    monkeypatch.setattr(main.requests, "get", lambda url, **kw: FakeResponse(200, ["unexpected"]))
    assert main.fetch_versium_online_audience({}, {}) is None


def test_melissa_lookup_maps_census_fields_only_with_a_street(monkeypatch):
    from conftest import FakeResponse
    monkeypatch.setattr(main, "MELISSA_API_KEY", "k")
    rec = {"AddressLine1": "1 Main", "City": "Novi", "State": "MI", "PostalCode": "48375", "Latitude": "42.48",
           "Longitude": "-83.47", "CensusTract": "141000", "CensusBlock": "2001", "CountyName": "Oakland",
           "UnifiedSchoolDistrictName": "Novi Community School District", "Results": "AS01"}
    seen = {}
    def get(url, params=None, **kw):
        seen.update(params); return FakeResponse(200, {"Records": [rec]})
    monkeypatch.setattr(main.requests, "get", get)
    out = main.melissa_lookup("A", "B", "a@b.com", "", "1 Main", "Novi", "MI", "48375")
    assert seen["act"] == "Check" and "GrpGeocode" in seen["cols"]
    assert (out["md_census_tract"], out["md_county_name"], out["md_unified_school_district_name"]) == \
        ("141000", "Oakland", "Novi Community School District")
    assert out["distance_miles"] is not None
    calls = []
    monkeypatch.setattr(main.requests, "get", lambda url, **kw: calls.append(url))
    out = main.melissa_lookup("A", "B", "a@b.com", "")                 # no street: Check can't help
    assert calls == [] and out == {"md_results": main.MELISSA_NO_STREET}
    monkeypatch.setattr(main, "MELISSA_API_KEY", None)
    assert main.melissa_lookup("A", "B", "a@b.com", "") == {}


def test_blank_melissa_answer_is_a_failure_not_data(monkeypatch, caplog):
    from conftest import FakeResponse
    monkeypatch.setattr(main, "MELISSA_API_KEY", "k")
    rejected = {"TransmissionResults": "GE05", "Records": [{"AddressLine1": " ", "Results": " "}]}
    monkeypatch.setattr(main.requests, "get", lambda url, **kw: FakeResponse(200, rejected))
    with caplog.at_level("ERROR", logger="pg_cdp"):
        assert main.melissa_lookup("A", "B", "a@b.com", "", "12 Real St", "Novi", "MI", "48375") == {}
    assert "GE05" in caplog.text
    # no match: Melissa answered, everything blank -> answered (not retried), blanks never stored
    blank = {"TransmissionResults": " ", "Records": [{"AddressLine1": " ", "HouseholdIncome": " ",
                                                       "MelissaIdentityKey": " ", "Results": " "}]}
    monkeypatch.setattr(main.requests, "get", lambda url, **kw: FakeResponse(200, blank))
    out = main.melissa_lookup("A", "B", "a@b.com", "", "12 Real St", "Novi", "MI", "48375")
    assert out["md_results"] == "NO_RESULT_CODES" and out["md_address1"] == "12 Real St"
    assert out["md_household_income"] is None and out["md_mik"] is None
    padded = {"Records": [{"AddressLine1": "12 Real St", "City": "Novi", "PostalCode": "48375", "Latitude": "42.4",
                           "Longitude": "-83.4", "HouseholdIncome": " ", "MelissaIdentityKey": " ", "Results": "AS01 "}]}
    monkeypatch.setattr(main.requests, "get", lambda url, **kw: FakeResponse(200, padded))
    out = main.melissa_lookup("A", "B", "a@b.com", "", "12 Real St", "Novi", "MI", "48375")
    assert out["md_household_income"] is None and out["md_mik"] is None and out["md_results"] == "AS01"


CENSUS_BODY = {"result": {"addressMatches": [{
    "coordinates": {"x": -83.4757, "y": 42.4806},
    "geographies": {"Census Tracts": [{"TRACT": "141000"}], "2020 Census Blocks": [{"BLOCK": "2001"}],
                    "Counties": [{"BASENAME": "Oakland", "NAME": "Oakland County"}],
                    "Unified School Districts": [{"NAME": "Novi Community School District"}]}}]}}


def test_census_geocode_parses_and_handles_no_match_and_failure(monkeypatch):
    from conftest import FakeResponse
    sent = {}
    def get(url, params=None, **kw):
        sent.update(params); return FakeResponse(200, CENSUS_BODY)
    monkeypatch.setattr(main.requests, "get", get)
    out = main.census_geocode("1 Main St", "Novi", "MI", "48375")
    assert (out["md_census_tract"], out["md_census_block"], out["md_county_name"]) == ("141000", "2001", "Oakland")
    assert out["md_unified_school_district_name"] == "Novi Community School District"
    assert 3 < out["distance_miles"] < 6 and set(sent) >= {"street", "zip"} and "email" not in sent
    assert main.census_geocode("", "Novi", "MI", "48375") is None          # no street: not asked
    assert main.census_geocode("1 Main St", "", "", "") is None            # no ZIP or city/state: not asked
    monkeypatch.setattr(main.requests, "get", lambda url, **kw: FakeResponse(200, {"result": {"addressMatches": []}}))
    assert main.census_geocode("1 Main St", "Novi", "MI", "48375") == {}
    def boom(url, **kw):
        raise main.requests.Timeout("x")
    monkeypatch.setattr(main.requests, "get", boom)
    assert main.census_geocode("1 Main St", "Novi", "MI", "48375") is None


def test_census_coordinates_only_fill_when_melissa_had_none():
    census = {"md_latitude": 1.0, "md_longitude": 2.0, "distance_miles": 3.0, "md_census_tract": "141000"}
    assert main.apply_census({"md_latitude": 42.4}, dict(census)) == {"md_latitude": 42.4, "md_census_tract": "141000"}
    assert main.apply_census({}, dict(census))["md_latitude"] == 1.0


def test_melissa_is_address_only_by_default():
    assert "Demographic" not in main.MELISSA_COLS and "GrpGeo" in main.MELISSA_COLS


def test_waterfall_adds_census_after_melissa(monkeypatch):
    from conftest import FakeResponse
    monkeypatch.setattr(main, "MELISSA_API_KEY", None)
    monkeypatch.setattr(main, "VERSIUM_API_KEY", None)
    monkeypatch.setattr(main.requests, "get", lambda url, **kw: FakeResponse(200, CENSUS_BODY))
    r = main.run_melissa_and_versium_waterfall("A", "B", "a@b.com", "", "1 Main St", "Novi", "MI", "48375")
    assert r["md_census_tract"] == "141000" and r["census_checked_at"] and r["distance_miles"]


def test_unaskable_census_address_is_not_marked_checked(monkeypatch):
    calls = []
    monkeypatch.setattr(main, "MELISSA_API_KEY", None)
    monkeypatch.setattr(main, "VERSIUM_API_KEY", None)
    monkeypatch.setattr(main.requests, "get", lambda url, **kw: calls.append(url))
    r = main.run_melissa_and_versium_waterfall("A", "B", "a@b.com", "", "1 Main St")
    assert calls == [] and "census_checked_at" not in r


def test_tripleseat_field_log_has_names_and_money_presence_but_no_values(caplog):
    raw = {"event": {"id": 99, "grand_total": "1234.50", "amount_due": "0.00", "name": "Smith Party",
                     "contact": {"email_addresses": [{"address": "secret@example.test"}]},
                     "payments": [{"amount": 200, "payment_method": "Check"}]}}
    with caplog.at_level("INFO", logger="pg_cdp"):
        main.log_tripleseat_fields(raw)
    text = caplog.text
    assert "event.grand_total" in text and "event.contact.email_addresses[].address" in text
    assert "filled: event.grand_total" in text and "event.payments[].amount" in text
    assert "empty: event.amount_due" in text
    for secret in ("secret@example.test", "Smith Party", "1234.50", "Check"):
        assert secret not in text


def _tripleseat_on(monkeypatch):
    monkeypatch.setattr(main, "TRIPLESEAT_CLIENT_ID", "test-ts-client")
    monkeypatch.setattr(main, "TRIPLESEAT_CLIENT_SECRET", "test-ts-secret")


def test_tripleseat_oauth_state_must_be_signed_recent_and_match_cookie(monkeypatch):
    _tripleseat_on(monkeypatch)
    state = main.make_tripleseat_state(now=1_000_000)
    assert main.tripleseat_state_valid(state, state, now=1_000_100)
    assert not main.tripleseat_state_valid(state, state, now=1_000_000 + main.TRIPLESEAT_STATE_MAX_AGE + 1)
    assert not main.tripleseat_state_valid(state, "", now=1_000_100)                       # other browser
    assert not main.tripleseat_state_valid(state, main.make_tripleseat_state(1_000_000), now=1_000_100)
    ts, nonce, _sig = state.split(".")
    forged = f"{ts}.{nonce}.{'0' * 64}"
    assert not main.tripleseat_state_valid(forged, forged, now=1_000_100)
    assert not main.tripleseat_state_valid("junk", "junk")


def test_tripleseat_authorize_url(monkeypatch):
    _tripleseat_on(monkeypatch)
    url = main.tripleseat_authorize_url("abc")
    assert url.startswith("https://login.tripleseat.com/oauth2/authorize?")
    assert "client_id=test-ts-client" in url and "response_type=code" in url and "state=abc" in url
    assert "redirect_uri=https%3A%2F%2Fpg-cdp-webhook.onrender.com%2Foauth%2Ftripleseat%2Fcallback" in url
    assert "client_secret" not in url and "scope=" not in url
    monkeypatch.setattr(main, "TRIPLESEAT_SCOPES", "read write")
    assert "scope=read+write" in main.tripleseat_authorize_url("abc")


def test_tripleseat_login_is_off_without_secret_and_needs_cron_key(monkeypatch):
    from fastapi.testclient import TestClient
    client = TestClient(main.app)
    monkeypatch.setattr(main, "TRIPLESEAT_CLIENT_SECRET", None)
    assert client.get("/oauth/tripleseat/login?key=test-cron-secret", follow_redirects=False).status_code == 503
    assert client.get("/oauth/tripleseat/callback?code=x&state=y").status_code == 503
    _tripleseat_on(monkeypatch)
    assert client.get("/oauth/tripleseat/login?key=wrong", follow_redirects=False).status_code == 401
    r = client.get("/oauth/tripleseat/login?key=test-cron-secret", follow_redirects=False)
    assert r.status_code == 302 and r.headers["location"].startswith(main.TRIPLESEAT_AUTHORIZE_URL)
    assert main.TRIPLESEAT_STATE_COOKIE in r.headers["set-cookie"]
    # A callback without the matching browser cookie is refused before any token request.
    assert client.get("/oauth/tripleseat/callback?code=x&state=forged",
                      cookies={main.TRIPLESEAT_STATE_COOKIE: "other"}).status_code == 400


class _FakeTokenDB:
    """Stands in for get_conn() around the oauth_tokens row."""

    def __init__(self, row):
        self.row, self.saved = row, []

    def __call__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return self

    def close(self):
        pass

    def execute(self, sql, params=None):
        if sql.lstrip().startswith("INSERT"):
            self.saved.append(params)

    def fetchone(self):
        return self.row


def test_tripleseat_token_reused_until_near_expiry_then_refreshed(monkeypatch):
    _tripleseat_on(monkeypatch)
    db = _FakeTokenDB({"access_token": "old", "refresh_token": "r1", "seconds_left": 3600})
    monkeypatch.setattr(main, "get_conn", db)
    posts = []

    def fake_post(url, data=None, **kw):
        posts.append(data)
        from conftest import FakeResponse
        return FakeResponse(200, {"access_token": "new", "expires_in": 7200})
    monkeypatch.setattr(main.requests, "post", fake_post)
    assert main.tripleseat_access_token() == "old" and not posts
    db.row["seconds_left"] = 60
    assert main.tripleseat_access_token() == "new"
    assert posts[0]["grant_type"] == "refresh_token" and posts[0]["refresh_token"] == "r1"
    # Tripleseat didn't send a new refresh token, so the old one is kept.
    assert db.saved[0][:2] == ("new", "r1")


def test_tripleseat_token_not_connected(monkeypatch):
    import pytest
    monkeypatch.setattr(main, "TRIPLESEAT_CLIENT_SECRET", None)
    with pytest.raises(main.TripleseatNotConnected):
        main.tripleseat_access_token()
    _tripleseat_on(monkeypatch)
    monkeypatch.setattr(main, "get_conn", _FakeTokenDB(None))
    with pytest.raises(main.TripleseatNotConnected):
        main.tripleseat_access_token()


def test_tripleseat_get_renews_once_on_401_and_waits_on_429(monkeypatch):
    from conftest import FakeResponse
    tokens = iter(["t1", "t2"])
    monkeypatch.setattr(main, "tripleseat_access_token", lambda force_refresh=False: next(tokens))
    monkeypatch.setattr(main.time, "sleep", lambda s: None)
    answers = [FakeResponse(401, {}), FakeResponse(429, {}), FakeResponse(200, [{"id": 1}])]
    seen = []

    def fake_get(url, headers=None, **kw):
        seen.append((url, headers["Authorization"]))
        r = answers.pop(0)
        r.headers = {}
        return r
    monkeypatch.setattr(main.requests, "get", fake_get)
    assert main.tripleseat_get("/sites.json") == [{"id": 1}]
    assert seen[0] == ("https://api.tripleseat.com/v1/sites.json", "Bearer t1")
    assert [h for _u, h in seen[1:]] == ["Bearer t2", "Bearer t2"]


def test_tripleseat_money_and_dates():
    assert main.ts_money("1,234.50") == 1234.5 and main.ts_money("$20") == 20.0
    assert main.ts_money("") is None and main.ts_money("n/a") is None and main.ts_money(None) is None
    assert main.ts_date("2026-12-05T18:00:00-05:00") == "2026-12-05"
    assert main.ts_date("3/7/2025") == "2025-03-07"
    assert main.ts_date("soon") is None and main.ts_date(None) is None
    assert main.ts_timestamp("2026-01-02T03:04:05Z") and main.ts_timestamp("01/02/2026") is None


def test_tripleseat_payment_summary_separates_card_from_check_and_refunds():
    out = main.ts_payment_summary([
        {"amount_due": "100.00", "payments": [
            {"id": 1, "amount": 500, "state": "paid", "payment_method": "Credit Card", "credit_card_type": "Visa",
             "paid_by": "Jane Payer", "internal_note": "call first", "reference": "1234"},
            {"id": 2, "amount": 300, "state": "paid", "payment_method": "Check", "paid_at": "2026-01-01"},
            {"id": 3, "amount": 200, "state": "refunded", "payment_method": "ACH", "amount_refunded": 200},
            {"id": 4, "amount": 900, "state": "unpaid", "payment_method": None},
        ]}])
    assert out["paid_total"] == 800.0 and out["paid_not_card"] == 300.0
    assert out["refunded_total"] == 200.0 and out["amount_due"] == 100.0
    assert [p["card"] for p in out["payments"]] == [True, False, False, False]
    stored = json.dumps(out["payments"])
    for private in ("Jane Payer", "call first", "1234"):
        assert private not in stored
    assert main.ts_payment_summary(None)["payments"] is None  # no payment data: stays unknown, not $0


def test_tripleseat_contact_identity_prefers_contact_then_lead_fields():
    ident = main.ts_contact_identity(
        {"first_name": "Ann", "last_name": "Lee", "email_addresses": [{"address": "ann@example.test"}],
         "phone_numbers": [{"number": "248-555-0100"}],
         "addresses": [{"address1": "1 Main", "city": "Novi", "state": "MI", "zip_code": "48375"}]},
        {"name": "Acme"})
    assert (ident["email"], ident["phone"], ident["zip"], ident["ts_account"]) == \
        ("ann@example.test", "248-555-0100", "48375", "Acme")
    lead = main.ts_contact_identity(None, None, fallback={"first_name": "Bo", "email_address": "bo@example.test",
                                                          "phone_number": "2485550101", "company": "Beta"})
    assert (lead["first_name"], lead["email"], lead["ts_account"]) == ("Bo", "bo@example.test", "Beta")


def test_tripleseat_pages_follow_total_pages(monkeypatch):
    calls = []

    def fake_get(path, params=None):
        calls.append(params["page"])
        return {"total_pages": 3, "results": [{"id": params["page"]}]}
    monkeypatch.setattr(main, "tripleseat_get", fake_get)
    monkeypatch.setattr(main.time, "sleep", lambda s: None)
    assert [r["id"] for r in main.tripleseat_pages("/bookings/search.json", {})] == [1, 2, 3]
    assert calls == [1, 2, 3]


def test_tripleseat_sync_filters_by_window_and_never_counts_history(monkeypatch):
    import datetime as dt
    asked, ingested = [], []
    monkeypatch.setattr(main, "tripleseat_pages", lambda path, params: asked.append((path, params)) or iter(
        [{"id": 7, "created_at": "2020-05-01T00:00:00Z"}] if "bookings" in path else [{"id": 8}]))
    monkeypatch.setattr(main, "get_conn", lambda: _FakeTokenDB(None))
    monkeypatch.setattr(main, "ingest_tripleseat_booking",
                        lambda cur, b, count_it, apply=True: ingested.append(("b", count_it, apply)) or
                        {"status": "matched", "has_payments": False})
    monkeypatch.setattr(main, "ingest_tripleseat_lead",
                        lambda cur, l, apply=True: ingested.append(("l", None, apply)) or {"status": "created"})
    out = main.run_tripleseat_sync(days_back=3)
    since = (dt.date.today() - dt.timedelta(days=3)).strftime("%m/%d/%Y")
    assert asked[0][1]["booking_updated_start_date"] == since and asked[0][1]["show_financial"] == "true"
    assert asked[1][1]["created_after"] == since
    assert ingested == [("b", True, True), ("l", None, True)]
    assert out["booking_matched"] == 1 and out["lead_created"] == 1 and out["bookings_without_payment_data"] == 1
    asked.clear(); ingested.clear()
    out = main.run_tripleseat_sync(days_back=None, count_created_since=dt.date.today().isoformat(), apply=False)
    assert "booking_updated_start_date" not in asked[0][1] and "created_after" not in asked[1][1]
    assert ingested[0] == ("b", False, False) and out["status"] == "tripleseat_sync_dry_run"


def test_tripleseat_sync_endpoint_off_without_secret(monkeypatch):
    from fastapi.testclient import TestClient
    monkeypatch.setattr(main, "TRIPLESEAT_CLIENT_SECRET", None)
    r = TestClient(main.app).post("/cron/sync-tripleseat?key=test-cron-secret")
    assert r.status_code == 200 and r.json()["status"] == "error"


# Real Versium answers use display names (seen on the live API, Nov 18 2026).
V_CONTACT = {"First Name": "Ann", "Last Name": "Lee", "Postal Address": "12 Oak St", "City": "Novi",
             "State": "MI", "Zip": "48375", "Zip+4": "48375-1234", "Country": "US",
             "Phone": "(248) 555-0199", "Email Address": "ann.alt@example.test"}
V_DEMO = {"Estimated Net Worth": "$250,000 - $499,999", "Home Market Value": "$300,000 - $349,999",
          "Auto Make": "Ford", "Auto Model": "ESCAPE", "Presence of Children": "Yes", "Household Income": "$100K+",
          "Religion": "X", "Ethnic Group": "X", "Party Affiliation": "X", "Credit Rating": "X",
          "Donor Political": "X", "Pets": "", "Individual Level Match": "Yes"}


def test_versium_contact_parsing_uses_display_names():
    c = main.parse_versium_contact(V_CONTACT, email="ann@example.test", phone="2485550100")
    assert c["alt_phones"] == ["2485550199"] and c["alt_emails"] == ["ann.alt@example.test"]
    assert c["address"] == {"md_address1": "12 Oak St", "md_city": "Novi", "md_state": "MI", "zip5": "48375",
                            "md_plus4": "1234"}
    same = main.parse_versium_contact({"Phone": "2485550100", "Email": "ann@example.test"}, "ann@example.test",
                                      "(248) 555-0100")
    assert same["alt_phones"] == [] and same["alt_emails"] == [] and same["address"] is None


def test_versium_demographics_snake_case_blocked_fields_and_columns():
    d = main.parse_versium_demographics(V_DEMO)
    f = d["lifestyle_flags"]
    assert f["presence_of_children"] == "Yes" and f["household_income"] == "$100K+" and "pets" not in f
    for blocked in ("religion", "ethnic_group", "party_affiliation", "credit_rating", "donor_political"):
        assert blocked not in f
    assert d["versium_net_worth"] == "$250,000 - $499,999"
    assert d["versium_home_market_value"] == "$300,000 - $349,999"
    assert d["versium_auto_make_model"] == "Ford Escape"


def test_versium_call_rejections_are_logged_and_retryable(monkeypatch, caplog):
    from conftest import FakeResponse
    answers = [FakeResponse(400, {"versium": {"errors": ["The request does not contain a valid output value"]}}),
               FakeResponse(200, {"versium": {"errors": ["Out of credits"], "results": []}}),
               FakeResponse(200, {"versium": {"results": []}})]
    monkeypatch.setattr(main.requests, "get", lambda url, **kw: answers.pop(0))
    with caplog.at_level("ERROR", logger="pg_cdp"):
        assert main.versium_call("demographic", {}, {"email": "a@b.com"}) is None
        assert main.versium_call("contact", {}, {"email": "a@b.com"}) is None
    assert "demographic rejected (HTTP 400)" in caplog.text and "Out of credits" in caplog.text
    assert "a@b.com" not in caplog.text
    assert main.versium_call("contact", {}, {}) == {}  # answered, no match


def _versium_world(monkeypatch, contact=None, demo=None, fail=()):
    from conftest import FakeResponse
    seen = []

    def get(url, params=None, **kw):
        seen.append((url, params or {}))
        for name in fail:
            if f"/v2/{name}" in url:
                return FakeResponse(500, {})
        if "/v2/contact" in url:
            return FakeResponse(200, {"versium": {"results": [contact if contact is not None else V_CONTACT]}})
        if "/v2/demographic" in url:
            return FakeResponse(200, {"versium": {"results": [demo if demo is not None else V_DEMO]}})
        if "onlineAudience" in url:
            return FakeResponse(200, OA_BODY)
        if "geocoding.geo.census.gov" in url:
            return FakeResponse(200, {"result": {"addressMatches": [{"coordinates": {"x": -83.47, "y": 42.48},
                                      "geographies": {"Census Tracts": [{"TRACT": "123400"}]}}]}})
        raise AssertionError(url)
    monkeypatch.setattr(main, "VERSIUM_API_KEY", "test-versium-key")
    monkeypatch.setattr(main, "MELISSA_LIVE", False)
    monkeypatch.setattr(main.requests, "get", get)
    return seen


def test_versium_only_waterfall_finds_address_then_census(monkeypatch):
    seen = _versium_world(monkeypatch)
    r = main.run_melissa_and_versium_waterfall("Ann", "Lee", "ann@example.test", "2485550100")
    urls = [u for u, _ in seen]
    assert not any("melissadata" in u for u in urls)
    contact = next(p for u, p in seen if "/v2/contact" in u)
    assert "address" in contact["output[]"]
    demo = next(p for u, p in seen if "/v2/demographic" in u)
    assert demo["output[]"] == ["demographic", "financial", "lifestyle"] and demo["address"] == "12 Oak St"
    census = next(p for u, p in seen if "census.gov" in u)
    assert census["street"] == "12 Oak St"
    assert r["md_address1"] == "12 Oak St" and r["zip5"] == "48375" and r["md_census_tract"] == "123400"
    assert r["distance_miles"] is not None and r["versium_net_worth"]
    assert r["lifestyle_flags"]["_versium_checked"] and r["md_results"] == main.MELISSA_OFF
    assert any("onlineAudience" in u for u in urls)


def test_versium_skips_address_output_when_street_known_and_ignores_conflicting_zip(monkeypatch):
    seen = _versium_world(monkeypatch)
    main.run_melissa_and_versium_waterfall("Ann", "Lee", "ann@example.test", "", "9 Elm", "Novi", "MI", "48375")
    assert "address" not in next(p for u, p in seen if "/v2/contact" in u)["output[]"]
    _versium_world(monkeypatch, contact={**V_CONTACT, "Zip": "48009", "Zip+4": ""})
    r = main.run_melissa_and_versium_waterfall("Ann", "Lee", "ann@example.test", "", zip_code="48375")
    assert not r.get("md_address1") and r["zip5"] == "48375"


def test_versium_address_in_excluded_area_stops_further_enrichment(monkeypatch):
    seen = _versium_world(monkeypatch, contact={**V_CONTACT, "City": "Detroit", "Zip": "48226", "Zip+4": ""})
    r = main.run_melissa_and_versium_waterfall("Ann", "Lee", "ann@example.test", "")
    urls = [u for u, _ in seen]
    assert r["md_city"] == "Detroit" and not any("/v2/demographic" in u or "onlineAudience" in u for u in urls)


def test_failed_versium_call_leaves_customer_retryable(monkeypatch):
    _versium_world(monkeypatch, fail=("demographic",))
    r = main.run_melissa_and_versium_waterfall("Ann", "Lee", "ann@example.test", "")
    assert r["md_results"] is None and "_versium_checked" not in r["lifestyle_flags"]


def test_versium_checked_recognises_csv_and_live_markers():
    assert main.versium_checked({"versium_lifestyle_flags": {"_source": "versium_reach_csv"}})
    assert main.versium_checked({"versium_lifestyle_flags": {"_versium_checked": "2026-11-18"}})
    assert not main.versium_checked({"versium_lifestyle_flags": {}}) and not main.versium_checked({})


def test_customer_street_kept_when_melissa_off(monkeypatch):
    _versium_world(monkeypatch)
    r = main.run_melissa_and_versium_waterfall("Ann", "Lee", "ann@example.test", "", "9 Elm", "Novi", "MI", "48375")
    assert r["md_address1"] == "9 Elm" and r["md_census_tract"]


def test_checked_customer_not_bought_again_and_partial_success_kept(monkeypatch):
    seen = _versium_world(monkeypatch)
    main.run_melissa_and_versium_waterfall("Ann", "Lee", "ann@example.test", "",
                                           known={"versium_lifestyle_flags": {"_versium_checked": "2026-11-18"}})
    assert not any("/v2/contact" in u or "/v2/demographic" in u for u, _ in seen)
    seen = _versium_world(monkeypatch, fail=("demographic",))
    r = main.run_melissa_and_versium_waterfall("Ann", "Lee", "ann@example.test", "")
    f = r["lifestyle_flags"]
    assert f["_versium_contact_done"] and "_versium_demo_done" not in f and "_versium_checked" not in f
    seen = _versium_world(monkeypatch)
    r = main.run_melissa_and_versium_waterfall("Ann", "Lee", "ann@example.test", "",
                                               known={"versium_lifestyle_flags": f})
    assert not any("/v2/contact" in u for u, _ in seen) and any("/v2/demographic" in u for u, _ in seen)
    assert r["lifestyle_flags"]["_versium_checked"]


def test_conflicting_versium_address_rejected_and_consistent_one_taken_whole():
    a = {"md_address1": "1 Main", "md_city": "Toledo", "md_state": "OH", "zip5": "43604", "md_plus4": None}
    assert not main.versium_address_consistent(a, None, "MI", "Novi")
    assert not main.versium_address_consistent(a, None, None, "Novi")
    assert not main.versium_address_consistent(a, "48375", None, None)
    assert main.versium_address_consistent({**a, "md_city": "Novi", "md_state": "Michigan", "zip5": "48375"},
                                           None, "MI", "novi")
    assert main.versium_address_consistent(a, None, None, None)


def test_versium_address_conflicting_city_state_not_merged(monkeypatch):
    seen = _versium_world(monkeypatch, contact={**V_CONTACT, "City": "Toledo", "State": "OH", "Zip": "43604",
                                                "Zip+4": ""})
    r = main.run_melissa_and_versium_waterfall("Ann", "Lee", "ann@example.test", "", city="Novi", state="MI")
    assert not r.get("md_address1") and r["md_city"] == "Novi" and not r.get("zip5")


def test_enrichment_plan():
    plan = main.enrichment_plan
    assert plan(None, "a@b.com", "", {}) == "new"
    assert plan(None, "", "", {}) is None
    assert plan({"enrich_after": "soon"}, "a@b.com", "", {}) == "pending"
    nomatch = {"email": "a@b.com", "md_results": "MELISSA_OFF", "md_city": "Novi", "zip5": "48375",
               "versium_lifestyle_flags": {"_versium_checked": "2026-11-18"}}
    assert plan(nomatch, "a@b.com", "", {}) is None                       # nothing new
    assert plan(nomatch, "new@b.com", "", {}) == "retry"
    assert plan(nomatch, "a@b.com", "2485550100", {}) == "retry"
    assert plan({**nomatch, "versium_lifestyle_flags": {"_versium_checked": "x", "_versium_retried": "x"}},
                "new@b.com", "", {}) is None                              # only one retry
    matched = {**nomatch, "versium_lifestyle_flags": {"_versium_checked": "x", "pets": "Y"}}
    assert plan(matched, "new@b.com", "", {}) is None                      # matched: never bought twice
    assert plan({**nomatch, "md_city": "Detroit", "zip5": "48226"}, "new@b.com", "", {}) is None


def test_as_text_turns_tripleseat_objects_into_text():
    assert main.as_text({"id": 7, "name": "Went with another venue"}) == "Went with another venue"
    assert main.as_text([{"id": 1, "name": "A"}, "B", None]) == "A, B"
    assert main.as_text(123) == "123" and main.as_text("  x ") == "x"
    assert main.as_text(None) is None and main.as_text("") is None and main.as_text({}) is None
