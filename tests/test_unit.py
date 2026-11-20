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


# --- Personas and scoring (behaviour + best goal fit, no base, no ratchet) ----------------------

NOVI = {"md_city": "Novi", "zip5": "48375"}
BIRTHDAY_FIT = {**NOVI, "md_children_age_range": "3-5,6-10", "distance_miles": 4,
                "versium_lifestyle_flags": {"age_range": "35-45", "household_income": "$150,000-199,999",
                                            "length_of_residence": "5-10 Years", "gender": "Female",
                                            "marital_status": "Married", "household_size": "2"}}


def test_no_behaviour_and_no_fit_scores_zero():
    persona, score = main.score_and_assign_persona({**NOVI})
    assert score == 0 and persona.startswith("PG-5")


def test_birthday_propensity_scores_as_specified():
    # age 10 + kids 10 + affluence 5 + tenure 5 + female 5 + within 5mi 10 + married 5 + household of 2 5
    assert main.goal_fits(BIRTHDAY_FIT)["birthday"] == 55
    persona, score = main.score_and_assign_persona(BIRTHDAY_FIT)
    assert persona.startswith("PG-2") and score == 55
    young = {**BIRTHDAY_FIT, "versium_lifestyle_flags": {**BIRTHDAY_FIT["versium_lifestyle_flags"],
                                                         "young_adult_in_household": "Yes"}}
    assert main.goal_fits(young)["birthday"] == 45
    assert main.winning_goal(BIRTHDAY_FIT) == "birthday"


def test_corporate_propensity_scores_as_specified():
    cust = {**NOVI, "first_name": "Pat", "last_name": "Lane", "ts_account": "Comerica Bank", "distance_miles": 14,
            "versium_lifestyle_flags": {"age_range": "55-65", "gender": "Female", "marital_status": "Married"}}
    # org 15 + age 35-65 10 + female 5 + within 15mi 2 + married 5; no income/home points
    assert main.goal_fits(cust)["corporate"] == 37
    persona, score = main.score_and_assign_persona(cust)
    assert persona.startswith("PG-1") and score == 37 and main.winning_goal(cust) == "corporate"


def test_celebration_propensity_scores_as_specified():
    cust = {**NOVI, "distance_miles": 9, "versium_home_market_value": "$500,000-749,999",
            "versium_lifestyle_flags": {"age_range": "45-55", "length_of_residence": "11-14 Years",
                                        "household_size": "2", "marital_status": "Married"}}
    # within 12mi 5 + age 5 + affluence (home) 5 + tenure 5 + household 2 5 + married 5
    assert main.goal_fits(cust)["celebration"] == 30
    # The same household is a stronger birthday fit (age counts 10 there), so the score is the better of the two
    assert main.goal_fits(cust)["birthday"] == 35 and main.score_and_assign_persona(cust)[1] == 35
    # With a young adult at home birthday drops 10 and celebration wins
    ya = {**cust, "versium_lifestyle_flags": {**cust["versium_lifestyle_flags"], "young_adult_in_household": "Yes"}}
    assert main.score_and_assign_persona(ya)[1] == 30 and main.winning_goal(ya) == "celebration"


def test_behaviour_points_and_dominance():
    booker = {**NOVI, "pw_bookings": 1, "true_lifetime_spend": 650}
    # booker 25 + $600+ 20 + recent 10 + 3+ separate visits 5
    assert main.behaviour_points(booker, {"recent": True, "visit_days": 3}) == 60
    assert main.behaviour_points({"true_lifetime_spend": 300}, {}) == 15
    assert main.score_and_assign_persona(booker, {"recent": True, "visit_days": 3})[1] > \
        main.score_and_assign_persona(BIRTHDAY_FIT)[1]


def test_income_bands_parse_by_lower_bound():
    # The old text match counted "$75,001-$100,000" as high income; affluence needs $150k+
    for band, rich in (("$75,001-$100,000", False), ("$125,001-$150,000", False), ("$150,001+", True)):
        assert main._fit_facts({"md_household_income": band})["affluent"] is rich, band
    assert main._fit_facts({"versium_lifestyle_flags": {"household_income": "> $250,000"}})["affluent"]
    assert not main._fit_facts({"versium_lifestyle_flags": {"household_income": "$100,000-149,999"},
                                "md_household_income": "$150,001+"})["affluent"]  # Versium first
    assert main._fit_facts({"versium_home_market_value": "450000"})["affluent"]


def test_large_family_party_without_an_organisation_is_not_corporate():
    fam = {**NOVI, "first_name": "Dana", "last_name": "Wood", "ts_account": "Wood Family", "ts_max_guests": 40,
           "ts_lead_status": "PROSPECT"}
    assert not main.score_and_assign_persona(fam)[0].startswith("PG-1")
    assert main.is_real_organisation({"first_name": "Dana", "last_name": "Noonan",
                                      "ts_account": "Noonan Electrical Services"})
    assert main.is_real_organisation({"first_name": "Dana", "last_name": "Wood", "md_delivery_indicator": "B"})
    assert not main.is_real_organisation({"first_name": "Dana", "last_name": "Wood", "ts_account": "Dana Wood"})


def test_excluded_geo_persona():
    persona, _ = main.score_and_assign_persona({"md_city": "Detroit", "zip5": "48226", "true_lifetime_spend": 50})
    assert persona.startswith("PG-4")


def test_audience_targets():
    t, ltv = main.audience_targets({"pg_mosaic_persona": "PG-2: x", "zip5": "48335", "md_city": "Farmington Hills",
                                    "in_partywirks": 1, "pw_bookings": 1, "true_lifetime_spend": 100})
    assert t == [main.AUDIENCE_ALL_NONDETROIT, main.AUDIENCE_PG2_FAMILIES, main.AUDIENCE_PRIME_SUBURBS]
    assert main.audience_targets({"md_city": "Detroit", "zip5": "48226", "pg_mosaic_persona": "PG-1"}) == ([], 0.0)
    assert main.audience_targets({"zip5": "48230", "md_city": "Grosse Pointe"})[0] == [main.AUDIENCE_ALL_NONDETROIT]


def test_lookalike_value_is_real_lifetime_spend_with_no_floor():
    for cust in ({"in_tripleseat": 1, "true_lifetime_spend": 100}, {"in_partywirks": 1, "pw_bookings": 1},
                 {"in_tripleseat": 1, "ts_lead_status": "DEFINITE", "true_lifetime_spend": 1234.5}):
        assert main.audience_targets({**NOVI, **cust})[1] == float(cust.get("true_lifetime_spend") or 0)


def test_seeds_follow_who_the_customer_is_not_score_or_distance():
    # A Tripleseat prospect is no longer in the corporate seed just for being in Tripleseat...
    t, _ = main.audience_targets({**NOVI, "in_tripleseat": 1, "ts_lead_status": "PROSPECT",
                                  "first_name": "A", "last_name": "B", "ts_account": "A B", "true_lifetime_spend": 700})
    assert main.AUDIENCE_PG1_CORP not in t
    # ...nor a cancelled-only Partywirks contact in the families seed
    assert main.AUDIENCE_PG2_FAMILIES not in main.audience_targets({**NOVI, "in_partywirks": 1})[0]
    # A party-age family with no distance and a score of 0 is in the birthday seed
    fam = {**BIRTHDAY_FIT, "distance_miles": None, "bowler_value_score": 0}
    assert main.AUDIENCE_PG2_FAMILIES in main.audience_targets(fam)[0]
    org = {**NOVI, "first_name": "A", "last_name": "B", "ts_account": "Ford Motor Company", "bowler_value_score": 0}
    assert main.AUDIENCE_PG1_CORP in main.audience_targets(org)[0]


def test_rejected_audience_upload_is_logged(fake, caplog):
    fake.meta_mode = "500"
    with caplog.at_level("ERROR", logger="pg_cdp"):
        res = main.upload_audience_rows(main.AUDIENCE_PG1_CORP, [["x"] * 9])
    assert res["ok"] is False
    assert "Corporate rejected (HTTP 500)" in caplog.text and "PG-1" not in caplog.text


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


def test_temporary_tripleseat_field_logging_is_gone():
    assert not hasattr(main, "log_tripleseat_fields") and not hasattr(main, "TRIPLESEAT_LOG_FIELDS")
    assert not hasattr(main, "field_paths") and not hasattr(main, "_TS_MONEY_HINTS")


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


# --- Broad Local replaces the PG-3 Celebrations audience ------------------------------------------

BROAD_ID = "777000222"
AFFLUENT_35_55 = {"versium_lifestyle_flags": {"age_range": "35-45", "household_income": "$150,000 - $174,999"}}


def _old_celebration_rule(cust, facts):
    """The pre-fold celebration seed, kept here to prove Broad Local contains all of it."""
    return (float(cust.get("true_lifetime_spend") or 0) >= main.TOP_QUARTER_SPEND
            or int((facts or {}).get("visit_days") or 0) >= 3
            or (cust.get("pg_mosaic_persona") or "").startswith("PG-3"))


def test_broad_local_membership(monkeypatch):
    monkeypatch.setattr(main, "AUDIENCE_BROAD_LOCAL", BROAD_ID)
    def inn(cust, facts=None):
        return BROAD_ID in main.audience_targets({**NOVI, **cust}, facts)[0]
    assert inn({"true_lifetime_spend": main.TOP_QUARTER_SPEND})                     # top-quarter spender
    assert inn({"true_lifetime_spend": 40}, {"visit_days": 3})                      # frequent low-spend visitor
    assert inn({**AFFLUENT_35_55, "md_children_age_range": "3-5", "true_lifetime_spend": 20})  # affluent 35-55 family
    assert not inn({"true_lifetime_spend": 278.99}, {"visit_days": 2})              # none of the three
    assert not inn({"versium_lifestyle_flags": {"age_range": "35-45"}, "true_lifetime_spend": 20})  # age, not affluent
    assert main.audience_targets({**AFFLUENT_35_55, "md_city": "Detroit", "zip5": "48226",
                                  "true_lifetime_spend": 900}, {"visit_days": 9}) == ([], 0.0)  # excluded area


def test_fold_loses_nobody_from_the_old_celebration_seed():
    personas = ["", "PG-3: High-Value Open Bowlers / Celebrations", "PG-2: Birthday Party Families"]
    demos = [{}, AFFLUENT_35_55, {"versium_lifestyle_flags": {"age_range": "55-65"}, "md_household_income": "$200,000+"}]
    checked = 0
    for spend in (0, 100, 278.99, 279, 5000):
        for days in (0, 2, 3, 10):
            for demo in demos:
                for persona in personas:
                    cust = {**NOVI, **demo, "true_lifetime_spend": spend, "pg_mosaic_persona": persona}
                    facts = {"visit_days": days}
                    # Old PG-3 personas imply spend >= $279 or 3+ visit days, so only real PG-3s count here.
                    real_pg3 = spend >= main.TOP_QUARTER_SPEND or days >= 3
                    if _old_celebration_rule(cust, facts) and (real_pg3 or not persona.startswith("PG-3")):
                        assert main.seed_memberships(cust, facts)["broad_local"], cust
                        checked += 1
    assert checked > 50


def test_no_audience_targets_pg3():
    assert not hasattr(main, "AUDIENCE_PG3_CELEBRATIONS")
    assert "celebration" not in main.seed_memberships(NOVI)
    assert not any("PG-3" in name or "Celebration" in name for name in main.audience_names().values())
    import pathlib
    src = pathlib.Path(main.__file__).read_text()
    assert "AUDIENCE_PG3" not in src


def test_stored_personas_never_affect_seed_membership():
    labels = ["", "PG-1: Corporate & Group Organisers", "PG-2: Birthday Party Families",
              "PG-3: High-Value Open Bowlers / Celebrations", "PG-4: Excluded Geo", "PG-5: Not Enough Data Yet",
              "PG-3: Celebrations (old)", "Prime Suburban Family", "PG-2 Affluent Families"]
    custs = [{**NOVI, "true_lifetime_spend": 10}, {**NOVI, **AFFLUENT_35_55}, {**NOVI, "pw_bookings": 1},
             {**NOVI, "first_name": "A", "last_name": "B", "ts_account": "Ford Motor Company"}]
    for cust in custs:
        base = main.seed_memberships(cust, {"visit_days": 1})
        for label in labels:
            assert main.seed_memberships({**cust, "pg_mosaic_persona": label}, {"visit_days": 1}) == base, label


# --- Audience uploads carry every known identifier ---------------------------------------------

H = lambda v: main.sha256_hash(v)
FB_E = ["a" * 64, "b" * 64]
FB_P = ["c" * 64]


def test_audience_rows_add_versium_identifiers_as_extra_rows():
    cust = {"email": "Own@Example.test", "phone": "(248) 555-0101", "first_name": "Ann", "last_name": "Lee",
            "md_city": "Novi", "md_state": "MI", "zip5": "48375",
            "versium_alt_emails": ["alt@example.test", "own@example.test"],   # the own email isn't repeated
            "versium_alt_phones": ["248-555-0199"],
            "versium_online_audience": {"facebook": {"email": FB_E + ["NOT-A-HASH"], "phone": FB_P + FB_P}}}
    rows = main.audience_rows(cust, 432.1)
    assert all(len(r) == len(main.AUDIENCE_SCHEMA) for r in rows)
    assert rows[0][:2] == [H("own@example.test"), H(main.meta_phone("2485550101"))]   # own details first
    extra_emails = [r[0] for r in rows[1:] if r[0]]
    extra_phones = [r[1] for r in rows[1:] if r[1]]
    assert extra_emails == [H("alt@example.test")] + FB_E                    # Facebook hashes sent as they are
    assert extra_phones == [H(main.meta_phone("248-555-0199"))] + FB_P       # no duplicates
    assert len(rows) == 1 + 3
    assert all(r[2:] == rows[0][2:] for r in rows)                           # same name/place and LOOKALIKE_VALUE
    assert rows[0][-1] == 432.1


def test_audience_rows_without_extras_is_one_row():
    rows = main.audience_rows({"email": "solo@example.test", "zip5": "48375"}, 0)
    assert len(rows) == 1 and rows[0][0] == H("solo@example.test") and rows[0][1] == ""


def test_audience_rows_cap_extra_identifiers():
    many = {"email": "x@example.test",
            "versium_online_audience": {"facebook": {"email": [f"{i:064x}" for i in range(30)]}}}
    assert len(main.audience_rows(many, 0)) == 1 + main.AUDIENCE_EXTRA_ROWS_MAX


def test_audience_labels_are_the_new_names():
    names = main.audience_names()
    assert names[main.AUDIENCE_PG1_CORP] == "Corporate" and names[main.AUDIENCE_PG2_FAMILIES] == "Birthday"
    assert names[main.AUDIENCE_BROAD_LOCAL] == "Broad Local"
    assert not any("PG-" in n for n in names.values())


# --- Fix #8: event time = transaction time; browser identifiers ---------------------------------

from datetime import datetime as _dt, timedelta as _td, timezone as _tz


def test_parse_source_time_reads_each_source_format():
    utc = _tz.utc
    assert main.parse_source_time("2026-11-19T15:04:05.123Z") == _dt(2026, 11, 19, 15, 4, 5, 123000, tzinfo=utc)
    assert main.parse_source_time("2026-11-19T10:04:05-05:00") == _dt(2026, 11, 19, 15, 4, 5, tzinfo=utc)
    # Partywirks: no offset, venue local time (EST in November = UTC-5)
    assert main.parse_source_time("2026-11-19 10:04:05", naive_tz=main.VENUE_TZ) == _dt(2026, 11, 19, 15, 4, 5, tzinfo=utc)
    assert main.parse_source_time("2026-12-05") is None          # a plain date is not a transaction time
    assert main.parse_source_time("12/05/2026") is None
    assert main.parse_source_time(None) is None and main.parse_source_time("junk") is None


def test_meta_event_time_never_future_never_too_old():
    now = _dt(2026, 11, 20, 12, 0, tzinfo=_tz.utc)
    two_hours_ago = now - _td(hours=2)
    assert main.meta_event_time(two_hours_ago, now) == two_hours_ago
    assert main.meta_event_time(now + _td(days=10), now) == now           # a future party date never
    assert main.meta_event_time(None, now) == now
    assert main.meta_event_time(now - _td(days=30), now) == now           # older than Meta accepts
    assert main.meta_event_time(now - _td(days=6), now) == now - _td(days=6)


FBP = "fb.1.1763600000000.1234567890"
FBC = "fb.1.1763600000000.IwAR2abcdefghijklmnop"


def test_tripleseat_custom_fields_give_browser_ids_only_in_meta_format():
    obj = {"custom_fields": [
        {"custom_field_name": "fbp", "value": FBP},
        {"custom_field_slug": "fbc", "value": FBC},
        {"custom_field_name": "User Agent", "value": "Mozilla/5.0 (iPhone)"},
        {"custom_field_name": "Theme", "value": "Pirates"}]}
    assert main.ts_browser_ids(obj) == {"fbp": FBP, "fbc": FBC, "ua": "Mozilla/5.0 (iPhone)"}
    bad = {"custom_fields": [{"custom_field_name": "fbp", "value": "not-a-cookie"},
                             {"custom_field_name": "fbc", "value": ""}]}
    assert main.ts_browser_ids(bad) == {}
    assert main.ts_browser_ids({}) == {} and main.ts_browser_ids({"custom_fields": None}) == {}


def test_browser_ids_only_on_tripleseat_events_and_only_while_fresh():
    recent = _dt.now(_tz.utc) - _td(days=3)
    fresh = {"meta_fbp": FBP, "meta_fbp_at": recent, "meta_fbc": None, "meta_fbc_at": None,
             "meta_client_user_agent": "Mozilla/5.0 (iPhone)", "meta_client_user_agent_at": recent}
    assert main.browser_user_data(fresh, "qb_ts_1_2026-12-05") == {"fbp": FBP, "client_user_agent": "Mozilla/5.0 (iPhone)"}
    assert main.browser_user_data(fresh, "lead_ts_1_2026-12-05")["fbp"] == FBP
    for eid in ("sq_pay_X", "wr_W1", "qb_wr_W1", "qb_pw_123"):
        assert main.browser_user_data(fresh, eid) == {}                    # Square / WebRes / Partywirks: none
    old = _dt.now(_tz.utc) - _td(days=91)
    stale = {**fresh, "meta_fbp_at": old, "meta_client_user_agent_at": old}
    assert main.browser_user_data(stale, "qb_ts_1_2026-12-05") == {}
    assert main.browser_user_data({"meta_fbp": FBP}, "qb_ts_1_2026-12-05") == {}   # no capture time: unknown
    # Each value ages by its own capture time: a fresh fbp with a stale fbc sends only the fbp.
    mixed = {"meta_fbp": FBP, "meta_fbp_at": recent, "meta_fbc": FBC, "meta_fbc_at": old}
    assert main.browser_user_data(mixed, "qb_ts_1_2026-12-05") == {"fbp": FBP}


def test_tripleseat_booking_conversion_time_is_its_status_change():
    obj = {"created_at": "2026-10-01T12:00:00Z", "status_changes": [
        {"status": "TENTATIVE", "created_at": "2026-11-01T12:00:00Z"},
        {"status": "DEFINITE", "created_at": "2026-11-19T18:30:00Z"}]}
    assert main.ts_conversion_time("booking", obj, "DEFINITE") == _dt(2026, 11, 19, 18, 30, tzinfo=_tz.utc)
    assert main.ts_conversion_time("event", {"created_at": "2026-11-19T18:30:00Z", "event_date": "12/05/2026"},
                                   "DEFINITE") == _dt(2026, 11, 19, 18, 30, tzinfo=_tz.utc)


# --- Fix #9: provider retries, diagnosable failure logs ---------------------------------------

import requests as _requests


class _Resp:
    def __init__(self, status, payload=None, text=None, headers=None):
        self.status_code, self._payload = status, payload
        self.text = text if text is not None else str(payload)
        self.headers = headers or {}

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


def _sequence(monkeypatch, answers):
    """requests.get returns (or raises) each answer in turn; returns the list of calls made."""
    calls, seq = [], list(answers)
    def fake_get(url, **kw):
        calls.append(url)
        a = seq.pop(0)
        if isinstance(a, Exception):
            raise a
        return a
    monkeypatch.setattr(main.requests, "get", fake_get)
    return calls


V_OK = {"versium": {"results": [{"Phone": "2485550101"}]}}


def test_transient_versium_failures_are_retried_until_it_answers(monkeypatch, caplog):
    calls = _sequence(monkeypatch, [_requests.exceptions.ReadTimeout("slow"),
                                    _Resp(500, {"versium": {"errors": ["internal"]}}), _Resp(200, V_OK)])
    waits = []
    monkeypatch.setattr(main, "_provider_sleep", waits.append)
    out = {}
    with caplog.at_level("INFO", logger="pg_cdp"):
        res = main.versium_call("contact", {}, {"email": "a@example.test"}, outcome=out)
    assert res == {"Phone": "2485550101"} and len(calls) == 3
    assert waits == list(main.PROVIDER_BACKOFF_SECONDS)                      # short backoff between attempts
    assert out["attempts"] == 3 and out["status"] == 200
    msgs = [r.getMessage() for r in caplog.records]
    assert any("Versium contact failed (attempt 1 of 3): ReadTimeout" in m for m in msgs)
    assert any("attempt 2 of 3): HTTP 500" in m and "internal" in m for m in msgs)   # status + body
    assert any("answered (HTTP 200) on attempt 3 of 3" in m for m in msgs)


def test_429_waits_for_retry_after(monkeypatch):
    _sequence(monkeypatch, [_Resp(429, {"errors": ["rate"]}, headers={"Retry-After": "2"}), _Resp(200, V_OK)])
    waits = []
    monkeypatch.setattr(main, "_provider_sleep", waits.append)
    assert main.versium_call("contact", {}, {}) == {"Phone": "2485550101"}
    assert waits == [2.0]


def test_permanent_versium_error_is_not_retried_and_logs_status_and_body(monkeypatch, caplog):
    calls = _sequence(monkeypatch, [_Resp(400, {"versium": {"errors": ["Invalid output value"]}})])
    out = {}
    with caplog.at_level("ERROR", logger="pg_cdp"):
        assert main.versium_call("demographic", {}, {}, outcome=out) is None
    assert len(calls) == 1 and out["status"] == 400
    rec = [r for r in caplog.records if "Versium demographic rejected" in r.getMessage()][0]
    assert "HTTP 400" in rec.getMessage() and "Invalid output value" in rec.getMessage()


def test_versium_giving_up_after_every_attempt_times_out(monkeypatch, caplog):
    calls = _sequence(monkeypatch, [_requests.exceptions.ReadTimeout()] * 3)
    with caplog.at_level("WARNING", logger="pg_cdp"):
        assert main.versium_call("contact", {}, {}) is None
    assert len(calls) == 3 and "giving up" in caplog.records[-1].getMessage()


def test_melissa_rejection_is_not_retried_and_5xx_is(monkeypatch, caplog):
    monkeypatch.setattr(main, "MELISSA_API_KEY", "k")
    monkeypatch.setattr(main, "MELISSA_ACT", "Check,Verify,Append")
    calls = _sequence(monkeypatch, [_Resp(200, {"TransmissionResults": "GE05", "Records": [{}]})])
    out = {}
    with caplog.at_level("ERROR", logger="pg_cdp"):
        assert main.melissa_lookup("A", "B", "a@example.test", "", outcome=out) == {}
    assert len(calls) == 1 and out["result"] == "rejected:GE05"
    assert any("TransmissionResults=GE05" in r.getMessage() for r in caplog.records)
    calls = _sequence(monkeypatch, [_Resp(503, None, text="Service Unavailable"),
                                    _Resp(200, {"Records": [{"Results": "AS01", "City": "Novi"}]})])
    out = {}
    assert main.melissa_lookup("A", "B", "a@example.test", "", "1 Main St", "Novi", "MI", "48375",
                               outcome=out)["md_results"] == "AS01"
    assert len(calls) == 2 and out["result"] == "answered"


def test_melissa_http_error_logs_status_and_body(monkeypatch, caplog):
    monkeypatch.setattr(main, "MELISSA_API_KEY", "k")
    monkeypatch.setattr(main, "MELISSA_ACT", "Check,Verify,Append")
    _sequence(monkeypatch, [_Resp(403, None, text="Forbidden: license expired")])
    with caplog.at_level("ERROR", logger="pg_cdp"):
        assert main.melissa_lookup("A", "B", "a@example.test", "") == {}
    msg = [r.getMessage() for r in caplog.records if "Melissa Personator failed" in r.getMessage()][0]
    assert "HTTP 403" in msg and "license expired" in msg


def test_melissa_without_any_input_spends_nothing(monkeypatch):
    monkeypatch.setattr(main, "MELISSA_API_KEY", "k")
    monkeypatch.setattr(main, "MELISSA_ACT", "Check,Verify,Append")
    calls = _sequence(monkeypatch, [])
    out = {}
    assert main.melissa_lookup("A", "B", "", "", outcome=out) == {} and calls == [] and out["result"] == "skipped_no_inputs"


def test_logged_bodies_carry_no_contact_details():
    red = main._redact("bad email jane.doe@example.com phone (248) 555-0101 " + "x" * 400)
    assert "jane.doe" not in red and "555-0101" not in red and len(red) <= 300


def test_missing_versium_key_is_a_failure_when_versium_is_required(monkeypatch):
    monkeypatch.setattr(main, "VERSIUM_API_KEY", None)
    monkeypatch.setattr(main, "VERSIUM_REQUIRED", True)
    res = main.run_versium_enrichment({}, "A", "B", "a@example.test", "", "", "Novi", "MI", "48375", {}, 0)
    assert res["_versium_failed"] and res["_outcome"]["versium"] == "failed:not_configured"
    monkeypatch.setattr(main, "VERSIUM_REQUIRED", False)
    assert not main.run_versium_enrichment({}, "A", "B", "a@example.test", "", "", "Novi", "MI", "48375", {}, 0).get("_versium_failed")


def test_enrichment_complete_needs_a_versium_answer(monkeypatch):
    monkeypatch.setattr(main, "VERSIUM_REQUIRED", True)
    assert not main.enrichment_complete({"md_city": "Novi", "zip5": "48375", "email": "a@example.test",
                                         "versium_lifestyle_flags": {}})
    assert not main.enrichment_complete({"phone": "2485550101",
                                         "versium_lifestyle_flags": {"_versium_contact_done": "2026-11-20"}})
    assert main.enrichment_complete({"versium_lifestyle_flags": {"_versium_checked": "2026-11-20"}})
    assert main.enrichment_complete({"versium_lifestyle_flags": {"_source": "csv"}})
    assert main.enrichment_complete({"md_city": "Detroit", "zip5": "48201"})      # Versium never runs there


def test_customer_with_nothing_to_look_up_counts_as_enriched(monkeypatch):
    monkeypatch.setattr(main, "VERSIUM_REQUIRED", True)
    assert main.enrichment_complete({"first_name": "Al", "zip5": "48375"})          # final: never re-queued
    assert not main.enrichment_complete({"first_name": "Al", "email": "al@example.test", "zip5": "48375"})


def test_missing_key_keeps_a_stored_versium_answer(monkeypatch):
    monkeypatch.setattr(main, "VERSIUM_API_KEY", None)
    monkeypatch.setattr(main, "VERSIUM_REQUIRED", True)
    for known in ({"versium_lifestyle_flags": {"_versium_checked": "2026-11-18"}},
                  {"versium_lifestyle_flags": {"_source": "csv"}}):
        res = main.run_versium_enrichment({}, "A", "B", "a@example.test", "", "", "Novi", "MI", "48375", known, 0)
        assert not res.get("_versium_failed") and res["_outcome"]["versium"] == "already"


def test_audit_judges_completion_at_the_drop_not_now():
    import importlib.util, os as _os
    from datetime import datetime as _d, timezone as _z
    path = _os.path.join(_os.path.dirname(__file__), "..", "scripts", "reevaluate_dropped_events.py")
    spec = importlib.util.spec_from_file_location("reevaluate_dropped_events", path)
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    dropped = _d(2026, 11, 20, 1, 21, tzinfo=_z.utc)
    base = {"dropped_at": dropped, "email": "a@example.test"}
    assert mod.complete_at_drop({**base, "versium_lifestyle_flags": {"_versium_checked": "2026-11-18"}})
    assert mod.complete_at_drop({**base, "versium_lifestyle_flags": {"_versium_checked": "2026-11-20"}})  # same day
    assert not mod.complete_at_drop({**base, "versium_lifestyle_flags": {"_versium_checked": "2026-11-21"}})  # later
    assert mod.complete_at_drop({**base, "versium_lifestyle_flags": {"_source": "csv"}})


# --- Fix #10: Tripleseat signature, shared-key rotation, no key in access logs ---------------------
# All secrets below are obviously fake. The signature fixtures are synthetic: no real Tripleseat delivery was
# captured (that would need its signing secret and a customer's payload); the construction is confirmed from the
# check-only logs of real deliveries, then pinned with TRIPLESEAT_SIGNATURE_SCHEME.

import hmac as _hmac, hashlib as _hashlib, base64 as _b64, logging as _logging

FAKE_TS_SECRET = "fake-tripleseat-signing-secret"


def _ts_sig(body: bytes, t: str, scheme="t.body", enc="hex", secret=FAKE_TS_SECRET) -> str:
    msg = {"t.body": t.encode() + b"." + body, "tbody": t.encode() + body, "body": body}[scheme]
    d = _hmac.new(secret.encode(), msg, _hashlib.sha256).digest()
    return d.hex() if enc == "hex" else _b64.b64encode(d).decode()


def test_tripleseat_signature_header_is_parsed():
    assert main.parse_tripleseat_signature("t=1763600000,v1=abc") == ("1763600000", ["abc"])
    assert main.parse_tripleseat_signature(" t=1, v1=a ,v1=b") == ("1", ["a", "b"])
    assert main.parse_tripleseat_signature("garbage") == (None, [])


def test_each_candidate_construction_is_recognised_by_name():
    body, t = b'{"lead": {"id": 1}}', "1763600000"
    for scheme in ("t.body", "tbody", "body"):              # the six PR #35 checked, names and behaviour unchanged
        for enc in ("hex", "base64"):
            header = f"t={t},v1={_ts_sig(body, t, scheme, enc)}"
            assert main.tripleseat_signature_scheme(body, header, FAKE_TS_SECRET) == f"{scheme}:{enc}"
            assert main.tripleseat_signature_scheme(body, header, FAKE_TS_SECRET, only="t.body:hex") == \
                (f"{scheme}:{enc}" if (scheme, enc) == ("t.body", "hex") else None)
    reformatted = b'{"lead":{"id":1}}'                     # a re-serialised body never verifies
    assert main.tripleseat_signature_scheme(reformatted, f"t={t},v1={_ts_sig(body, t)}", FAKE_TS_SECRET) is None
    assert main.tripleseat_signature_scheme(body, f"t={t},v1={_ts_sig(body, t, secret='other')}", FAKE_TS_SECRET) is None


# --- Tripleseat signature format determination: the widened candidate set (check-only) ----------------------
# Fixtures are synthetic: fake secrets, fake bodies, a fake URL. The expected signed strings are written out here
# independently of main's builders, so a builder that drifts from its name fails.

_TS_URL = "https://cdp.example.test/webhooks/tripleseat?key=fake-shared-key"
_TS_BODY = b'{"lead": {"id": 7, "note": "<a&b>"}}\n'


def _ts_expected_message(payload: str, t: str, body: bytes, url: str) -> bytes:
    T = t.encode()
    noq = url.split("?")[0]
    path = "/webhooks/tripleseat"
    compact = b'{"lead":{"id":7,"note":"<a&b>"}}'
    rails = b'{"lead":{"id":7,"note":"\\u003ca\\u0026b\\u003e"}}'
    bodyhash = _hashlib.sha256(body).hexdigest().encode()
    return {
        "t.body": T + b"." + body, "tbody": T + body, "body": body,
        "t:body": T + b":" + body, "t-nl-body": T + b"\n" + body, "t-sp-body": T + b" " + body,
        "body.t": body + b"." + T,
        "t.body-rstrip": T + b"." + body.rstrip(b"\n"), "t.body+nl": T + b"." + body + b"\n",
        "body-rstrip": body.rstrip(b"\n"),
        "t.url.body": T + b"." + url.encode() + b"." + body,
        "t.urlnoq.body": T + b"." + noq.encode() + b"." + body,
        "t.path.body": T + b"." + path.encode() + b"." + body,
        "t.pathq.body": T + b"." + (path + "?key=fake-shared-key").encode() + b"." + body,
        "url.body": url.encode() + body, "urlnoq.body": noq.encode() + body,
        "t.json": T + b"." + compact, "t.json-rails": T + b"." + rails,
        "t.bodyhash": T + b"." + bodyhash, "bodyhash": bodyhash,
    }[payload]


def _ts_encode_ref(digest: bytes, enc: str) -> str:
    return {"hex": digest.hex(), "base64": _b64.b64encode(digest).decode(),
            "base64url": _b64.urlsafe_b64encode(digest).decode().rstrip("=")}[enc]


def test_every_new_candidate_verifies_a_fixture_built_that_way():
    t = "1763600000"
    assert set(main.TRIPLESEAT_SIGNATURE_PAYLOADS) == {
        "t.body", "tbody", "body", "t:body", "t-nl-body", "t-sp-body", "body.t", "t.body-rstrip", "t.body+nl",
        "body-rstrip", "t.url.body", "t.urlnoq.body", "t.path.body", "t.pathq.body", "url.body", "urlnoq.body",
        "t.json", "t.json-rails", "t.bodyhash", "bodyhash"}
    for payload in main.TRIPLESEAT_SIGNATURE_PAYLOADS:
        for enc in main.TRIPLESEAT_SIGNATURE_ENCODINGS:
            d = _hmac.new(FAKE_TS_SECRET.encode(), _ts_expected_message(payload, t, _TS_BODY, _TS_URL),
                          _hashlib.sha256).digest()
            header = f"t={t},v1={_ts_encode_ref(d, enc)}"
            got = main.tripleseat_signature_scheme(_TS_BODY, header, FAKE_TS_SECRET, url=_TS_URL)
            assert got == f"{payload}:{enc}", (payload, enc, got)
            assert main.tripleseat_signature_scheme(_TS_BODY, header, FAKE_TS_SECRET, url=_TS_URL,
                                                    only=f"{payload}:{enc}") == f"{payload}:{enc}"
            assert main.tripleseat_signature_scheme(_TS_BODY, header, "fake-other-secret", url=_TS_URL) is None


def test_url_candidates_need_the_url_and_padded_base64url_also_matches():
    t = "1763600000"
    d = _hmac.new(FAKE_TS_SECRET.encode(), _ts_expected_message("t.url.body", t, _TS_BODY, _TS_URL),
                  _hashlib.sha256).digest()
    assert main.tripleseat_signature_scheme(_TS_BODY, f"t={t},v1={d.hex()}", FAKE_TS_SECRET) is None  # no url
    padded = _b64.urlsafe_b64encode(d).decode()
    assert padded.endswith("=")
    assert main.tripleseat_signature_scheme(_TS_BODY, f"t={t},v1={padded}", FAKE_TS_SECRET, url=_TS_URL) == \
        "t.url.body:base64url"


def test_decoded_secret_key_forms_verify():
    t, msg = "1763600000", _ts_expected_message("t.body", "1763600000", _TS_BODY, _TS_URL)
    raw = bytes(range(32))
    cases = {"b64key": _b64.b64encode(raw).decode(), "hexkey": raw.hex(),
             "b64urlkey": _b64.urlsafe_b64encode(bytes([251] * 32)).decode().rstrip("="),
             "whsec": "whsec_" + _b64.b64encode(raw).decode()}
    for form, secret in cases.items():
        key = bytes([251] * 32) if form == "b64urlkey" else raw
        d = _hmac.new(key, msg, _hashlib.sha256).digest()
        got = main.tripleseat_signature_scheme(_TS_BODY, f"t={t},v1={d.hex()}", secret)
        assert got == f"t.body:hex@{form}", (form, got)
    # the raw-text key is still tried first, under the old name
    d = _hmac.new(cases["hexkey"].encode(), msg, _hashlib.sha256).digest()
    assert main.tripleseat_signature_scheme(_TS_BODY, f"t={t},v1={d.hex()}", cases["hexkey"]) == "t.body:hex"


def test_only_timestamp_signed_candidates_can_be_pinned():
    for name in ("t.body:hex", "tbody:base64", "t:body:hex", "body.t:base64url", "t.url.body:hex@b64key",
                 "t.json-rails:base64", "t.bodyhash:hex"):
        assert main.tripleseat_scheme_signs_timestamp(name), name
    for name in ("body:hex", "body-rstrip:base64", "url.body:base64", "urlnoq.body:hex", "bodyhash:hex@hexkey",
                 "", "t.body", "t.body:sha1", "nonsense:hex", "t.body:hex@unknown"):
        assert not main.tripleseat_scheme_signs_timestamp(name), name
    # an unknown pinned name verifies nothing
    assert main.tripleseat_signature_scheme(_TS_BODY, "t=1,v1=ab", FAKE_TS_SECRET, only="t.body:sha1") is None


def _ts_request(headers: dict, query: str = "key=fake-shared-key"):
    from starlette.requests import Request as _Req
    return _Req({"type": "http", "method": "POST", "path": "/webhooks/tripleseat", "query_string": query.encode(),
                 "scheme": "http", "server": ("cdp.example.test", 80),
                 "headers": [(k.lower().encode(), v.encode()) for k, v in
                             {"host": "cdp.example.test", "x-forwarded-proto": "https", **headers}.items()]})


def test_mismatch_log_names_the_candidates_and_leaks_nothing(monkeypatch, caplog):
    monkeypatch.setattr(main, "TRIPLESEAT_WEBHOOK_SECRET", FAKE_TS_SECRET)
    monkeypatch.setattr(main, "TRIPLESEAT_SIGNATURE_ENFORCE", False)
    monkeypatch.setattr(main, "TRIPLESEAT_SIGNATURE_SCHEME", "")
    t = str(int(__import__("time").time()))
    bogus = "ab" * 32
    header = f"t={t},v1={bogus}"
    with caplog.at_level("INFO", logger="pg_cdp"):
        assert main.check_tripleseat_signature(_ts_request({"x-signature": header}), _TS_BODY) is False
    text = caplog.text
    assert "Tripleseat signature MISMATCH" in text and "none matched" in text and "not enforced yet" in text
    assert "1 v1 value(s)" in text and f"body {len(_TS_BODY)} bytes" in text
    for name in ("t.body", "t-nl-body", "t.url.body", "t.json-rails", "t.bodyhash", "base64url"):
        assert name in text, name
    # 20 payloads x 3 encodings x 2 keys (the fake secret's text, and the same text base64url-decoded)
    assert "120 candidates" in text and "keys [raw, b64urlkey]" in text
    for leak in (FAKE_TS_SECRET, bogus, header, "fake-shared-key", '"lead"', "<a&b>"):
        assert leak not in text, leak
    for payload in main.TRIPLESEAT_SIGNATURE_PAYLOADS:      # nor any HMAC value we computed
        for key in main._ts_key_forms(FAKE_TS_SECRET).values():
            msg = main.TRIPLESEAT_SIGNATURE_PAYLOADS[payload][0](main._ts_context(t, _TS_BODY, _TS_URL))
            if msg is None:
                continue
            d = _hmac.new(key, msg, _hashlib.sha256).digest()
            for enc in main.TRIPLESEAT_SIGNATURE_ENCODINGS:
                assert main._ts_encode(d, enc) not in text


def test_check_uses_the_public_url_tripleseat_posted_to(monkeypatch, caplog):
    monkeypatch.setattr(main, "TRIPLESEAT_WEBHOOK_SECRET", FAKE_TS_SECRET)
    monkeypatch.setattr(main, "TRIPLESEAT_SIGNATURE_ENFORCE", False)
    monkeypatch.setattr(main, "TRIPLESEAT_SIGNATURE_SCHEME", "")
    t = str(int(__import__("time").time()))
    d = _hmac.new(FAKE_TS_SECRET.encode(), _ts_expected_message("t.url.body", t, _TS_BODY, _TS_URL),
                  _hashlib.sha256).digest()
    with caplog.at_level("INFO", logger="pg_cdp"):
        assert main.check_tripleseat_signature(_ts_request({"x-signature": f"t={t},v1={d.hex()}"}), _TS_BODY) is False
    assert "Tripleseat signature verified (scheme t.url.body:hex" in caplog.text and "not enforced yet" in caplog.text
    assert "fake-shared-key" not in caplog.text


def test_body_only_new_candidates_never_count_as_authenticated(monkeypatch, caplog):
    monkeypatch.setattr(main, "TRIPLESEAT_WEBHOOK_SECRET", FAKE_TS_SECRET)
    monkeypatch.setattr(main, "TRIPLESEAT_SIGNATURE_ENFORCE", True)
    monkeypatch.setattr(main, "TRIPLESEAT_SIGNATURE_SCHEME", "url.body:base64")
    d = _hmac.new(FAKE_TS_SECRET.encode(), _ts_expected_message("url.body", "1", _TS_BODY, _TS_URL),
                  _hashlib.sha256).digest()
    with caplog.at_level("INFO", logger="pg_cdp"):
        ok = main.check_tripleseat_signature(_ts_request({"x-signature": f"t=1,v1={_b64.b64encode(d).decode()}"}),
                                             _TS_BODY)
    assert ok is False                                       # the shared key is still required
    assert "not a timestamp-signed construction" in caplog.text and "can't replace the shared key" in caplog.text


def test_secret_is_read_stripped_and_its_shape_never_shows_the_value():
    import subprocess, sys as _sys
    code = ("import main; print(repr(main.TRIPLESEAT_WEBHOOK_SECRET)); "
            "print(main.tripleseat_secret_shape(main.TRIPLESEAT_WEBHOOK_SECRET, main._TS_SECRET_AS_SET))")
    env = {**__import__("os").environ, "TRIPLESEAT_WEBHOOK_SECRET": "  fakeSecret123\n"}
    out = subprocess.run([_sys.executable, "-c", code], env=env, capture_output=True, text=True,
                         cwd=__import__("os").path.dirname(main.__file__), check=True).stdout.splitlines()
    assert out[0] == "'fakeSecret123'"
    assert out[1] == "13 characters, alphanumeric, whitespace trimmed from the Render value: yes"
    assert main.tripleseat_secret_shape("fake-se_cret", "fake-se_cret") == \
        "12 characters, URL-safe (letters, digits, - _), whitespace trimmed from the Render value: no"
    assert main.tripleseat_secret_shape(None, "") == "not set"
    assert "fake" not in main.tripleseat_secret_shape("fake+/x=", "fake+/x=")


def test_access_log_never_shows_a_key():
    rec = _logging.LogRecord("uvicorn.access", _logging.INFO, "", 0, '%s - "%s %s HTTP/%s" %d', (
        "1.2.3.4:0", "POST", "/webhooks/tripleseat?key=fake-shared-key&x=1", "1.1", 200), None)
    main._RedactKeyInAccessLog().filter(rec)
    line = rec.getMessage()
    assert "fake-shared-key" not in line and "key=<redacted>&x=1" in line
    rec2 = _logging.LogRecord("uvicorn.access", _logging.INFO, "", 0, "GET /cron/x?key=fake-cron-key", None, None)
    main._RedactKeyInAccessLog().filter(rec2)
    assert "fake-cron-key" not in rec2.getMessage()
    assert any(isinstance(f, main._RedactKeyInAccessLog) for f in _logging.getLogger("uvicorn.access").filters)
