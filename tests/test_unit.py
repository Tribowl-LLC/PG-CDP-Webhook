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
    assert calls == []                                      # excluded: no Versium at all


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
