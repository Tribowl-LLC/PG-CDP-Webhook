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
