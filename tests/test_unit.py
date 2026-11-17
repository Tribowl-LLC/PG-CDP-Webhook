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
