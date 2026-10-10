from contracts.geo import DISTRICTS
from contracts.stores import STORES, district_rows, store_for, store_rows

SERVED = ["Cairo", "Giza", "Alexandria", "Mansoura", "Tanta"]


def test_eight_stores_with_the_agreed_split_per_city():
    by_city = {}
    for _, (city, _, _) in STORES.items():
        by_city[city] = by_city.get(city, 0) + 1
    assert len(STORES) == 8
    assert by_city == {"Cairo": 3, "Giza": 1, "Alexandria": 2, "Mansoura": 1, "Tanta": 1}


def test_every_district_of_a_served_city_has_exactly_one_store():
    for city in SERVED:
        for district in DISTRICTS[city]:
            owners = [s for s, (c, _, ds) in STORES.items() if c == city and district in ds]
            assert len(owners) == 1, (city, district, owners)
            assert store_for(city, district) == owners[0]


def test_a_store_serves_only_districts_of_its_own_city():
    for store_id, (city, _, districts) in STORES.items():
        assert set(districts) <= set(DISTRICTS[city]), store_id


def test_unserved_places_have_no_store():
    assert store_for("Assiut", "Assiut First") is None
    assert store_for("Other", "Other") is None
    assert store_for("Cairo", "unknown") is None
    assert store_for(None, None) is None
    assert store_for("Atlantis", "Maadi") is None


def test_the_helpers_match_the_map():
    assert store_for("Cairo", "Maadi") == "store_cairo_maadi"
    assert len(store_rows()) == 8
    assert {r["store_id"] for r in store_rows()} == set(STORES)
    assert len(district_rows()) == sum(len(DISTRICTS[c]) for c in SERVED)
    assert {(r["city"], r["district"]): r["store_id"] for r in district_rows()}[("Giza", "Dokki")] == "store_giza_dokki"
