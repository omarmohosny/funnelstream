import random
from datetime import datetime, timezone
from simulator.catalog import make_catalog, make_customers
from simulator.core import journey
from contracts.validate import validate_event

START = datetime(2026, 10, 4, 18, 0, tzinfo=timezone.utc)
CAMPAIGN = {"campaign_id": "camp_202", "utm_campaign": "ramadan_offer_1", "utm_source": "meta", "channel": "paid_social"}


def _journeys(n=200, seed=7):
    catalog = make_catalog(seed)
    customers = make_customers(seed, 50)
    rng = random.Random(seed)
    out = []
    for i in range(n):
        cust = customers[i % len(customers)]
        camp = CAMPAIGN if i % 2 == 0 else None
        out.append(journey(rng, cust, catalog, camp, START))
    return out


def test_every_event_is_valid():
    for events in _journeys():
        for e in events:
            ok, code = validate_event(e, now=datetime(2026, 10, 5, tzinfo=timezone.utc))
            assert ok, code


def test_funnel_is_monotonic():
    for events in _journeys():
        counts = {}
        for e in events:
            counts[e["event_type"]] = counts.get(e["event_type"], 0) + 1
        assert counts.get("product_view", 0) >= counts.get("add_to_cart", 0)
        assert counts.get("add_to_cart", 0) >= counts.get("checkout_started", 0)
        assert counts.get("checkout_started", 0) >= counts.get("purchase", 0)


def test_session_id_is_constant_within_journey():
    for events in _journeys(50):
        assert len({e["session_id"] for e in events}) == 1


def test_utm_only_on_campaign_sessions_first_touch_events():
    for events in _journeys():
        with_utm = [e for e in events if "utm_campaign" in e["payload"]]
        for e in with_utm:
            assert e["event_type"] in ("page_view", "product_view")


def test_anonymous_journeys_have_no_customer_id():
    catalog = make_catalog(1)
    rng = random.Random(1)
    events = journey(rng, None, catalog, None, START)
    assert all("customer_id" not in e or e["customer_id"] is None for e in events)


def test_event_times_are_non_decreasing():
    for events in _journeys(30):
        times = [e["event_time"] for e in events]
        assert times == sorted(times)


def test_phone_format_is_egyptian():
    for c in make_customers(3, 20):
        phone = c["phone"]
        assert phone.startswith("+20")
        assert phone[3:5] in ("10", "11", "12", "15")
        assert len(phone) == 13


def test_no_inactive_products_in_events():
    """Test that inactive products never appear in any event payload."""
    # Create a catalog with half inactive products
    catalog = make_catalog(42, n_products=200)
    # Manually mark half as inactive for testing
    for i in range(len(catalog)):
        if i % 2 == 0:
            catalog[i]["is_active"] = False

    active_product_ids = {p["product_id"] for p in catalog if p["is_active"]}

    # Run 200 journeys
    customers = make_customers(42, 50)
    rng = random.Random(42)
    for i in range(200):
        cust = customers[i % len(customers)]
        camp = CAMPAIGN if i % 2 == 0 else None
        events = journey(rng, cust, catalog, camp, START)

        # Check no event has an inactive product
        for e in events:
            if "product_id" in e["payload"]:
                product_id = e["payload"]["product_id"]
                assert product_id in active_product_ids, f"Event contains inactive product {product_id}"


def test_reproducibility_with_seed():
    """Test that two runs with the same seed produce identical event lists."""
    catalog = make_catalog(7)
    customers = make_customers(7, 50)

    # First run
    rng1 = random.Random(7)
    events1 = []
    for i in range(10):
        cust = customers[i % len(customers)]
        camp = CAMPAIGN if i % 2 == 0 else None
        events1.extend(journey(rng1, cust, catalog, camp, START))

    # Second run with same seed
    rng2 = random.Random(7)
    events2 = []
    for i in range(10):
        cust = customers[i % len(customers)]
        camp = CAMPAIGN if i % 2 == 0 else None
        events2.extend(journey(rng2, cust, catalog, camp, START))

    # Compare
    assert len(events1) == len(events2), "Different number of events generated"
    for e1, e2 in zip(events1, events2):
        assert e1["event_id"] == e2["event_id"], "Event IDs differ"
        assert e1["session_id"] == e2["session_id"], "Session IDs differ"
        assert e1["event_type"] == e2["event_type"], "Event types differ"
        assert e1["event_time"] == e2["event_time"], "Event times differ"
        assert e1["payload"] == e2["payload"], "Payloads differ"
