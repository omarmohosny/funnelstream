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
