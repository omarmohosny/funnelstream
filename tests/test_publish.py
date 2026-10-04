import random
from datetime import datetime, timedelta, timezone

from contracts.validate import validate_event
from scripts.run_simulator import build_batch
from simulator.catalog import make_catalog, make_customers
from simulator.publish import build_producer_config, route

START = datetime(2026, 9, 27, tzinfo=timezone.utc)


def _batch(seed=7, sessions=300):
    rng = random.Random(seed)
    return build_batch(rng, make_customers(seed, 200), make_catalog(seed), START, sessions=sessions)


def test_app_events_keyed_by_session():
    assert route("app", {"event_type": "page_view", "session_id": "sess_1"}) == ("app.events", "sess_1")


def test_order_events_keyed_by_order():
    assert route("order", {"event_type": "order_created", "order_id": "ord_9"}) == ("orders", "ord_9")
    assert route("order", {"event_type": "payment_authorised", "order_id": "ord_9"}) == ("orders", "ord_9")


def test_delivery_events_keyed_by_order():
    for et in ("out_for_delivery", "delivered", "delivery_failed"):
        assert route("order", {"event_type": et, "order_id": "ord_9"}) == ("delivery.events", "ord_9")


def test_producer_is_idempotent_and_acks_all():
    cfg = build_producer_config("kafka:9092")
    assert cfg["enable.idempotence"] is True
    assert cfg["acks"] == "all"
    assert cfg["linger.ms"] == 20


def test_all_batch_events_validate():
    now = START + timedelta(days=30)
    batch = _batch()
    assert batch
    for _, e in batch:
        ok, reason = validate_event(e, now=now)
        assert ok, (reason, e)


def test_orders_match_purchases():
    batch = _batch()
    events = [e for _, e in batch]
    purchases = [e for e in events if e["event_type"] == "purchase"]
    created = [e for e in events if e["event_type"] == "order_created"]
    purchase_ids = {p["payload"]["order_id"] for p in purchases}
    assert purchases and created
    for c in created:
        assert c["payload"]["order_id"] in purchase_ids
    for p in purchases:
        n = sum(1 for c in created if c["order_id"] == p["payload"]["order_id"])
        assert n == (1 if "customer_id" in p else 0)


def test_order_created_carries_session_and_click():
    batch = _batch()
    events = [e for _, e in batch]
    sessions_with_campaign = {e["session_id"] for e in events
                              if e["payload"].get("utm_campaign")}
    by_order = {e["payload"]["order_id"]: e["session_id"] for e in events if e["event_type"] == "purchase"}
    created = [e for e in events if e["event_type"] == "order_created"]
    seen_campaign = seen_none = False
    for c in created:
        sid = c["payload"]["session_id"]
        assert sid == by_order[c["order_id"]]
        assert c["session_id"] is None
        if sid in sessions_with_campaign:
            assert c["payload"]["click_id"] == "clk_" + sid[5:]
            seen_campaign = True
        else:
            assert c["payload"]["click_id"] is None
            seen_none = True
    assert seen_campaign and seen_none
    for e in events:
        if e["payload"].get("utm_campaign") and e["event_type"] in ("page_view", "product_view"):
            assert e["payload"]["click_id"] == "clk_" + e["session_id"][5:]


def test_anonymous_sessions_have_no_orders():
    events = [e for _, e in _batch()]
    anon_purchases = [e for e in events if e["event_type"] == "purchase" and "customer_id" not in e]
    assert anon_purchases
    anon_ids = {p["payload"]["order_id"] for p in anon_purchases}
    assert not any(e.get("order_id") in anon_ids for e in events if e["event_type"] != "purchase")


def test_batch_deterministic():
    assert _batch(seed=3, sessions=60) == _batch(seed=3, sessions=60)
