import random
from datetime import datetime, timezone
from contracts.validate import validate_event
from simulator.catalog import make_catalog, make_customers
from simulator.orders import make_order
from streaming.common.status import STATUS_RANK, resolve_current_status

T0 = datetime(2026, 10, 4, 18, 0, tzinfo=timezone.utc)


def _orders(n=300, seed=5):
    catalog = make_catalog(seed)
    customers = make_customers(seed, 40)
    rng = random.Random(seed)
    return [make_order(rng, customers[i % 40], catalog, None, T0, f"ord_{i:05d}") for i in range(n)]


def test_rank_is_strictly_ordered_on_main_path():
    path = ["order_created", "payment_authorised", "order_packed", "order_shipped",
            "out_for_delivery", "delivered", "order_completed"]
    ranks = [STATUS_RANK[s] for s in path]
    assert ranks == sorted(ranks)
    assert len(set(ranks[:4])) == 4


def test_resolve_ignores_arrival_order():
    assert resolve_current_status(["delivered", "order_shipped", "order_created"]) == "delivered"


def test_cancel_before_shipment_wins():
    assert resolve_current_status(["order_created", "order_cancelled"]) == "order_cancelled"


def test_cancel_after_shipment_is_not_current():
    assert resolve_current_status(["order_created", "order_shipped", "order_cancelled"]) == "order_shipped"


def test_every_delivered_order_was_shipped():
    for events in _orders():
        types = [e["event_type"] for _, e in events if e["event_type"] in ("order_shipped", "delivered")]
        if "delivered" in types:
            assert "order_shipped" in types


def test_failed_attempts_are_followed_by_retry_or_return():
    for events in _orders():
        seq = [e["event_type"] for _, e in events
               if e["event_type"] in ("delivered", "delivery_failed", "out_for_delivery", "order_returned")]
        for i, s in enumerate(seq):
            if s == "delivery_failed":
                assert i + 1 < len(seq)
                assert seq[i + 1] in ("out_for_delivery", "order_returned")
        if "order_returned" in seq:
            assert seq[-1] == "order_returned" and seq[-2] == "delivery_failed"


def test_topics_are_routed_correctly():
    for events in _orders(50):
        for topic, e in events:
            if e["event_type"].startswith("order_") or e["event_type"] == "payment_authorised":
                assert topic == "orders"
            else:
                assert topic == "delivery.events"


def test_all_events_pass_contract_validation():
    now = datetime(2027, 1, 1, tzinfo=timezone.utc)
    for events in _orders():
        for _, e in events:
            ok, reason = validate_event(e, now=now)
            assert ok, (e["event_type"], reason)


def test_seeded_runs_are_identical():
    assert _orders(100, seed=5) == _orders(100, seed=5)


def test_retry_then_delivered_and_cancellations_occur():
    orders = _orders(300)
    retried = 0
    cancelled = 0
    for events in orders:
        types = [e["event_type"] for _, e in events]
        if "order_cancelled" in types:
            cancelled += 1
        if types.count("delivery_failed") >= 1 and "delivered" in types:
            retried += 1
    assert retried >= 1
    assert cancelled >= 1


def test_event_times_non_decreasing_per_order():
    for events in _orders():
        times = [e["event_time"] for _, e in events]
        assert times == sorted(times)


def test_cancelled_order_has_no_shipment():
    for events in _orders():
        types = [e["event_type"] for _, e in events]
        if "order_cancelled" in types:
            assert "order_shipped" not in types
            assert types == ["order_created", "order_cancelled"]


def test_only_active_products_ordered():
    catalog = make_catalog(5)
    active = {p["product_id"] for p in catalog if p["is_active"]}
    for events in _orders():
        for pl in events[0][1]["payload"]["items"]:
            assert pl["product_id"] in active
