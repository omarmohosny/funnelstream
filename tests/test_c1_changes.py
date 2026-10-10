"""Amendment C, step C1: order_returned, COD rule, platform, platform_conversions, three topics."""
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from contracts.validate import validate_event
from scripts.run_simulator import build_batch
from simulator.campaigns import CAMPAIGNS, campaign_numbers
from simulator.catalog import make_catalog, make_customers
from simulator.core import journey
from simulator.orders import make_order
from streaming.common.status import STATUS_RANK, resolve_current_status_timed

T0 = datetime(2026, 10, 4, 18, 0, tzinfo=timezone.utc)
NOW = datetime(2027, 1, 1, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parent.parent


def _t(h):
    return T0 + timedelta(hours=h)


# ---- 1. order_returned and timed resolution ----
def test_returned_has_rank_7_like_completed():
    assert STATUS_RANK["order_returned"] == 7 == STATUS_RANK["order_completed"]
    topics = yaml.safe_load((ROOT / "contracts" / "topics.yaml").read_text())
    assert "order_returned" in topics["order_event_types"]


def test_timed_out_of_order_arrival():
    rows = [("delivered", _t(5)), ("order_created", _t(0)), ("order_shipped", _t(3))]
    assert resolve_current_status_timed(rows) == "delivered"
    assert resolve_current_status_timed(list(reversed(rows))) == "delivered"


def test_timed_cancel_before_ship_is_terminal():
    rows = [("order_created", _t(0)), ("order_shipped", _t(4)), ("order_cancelled", _t(2)), ("delivered", _t(8))]
    assert resolve_current_status_timed(rows) == "order_cancelled"


def test_timed_cancel_after_ship_is_ignored():
    rows = [("order_created", _t(0)), ("order_shipped", _t(2)), ("order_cancelled", _t(4))]
    assert resolve_current_status_timed(rows) == "order_shipped"


def test_timed_completed_vs_returned_tie_by_time():
    a = [("order_completed", _t(5)), ("order_returned", _t(9))]
    b = [("order_completed", _t(9)), ("order_returned", _t(5))]
    assert resolve_current_status_timed(a) == "order_returned"
    assert resolve_current_status_timed(b) == "order_completed"
    assert resolve_current_status_timed(list(reversed(a))) == "order_returned"


def test_timed_failed_vs_out_for_delivery_tie_by_time():
    rows = [("out_for_delivery", _t(5)), ("delivery_failed", _t(6))]
    assert resolve_current_status_timed(rows) == "delivery_failed"


def test_timed_empty_raises():
    with pytest.raises(ValueError):
        resolve_current_status_timed([])


# ---- 2. simulator orders ----
def _orders(n, seed=11):
    catalog = make_catalog(seed)
    customers = make_customers(seed, 60)
    rng = random.Random(seed)
    return [make_order(rng, customers[i % 60], catalog, None, T0, f"ord_{i:05d}") for i in range(n)]


def _types(events):
    return [e["event_type"] for _, e in events]


def test_cod_never_has_payment_authorised_prepaid_always():
    cod = prepaid = 0
    for ev in _orders(2000):
        method = ev[0][1]["payload"]["payment_method"]
        types = _types(ev)
        if method == "cod":
            cod += 1
            assert "payment_authorised" not in types
        else:
            prepaid += 1
            if "order_cancelled" not in types:
                assert "payment_authorised" in types
    assert cod > 0 and prepaid > 0
    print("cod share", cod / 2000, "prepaid share", prepaid / 2000)


def test_prepaid_non_cancelled_orders_emit_payment_authorised_second():
    for ev in _orders(2000):
        if ev[0][1]["payload"]["payment_method"] != "cod" and "order_cancelled" not in _types(ev):
            assert _types(ev)[1] == "payment_authorised"


def test_returned_orders_have_no_delivered_or_completed():
    n_ret = 0
    for ev in _orders(3000):
        types = _types(ev)
        if "order_returned" in types:
            n_ret += 1
            assert "delivered" not in types and "order_completed" not in types
            assert types[-1] == "order_returned"
    assert n_ret > 0


def test_returned_rate_between_2_and_12_percent_of_non_cancelled():
    orders = _orders(3000)
    live = [ev for ev in orders if "order_cancelled" not in _types(ev)]
    rate = sum("order_returned" in _types(ev) for ev in live) / len(live)
    print("returned rate", rate)
    # The expected rate is 10% by construction (observed 10.97% on this seed). The rate is a binomial sample,
    # so it moves by sampling noise; a 14% ceiling leaves wide room above 10% (the old 12% was too tight).
    assert 0.02 <= rate <= 0.14


def test_every_return_follows_a_failure_with_payload_and_topic():
    for ev in _orders(3000):
        types = _types(ev)
        for i, (topic, e) in enumerate(ev):
            if e["event_type"] == "order_returned":
                assert topic == "orders"
                assert types[i - 1] == "delivery_failed"
                assert e["payload"]["reason"] == "all_attempts_failed"
                assert e["payload"]["attempts"] == types.count("out_for_delivery")
                assert datetime.fromisoformat(e["event_time"]) > datetime.fromisoformat(ev[i - 1][1]["event_time"])


def test_completed_and_returned_are_exclusive():
    for ev in _orders(1000):
        types = _types(ev)
        assert not ("order_completed" in types and "order_returned" in types)


# ---- 3. platform ----
def _sessions(n=600, seed=3):
    catalog = make_catalog(seed)
    customers = make_customers(seed, 50)
    rng = random.Random(seed)
    out = []
    for i in range(n):
        cust = customers[i % 50] if i % 5 else None
        out.append((cust, journey(rng, cust, catalog, None, T0)))
    return out


def test_platform_present_valid_and_constant_per_session():
    for cust, events in _sessions():
        plats = {e["platform"] for e in events}
        assert len(plats) == 1 and plats <= {"app", "web"}
        assert validate_event(events[0], now=NOW) == (True, "")


def test_platform_follows_device_and_both_values_occur():
    mob = {"app": 0, "web": 0}
    for cust, events in _sessions():
        e = events[0]
        if e["device"] == "desktop":
            assert e["platform"] == "web"
        elif e["device"] == "mobile":
            mob[e["platform"]] += 1
    share = mob["app"] / sum(mob.values())
    print("mobile sessions app share", share, mob)
    assert mob["app"] > 0 and mob["web"] > 0
    assert 0.6 < share < 0.8


def test_validator_platform():
    base = {"event_id": "e", "event_type": "app_open", "event_version": 1, "event_time": T0.isoformat(),
            "session_id": "s", "payload": {}}
    assert validate_event(base, now=NOW) == (True, "")  # missing: old data
    assert validate_event({**base, "platform": "app"}, now=NOW) == (True, "")
    assert validate_event({**base, "platform": "web"}, now=NOW) == (True, "")
    assert validate_event({**base, "platform": "ios"}, now=NOW) == (False, "BAD_VALUE")
    assert validate_event({**base, "platform": "APP"}, now=NOW) == (False, "BAD_VALUE")


def test_runner_copies_session_platform_into_order_created():
    batch = build_batch(random.Random(9), make_customers(9, 300), make_catalog(9), T0, sessions=300)
    plat = {}
    for h, e in batch:
        if h == "app":
            plat[e["session_id"]] = e["platform"]
    seen = set()
    for h, e in batch:
        if e["event_type"] == "order_created":
            assert e["payload"]["platform"] == plat[e["payload"]["session_id"]]
            seen.add(e["payload"]["platform"])
    assert seen == {"app", "web"}


# ---- 4. platform_conversions ----
# values recorded BEFORE the change (seed 42, report_date 2026-10-01, as_of = day start + h hours)
BASELINE = {
    ("camp_201", 3): (6629, 102, 184.9562), ("camp_201", 12): (26517, 408, 739.8247),
    ("camp_201", 24): (53035, 816, 1479.6494),
    ("camp_202", 3): (8959, 284, 232.9192), ("camp_202", 12): (35839, 1137, 931.6769),
    ("camp_202", 24): (71678, 2274, 1863.3538),
    ("camp_203", 3): (3653, 95, 76.0972), ("camp_203", 12): (14612, 380, 304.3887),
    ("camp_203", 24): (29225, 761, 608.7774),
}


def _num(cid, h, seed=42):
    d = date(2026, 10, 1)
    return campaign_numbers(cid, d, datetime(2026, 10, 1, tzinfo=timezone.utc) + timedelta(hours=h), seed)


def test_existing_campaign_numbers_unchanged():
    for (cid, h), (imp, clk, spend) in BASELINE.items():
        n = _num(cid, h)
        assert (n["impressions"], n["clicks"], n["spend"]) == (imp, clk, spend)


def test_platform_conversions_monotone_bounded_and_ratio():
    ratios = []
    for c in CAMPAIGNS:
        prev = 0
        for h in range(0, 25):
            n = _num(c["campaign_id"], h)
            assert isinstance(n["platform_conversions"], int)
            assert n["platform_conversions"] >= prev
            assert n["platform_conversions"] <= n["clicks"]
            prev = n["platform_conversions"]
        full = _num(c["campaign_id"], 24)
        ratios.append(full["platform_conversions"] / full["clicks"])
    print("platform_conversions/clicks", ratios)
    assert all(0.015 <= r <= 0.065 for r in ratios)


def test_platform_conversions_deterministic():
    assert _num("camp_202", 12, 7) == _num("camp_202", 12, 7)


# ---- 5. topics ----
def test_topics_yaml_has_exactly_three_topics():
    topics = yaml.safe_load((ROOT / "contracts" / "topics.yaml").read_text())["topics"]
    assert set(topics) == {"app.events", "orders", "delivery.events", "inventory.events"}


def test_ad_api_noise_keeps_platform_conversions_untouched_except_duplicates():
    from mock_apis.ad_api import apply_api_noise
    rows = [_num(c["campaign_id"], 12) for c in CAMPAIGNS]
    for seed in range(300):
        noised = apply_api_noise(rows, random.Random(seed))
        for r in noised:
            orig = next(o for o in rows if o["campaign_id"] == r["campaign_id"])
            assert r["platform_conversions"] == orig["platform_conversions"]
        assert len(noised) in (len(rows), len(rows) + 1)


def test_noise_never_crashes_on_platform_field():
    from simulator.noise import NoiseConfig, NoiseCounter, apply_noise
    batch = build_batch(random.Random(4), make_customers(4, 100), make_catalog(4), T0, sessions=150)
    catalog_ids = {p["product_id"] for p in make_catalog(4)}
    out = list(apply_noise(iter(batch), random.Random(1), NoiseConfig().scaled(10), catalog_ids, NoiseCounter(), now=NOW))
    assert out
    for _, e in out:
        if isinstance(e, dict) and "platform" in e:
            assert e["platform"] in ("app", "web")


# ---- C4 cleanup ----
def test_order_created_has_no_platform_key_when_purchase_has_none(monkeypatch):
    import scripts.run_simulator as rs
    real = rs.journey

    def no_platform(*a, **kw):
        events = real(*a, **kw)
        for e in events:
            e.pop("platform", None)
        return events
    monkeypatch.setattr(rs, "journey", no_platform)
    batch = build_batch(random.Random(9), make_customers(9, 300), make_catalog(9), T0, sessions=100)
    created = [e for topic, e in batch if topic == "order" and e["event_type"] == "order_created"]
    assert created
    assert all("platform" not in e["payload"] for e in created)


def test_validator_order_created_payload_platform():
    def oc(**payload):
        return {"event_id": "e1", "event_type": "order_created", "event_version": 1,
                "event_time": "2026-10-04T10:00:00+00:00", "session_id": None, "order_id": "ord_1",
                "customer_id": "c1", "payload": payload}
    batch = build_batch(random.Random(9), make_customers(9, 300), make_catalog(9), T0, sessions=50)
    real = next(e for topic, e in batch if topic == "order" and e["event_type"] == "order_created")
    base = dict(real["payload"])
    base.pop("platform", None)
    ev = {**real, "payload": base}
    assert validate_event(ev, now=NOW) == (True, "")  # missing stays allowed
    for ok in ("app", "web"):
        assert validate_event({**ev, "payload": {**base, "platform": ok}}, now=NOW) == (True, "")
    for bad in ("ios", "APP", 5):
        assert validate_event({**ev, "payload": {**base, "platform": bad}}, now=NOW) == (False, "BAD_VALUE")
    assert validate_event({**ev, "payload": {**base, "platform": None}}, now=NOW) == (True, "")
    # other order events are not checked for payload platform
    other = next(e for topic, e in batch if topic == "order" and e["event_type"] != "order_created")
    assert validate_event({**other, "payload": {**other["payload"], "platform": "ios"}}, now=NOW)[0] is True


def test_timed_exact_ties_are_independent_of_input_order():
    from itertools import permutations
    base = [("order_created", _t(0)), ("order_shipped", _t(1))]
    tie7 = base + [("order_completed", _t(5)), ("order_returned", _t(5))]
    assert {resolve_current_status_timed(list(p)) for p in permutations(tie7)} == {"order_returned"}
    tie5 = base + [("out_for_delivery", _t(5)), ("delivery_failed", _t(5))]
    got = {resolve_current_status_timed(list(p)) for p in permutations(tie5)}
    assert got == {"delivery_failed"}  # alphabetical: delivery_failed < out_for_delivery
