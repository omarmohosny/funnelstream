import copy
import random
from datetime import datetime, timedelta, timezone

import pytest

from contracts.stores import store_for
from contracts.validate import validate_event
from scripts import run_simulator as rs
from scripts.run_simulator import build_parser, iter_batch, run_pipeline
from simulator.catalog import make_catalog, make_customers
from simulator.noise import NoiseConfig, NoiseCounter, apply_noise, apply_noise_timed
from simulator.orders import make_express_order, make_order
from simulator.stock import StockLedger

T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)
CATALOG = make_catalog(7)
CUSTOMERS = make_customers(7, 1000)
SERVED = next(c for c in CUSTOMERS if store_for(c["city"], c.get("district")))
UNSERVED = [c for c in CUSTOMERS if store_for(c["city"], c.get("district")) is None]
CATALOG_IDS = {p["product_id"] for p in CATALOG}


class Pub:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, event):
        self.sent.append((topic, key, event))

    def flush(self):
        pass


def minutes_after_created(events):
    created = next(e for _, e in events if e["event_type"] == "order_created")
    t0 = datetime.fromisoformat(created["event_time"])
    return {e["event_type"]: (datetime.fromisoformat(e["event_time"]) - t0).total_seconds() / 60 for _, e in events}


def express(seed=1, **kw):
    return make_express_order(random.Random(seed), SERVED, CATALOG, None, T0, "ord_x", **kw)


def delivery_minutes(events, created):
    """Minutes from order_created to delivered, for every express order of `created` ({order_id: event})."""
    return {e["order_id"]: (datetime.fromisoformat(e["event_time"])
                            - datetime.fromisoformat(created[e["order_id"]]["event_time"])).total_seconds() / 60
            for e in events if e["event_type"] == "delivered" and e["order_id"] in created}


def express_created(events):
    return {e["order_id"]: e for e in events if e["event_type"] == "order_created"
            and e["payload"].get("service") == "express"}


def order_events(stream):
    return [e for h, e in stream if h == "order"]


def test_the_express_order_event_carries_service_and_promise_and_is_valid():
    events = express()
    created = next(e for _, e in events if e["event_type"] == "order_created")
    assert created["payload"]["service"] == "express" and created["payload"]["promised_minutes"] == 20
    for _, e in events:
        assert validate_event(e, T0 + timedelta(days=30)) == (True, ""), e["event_type"]


def test_the_timeline_follows_the_express_script():
    for seed in range(200):
        events = express(seed)
        m = minutes_after_created(events)
        kinds = [e["event_type"] for _, e in events]
        assert kinds[0] == "order_created" and kinds[-1] == "order_completed"
        assert not {"order_cancelled", "delivery_failed", "order_returned"} & set(kinds)
        if "payment_authorised" in m:
            assert 10 / 60 <= m["payment_authorised"] <= 40 / 60
        assert 2 <= m["order_packed"] <= 4
        assert 0.9 <= m["order_shipped"] - m["order_packed"] <= 1.1
        assert 0.5 <= m["out_for_delivery"] - m["order_shipped"] <= 1.0
        assert 8 <= m["delivered"] <= 17.5
        assert m["order_completed"] - m["delivered"] == pytest.approx(0.5)
        times = [datetime.fromisoformat(e["event_time"]) for _, e in events]
        assert times == sorted(times)


def test_cash_on_delivery_skips_the_payment_event():
    seen = set()
    for seed in range(100):
        events = express(seed)
        created = next(e for _, e in events if e["event_type"] == "order_created")
        has = any(e["event_type"] == "payment_authorised" for _, e in events)
        cod = created["payload"]["payment_method"] == "cod"
        assert has != cod
        seen.add(cod)
    assert seen == {True, False}


def test_the_topics_match_make_order():
    standard = make_order(random.Random(3), SERVED, CATALOG, None, T0, "ord_x")
    topic_of = {e["event_type"]: t for t, e in standard}
    got = express(3)
    assert all(t == topic_of[e["event_type"]] for t, e in got if e["event_type"] in topic_of)
    assert {t for t, _ in got} <= {"orders", "delivery.events"}


def test_first_item_leads_the_lines():
    first = next(p for p in CATALOG if p["is_active"])
    created = next(e for _, e in express(first_item=first) if e["event_type"] == "order_created")
    assert created["payload"]["items"][0]["product_id"] == first["product_id"]


def test_a_late_order_is_delivered_after_the_given_minutes():
    m = minutes_after_created(express(late_minutes=23.0))
    assert m["delivered"] == pytest.approx(23.0)
    assert m["order_completed"] > m["delivered"]


def test_a_shorter_promise_scales_the_timeline_and_never_exceeds_the_promise():
    top = 0
    for seed in range(300):
        events = express(seed, promised_minutes=2)
        created = next(e for _, e in events if e["event_type"] == "order_created")
        assert created["payload"]["promised_minutes"] == 2
        m = minutes_after_created(events)
        top = max(top, m["delivered"])
        assert m["order_shipped"] < m["out_for_delivery"] <= m["delivered"] <= m["order_completed"]
    assert top < 1.9


def test_the_runner_flags_exist_with_defaults():
    args = build_parser().parse_args([])
    assert args.express_promise_minutes == 20 and args.late_express_every == 0
    args = build_parser().parse_args(["--express-promise-minutes", "2", "--late-express-every", "5"])
    assert args.express_promise_minutes == 2 and args.late_express_every == 5


def test_with_the_ledger_a_served_customers_order_is_express_and_others_are_standard():
    ledger = StockLedger(3, CATALOG, T0)
    stream = list(iter_batch(random.Random(4), CUSTOMERS, CATALOG, T0, max_events=3000, ledger=ledger))
    created = [e for e in order_events(stream) if e["event_type"] == "order_created"]
    kinds = set()
    for e in created:
        served = store_for(e["city"], e.get("district")) is not None
        assert (e["payload"].get("service") == "express") == served, e["order_id"]
        kinds.add(served)
        if served:
            assert e["payload"]["promised_minutes"] == 20
        else:
            assert "promised_minutes" not in e["payload"]
    assert kinds == {True, False}
    assert any(h == "inventory" and e["payload"].get("reason") == "sold" for h, e in stream)  # lines reach the ledger


def test_no_express_order_is_late_without_the_flag():
    ledger = StockLedger(3, CATALOG, T0)
    events = order_events(iter_batch(random.Random(8), CUSTOMERS, CATALOG, T0, max_events=5000, ledger=ledger))
    created = express_created(events)
    durations = delivery_minutes(events, created)
    assert len(created) > 20 and len(durations) == len(created)
    assert max(durations.values()) < 19.5


def test_every_kth_express_order_is_late_with_the_flag():
    ledger = StockLedger(3, CATALOG, T0)
    events = order_events(iter_batch(random.Random(8), CUSTOMERS, CATALOG, T0, max_events=3000, ledger=ledger,
                                     late_express_every=4))
    created = express_created(events)
    durations = delivery_minutes(events, created)
    late = [o for o, d in durations.items() if d > 20]
    assert len(late) == len(created) // 4 and late
    assert all(d < 19.5 for o, d in durations.items() if o not in late)
    assert all(21 <= durations[o] <= 28 for o in late)


def test_with_a_shorter_promise_the_stream_carries_it():
    ledger = StockLedger(3, CATALOG, T0)
    events = order_events(iter_batch(random.Random(8), CUSTOMERS, CATALOG, T0, max_events=3000, ledger=ledger,
                                     express_promise_minutes=2))
    created = express_created(events)
    assert created and all(e["payload"]["promised_minutes"] == 2 for e in created.values())
    assert max(delivery_minutes(events, created).values()) < 1.9


def test_with_only_unserved_customers_the_ledger_stream_has_no_express_order():
    stream = list(iter_batch(random.Random(4), UNSERVED, CATALOG, T0, max_events=1500,
                             ledger=StockLedger(3, CATALOG, T0)))
    assert [e for e in order_events(stream) if e["event_type"] == "order_created"]
    assert not [e for e in order_events(stream) if e["payload"].get("service")]
    plain = list(iter_batch(random.Random(4), UNSERVED, CATALOG, T0, max_events=1500,
                            ledger=StockLedger(3, CATALOG, T0), express=False))
    assert plain == stream  # identical to the stream before express orders existed


# ---- noise must not touch express orders ----

def heavy():
    return NoiseConfig().scaled(40)


def express_stream(n=3000):
    ids = set()
    stream = list(iter_batch(random.Random(8), CUSTOMERS, CATALOG, T0, max_events=n,
                             ledger=StockLedger(3, CATALOG, T0), express_ids=ids))
    return stream, ids


def protect_of(ids):
    return lambda e: isinstance(e, dict) and e.get("order_id") in ids


def test_the_express_ids_are_collected_by_the_generator():
    stream, ids = express_stream(1500)
    assert ids and ids == set(express_created(order_events(stream)))


def test_express_events_are_unchanged_under_heavy_noise():
    stream, ids = express_stream()
    counter = NoiseCounter()
    out = list(apply_noise(iter(stream), random.Random(1), heavy(), CATALOG_IDS, counter, now=NOW,
                           protect=protect_of(ids)))
    assert sum(counter.counts.values()) > 100
    want = [e for e in order_events(stream) if e["order_id"] in ids]
    got = [e for h, e in out if h == "order" and isinstance(e, dict) and e.get("order_id") in ids]
    assert got == want  # same events, same times, same order, no duplicates, nothing extra


def test_without_the_predicate_noise_does_move_them():
    stream, ids = express_stream()
    out = list(apply_noise(iter(stream), random.Random(1), heavy(), CATALOG_IDS, NoiseCounter(), now=NOW))
    want = [e for e in order_events(stream) if e["order_id"] in ids]
    got = [e for h, e in out if h == "order" and isinstance(e, dict) and e.get("order_id") in ids]
    assert got != want


def test_the_timed_variant_honours_the_predicate_too():
    stream, ids = express_stream(1500)
    out = list(apply_noise_timed(iter(stream), random.Random(1), heavy(), CATALOG_IDS, NoiseCounter(), now=NOW,
                                 protect=protect_of(ids)))
    want = [e for e in order_events(stream) if e["order_id"] in ids]
    got = [p for h, p, _ in out if h == "order" and isinstance(p, dict) and p.get("order_id") in ids]
    assert got == want


# ---- live mode keeps real minutes ----

def offsets(batch, ids, want_express):
    created = {e["order_id"]: datetime.fromisoformat(e["event_time"]) for h, e in batch
               if h == "order" and e["event_type"] == "order_created"}
    return {(e["order_id"], e["event_type"]): (datetime.fromisoformat(e["event_time"]) - created[e["order_id"]]).total_seconds()
            for h, e in batch if h == "order" and e["order_id"] in created and (e["order_id"] in ids) == want_express}


def test_live_compression_leaves_express_orders_in_real_minutes_and_shrinks_standard_ones():
    ids, starts = set(), {}
    batch = list(iter_batch(random.Random(11), CUSTOMERS, CATALOG, T0, max_events=1500, mean_gap=0.2, starts=starts,
                            express=True, express_ids=ids))
    assert ids and any(e["event_type"] == "order_created" and e["order_id"] not in ids for h, e in batch if h == "order")
    before_express, before_std = offsets(batch, ids, True), offsets(batch, ids, False)
    batch = copy.deepcopy(batch)
    rs.compress_sessions(batch, 60.0, starts)
    rs.compress_orders(batch, 21600.0)
    rs.cap_order_tail(batch, 60.0)
    assert offsets(batch, ids, True) == pytest.approx(before_express)
    assert max(offsets(batch, ids, False).values()) <= 60.0 + 1e-6 < max(before_std.values())


def test_the_live_pipeline_keeps_the_express_promise_and_ledger_sales(tmp_path):
    live_now = datetime.now(timezone.utc)
    pub = Pub()
    run_pipeline(seed=5, events=600, start=live_now, now=live_now, live=True, pub=pub, config=NoiseConfig.off(),
                 manifest_path=str(tmp_path / "m.json"), rate=50.0, sleep_fn=lambda s: None,
                 now_fn=lambda: live_now + timedelta(hours=1), inventory=True, express_promise_minutes=2,
                 late_express_every=3)
    ev = [e for t, _, e in pub.sent if isinstance(e, dict)]
    created = express_created(ev)
    assert created and all(e["payload"]["promised_minutes"] == 2 for e in created.values())
    durations = list(delivery_minutes(ev, created).values())
    assert any(d > 2 for d in durations) and any(d < 1.9 for d in durations)  # some late, most on time
    assert any(e["event_type"] == "stock_movement" and e["payload"]["reason"] == "sold" for e in ev)


def test_the_history_runner_keeps_every_express_order_on_time_under_noise(tmp_path):
    pub = Pub()
    run_pipeline(seed=5, events=10000, start=NOW - timedelta(days=3), now=NOW, live=False, pub=pub,
                 config=NoiseConfig().scaled(10), manifest_path=str(tmp_path / "m.json"), inventory=True)
    ev = [e for t, _, e in pub.sent if isinstance(e, dict) and "order_id" in e]
    created = express_created(ev)
    durations = delivery_minutes(ev, created)
    assert len(created) > 20 and durations
    assert max(durations.values()) < 19.5
