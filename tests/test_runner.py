import random
from datetime import datetime, timedelta, timezone

import pytest

from scripts.run_simulator import (build_batch, compress_orders, guard_past, iter_batch,
                                   pace_live, resolve_start)
from simulator.catalog import make_catalog, make_customers
from simulator.publish import Publisher, build_producer_config

START = datetime(2026, 9, 27, tzinfo=timezone.utc)


def test_pace_live_never_early_and_duration_matches():
    base = datetime(2026, 10, 4, tzinfo=timezone.utc)
    batch = build_batch(random.Random(5), make_customers(5, 100), make_catalog(5), base,
                        sessions=20, mean_gap=0.5)
    clock = {"t": base}

    def sleep_fn(s):
        clock["t"] += timedelta(seconds=s)

    emitted = []

    def emit(h, e):
        emitted.append((clock["t"], datetime.fromisoformat(e["event_time"])))

    pace_live(batch, emit, now_fn=lambda: clock["t"], sleep_fn=sleep_fn)
    assert len(emitted) == len(batch)
    assert all(now >= et for now, et in emitted)
    last = max(et for _, et in emitted)
    assert clock["t"] - base == last - base


def test_compress_orders_scales_lifecycle():
    batch = build_batch(random.Random(7), make_customers(7, 200), make_catalog(7), START, sessions=200)
    orig = {e["order_id"]: datetime.fromisoformat(e["event_time"])
            for _, e in batch if e["event_type"] == "order_created"}
    long = [e for _, e in batch if e["event_type"] == "delivered"]
    assert long
    before = {e["order_id"]: (datetime.fromisoformat(e["event_time"]) - orig[e["order_id"]]).total_seconds()
              for e in long}
    compress_orders(batch, 3600)
    for e in long:
        d = (datetime.fromisoformat(e["event_time"]) - orig[e["order_id"]]).total_seconds()
        assert abs(d - before[e["order_id"]] / 3600) < 1e-3


def test_history_default_start_stays_in_past():
    now = datetime.now(timezone.utc)
    start = resolve_start(None, False, now)
    it = iter_batch(random.Random(1), make_customers(1, 100), make_catalog(1), start, max_events=2000)
    assert len(list(guard_past(it, now))) >= 2000


def test_history_start_now_raises():
    now = datetime.now(timezone.utc)
    it = iter_batch(random.Random(1), make_customers(1, 100), make_catalog(1), now, max_events=200)
    with pytest.raises(ValueError, match="earlier --start"):
        list(guard_past(it, now))


def test_resolve_start_naive_is_utc():
    s = resolve_start("2026-10-01T00:00:00", False, datetime.now(timezone.utc))
    assert s.utcoffset() == timedelta(0)


def test_iter_batch_is_generator_and_matches_build_batch():
    it = iter_batch(random.Random(2), make_customers(2, 50), make_catalog(2), START, sessions=10)
    assert iter(it) is it
    assert list(it) == build_batch(random.Random(2), make_customers(2, 50), make_catalog(2), START, sessions=10)


class FakeProducer:
    def __init__(self, fails):
        self.fails, self.produced, self.polls = fails, [], []

    def produce(self, topic, key=None, value=None):
        if self.fails:
            self.fails -= 1
            raise BufferError("full")
        self.produced.append((topic, key, value))

    def poll(self, t):
        self.polls.append(t)


def test_publisher_retries_on_buffer_error():
    fp = FakeProducer(2)
    Publisher("x", producer=fp).send("orders", "k", {"a": 1})
    assert len(fp.produced) == 1
    assert fp.polls.count(1) == 2


def test_publisher_gives_up_after_retries():
    with pytest.raises(BufferError):
        Publisher("x", producer=FakeProducer(10**6), max_retries=3).send("orders", "k", {})


def test_producer_queue_size_configured():
    cfg = build_producer_config("x")
    assert cfg["queue.buffering.max.messages"] == 500000
    assert cfg["enable.idempotence"] is True and cfg["acks"] == "all"


def test_universe_seed_keeps_customers_and_catalog_but_changes_event_ids():
    import random
    from datetime import datetime, timezone
    from scripts.run_simulator import build_batch
    from simulator.catalog import make_catalog, make_customers
    t0 = datetime(2026, 9, 1, tzinfo=timezone.utc)

    def stream(event_seed, universe):
        batch = build_batch(random.Random(event_seed), make_customers(universe, 200), make_catalog(universe), t0, sessions=30)
        return [e["event_id"] for _, e in batch], {e.get("customer_id") for _, e in batch if e.get("customer_id")}

    ids_a, custs_a = stream(21, 7)
    ids_b, _ = stream(22, 7)
    assert not set(ids_a) & set(ids_b)  # a new event seed never repeats ids
    assert all(c.startswith("cust_") for c in custs_a)
    assert make_customers(7, 5)[0] == make_customers(7, 5)[0]


def test_run_pipeline_accepts_universe_seed(tmp_path):
    import inspect
    from scripts.run_simulator import run_pipeline
    assert "universe_seed" in inspect.signature(run_pipeline).parameters


def test_format_event_shows_type_time_and_key_fields_and_handles_raw():
    from scripts.run_simulator import format_event
    line = format_event("app.events", {"event_type": "page_view", "event_time": "2026-10-06T12:34:56+00:00",
                                       "session_id": "sess_0123456789abcdef", "city": "Cairo", "district": "Maadi",
                                       "platform": "app", "payload": {"page": "home",
                                       "page_url": "https://shop.example.eg/?utm_source=meta"}})
    assert "page_view" in line and "12:34:56" in line and "Cairo/Maadi" in line and "utm_source=meta" in line
    assert "89abcdef" in line and "0123456789abc" not in line  # only the last 8 characters of an id
    assert format_event("app.events", "{broken json").startswith("app.events") and "RAW" in format_event("x", "{broken")


def test_run_pipeline_shows_every_nth_event():
    import inspect
    from scripts.run_simulator import run_pipeline
    assert {"show_every", "show_fn"} <= set(inspect.signature(run_pipeline).parameters)
