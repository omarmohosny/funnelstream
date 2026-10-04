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
