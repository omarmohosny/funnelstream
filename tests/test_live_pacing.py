"""Live-mode schedule measured on a fake clock (no sleeping)."""
from datetime import datetime, timedelta, timezone

import pytest

from scripts import run_simulator as rs
from simulator.noise import NoiseConfig

START = datetime(2026, 10, 4, 12, tzinfo=timezone.utc)
RATE = 50.0
SEED = 11


class RecPub:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, payload):
        self.sent.append((topic, key, payload))

    def flush(self):
        pass


def run_live(tmp_path, events=3000, seed=SEED, rate=RATE, **kw):
    clock = {"t": START}

    def sleep_fn(s):
        clock["t"] += timedelta(seconds=s)

    pub = RecPub()
    rs.run_pipeline(seed=seed, events=events, start=START, now=START, live=True, pub=pub,
                    config=NoiseConfig.off(), manifest_path=str(tmp_path / "m.json"), rate=rate,
                    now_fn=lambda: clock["t"], sleep_fn=sleep_fn, **kw)
    return pub.sent, (clock["t"] - START).total_seconds()


@pytest.fixture(scope="module")
def live_run(tmp_path_factory):
    return run_live(tmp_path_factory.mktemp("live"))


def _times(sent):
    return [(t, datetime.fromisoformat(p["event_time"]), p) for t, _, p in sent]


def test_app_event_rate_close_to_requested(live_run):
    sent, _ = live_run
    app = [et for t, et, _ in _times(sent) if t == "app.events"]
    span = (max(app) - min(app)).total_seconds()
    rate = len(app) / span
    print("measured app-event rate %.2f/s over %.1fs" % (rate, span))
    assert abs(rate - RATE) / RATE <= 0.25


def test_total_span_including_order_tail_is_short(live_run):
    sent, wall = live_run
    ts = [et for _, et, _ in _times(sent)]
    span = (max(ts) - min(ts)).total_seconds()
    print("total event_time span %.1fs, fake wall clock %.1fs" % (span, wall))
    assert span <= 150
    assert wall <= 150


def test_order_events_keep_order_and_never_go_back_in_time(live_run):
    sent, _ = live_run
    by_order = {}
    for topic, _, p in sent:  # sent order == emission order
        if "order_id" in p and topic != "app.events":
            by_order.setdefault(p["order_id"], []).append(datetime.fromisoformat(p["event_time"]))
    assert by_order
    for times in by_order.values():
        assert times == sorted(times)


def test_order_status_sequence_preserved(live_run, tmp_path):
    sent, _ = live_run
    seq = {}
    for topic, _, p in sent:
        if "order_id" in p and topic != "app.events":
            seq.setdefault(p["order_id"], []).append(p["event_type"])
    assert all(v[0] == "order_created" for v in seq.values() if "order_created" in v)


def test_deterministic_for_fixed_seed(tmp_path):
    a, wa = run_live(tmp_path, events=600)
    b, wb = run_live(tmp_path, events=600)
    assert a == b and wa == wb


def test_max_tail_caps_lifecycle(tmp_path):
    sent, wall = run_live(tmp_path, events=1500, max_tail=5.0, time_scale=1.0)
    created = {p["order_id"]: datetime.fromisoformat(p["event_time"])
               for _, _, p in sent if p.get("event_type") == "order_created"}
    for _, _, p in sent:
        if "order_id" in p and p["order_id"] in created and p.get("event_type") != "order_created":
            assert (datetime.fromisoformat(p["event_time"]) - created[p["order_id"]]).total_seconds() <= 5.0 + 1e-6


def test_avg_events_per_session_constant_is_measured():
    import random
    from simulator.catalog import make_catalog, make_customers
    b = rs.build_batch(random.Random(3), make_customers(3, 2000), make_catalog(3), START, sessions=1500)
    measured = len(b) / 1500
    print("measured events per session %.2f" % measured)
    assert abs(rs.AVG_EVENTS_PER_SESSION - measured) / measured <= 0.2
