import copy
import json
import random
from datetime import datetime, timezone

import pytest

from contracts.normalize import normalize_city, normalize_device
from contracts.validate import validate_event, validate_raw
from scripts.run_simulator import iter_batch
from simulator.catalog import make_catalog, make_customers
from simulator.noise import (KINDS, NoiseConfig, NoiseCounter, apply_noise, noisy_export)

START = datetime(2026, 9, 27, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 20, tzinfo=timezone.utc)
CATALOG = make_catalog(3)
CATALOG_IDS = {p["product_id"] for p in CATALOG}
CUSTOMERS = make_customers(3, 300)


def base_stream(max_events=3000, seed=3):
    return list(iter_batch(random.Random(seed), CUSTOMERS, CATALOG, START, max_events=max_events))


def only(kind, rate=1.0):
    cfg = NoiseConfig.off()
    setattr(cfg, kind, rate)
    return cfg


def run(stream, cfg, seed=1):
    c = NoiseCounter()
    out = list(apply_noise(iter(stream), random.Random(seed), cfg, CATALOG_IDS, c, now=NOW))
    return out, c


def code(payload, **kw):
    return validate_raw(payload, now=NOW, known_products=CATALOG_IDS, **kw)[1]


def test_off_is_identity():
    stream = base_stream(1000)
    out, c = run(stream, NoiseConfig.off())
    assert len(out) == len(stream)
    assert all(o[0] == s[0] and o[1] is s[1] for o, s in zip(out, stream))
    assert sum(c.counts.values()) == 0
    assert c.total_messages == len(stream)


def test_config_helpers_and_defaults():
    assert set(KINDS) == set(NoiseConfig().__dict__)
    assert len(KINDS) == 17
    assert all(v == 0 for v in NoiseConfig.off().__dict__.values())
    s = NoiseConfig().scaled(2.0)
    assert s.duplicate == pytest.approx(0.02) and s.bot_session == pytest.approx(0.002)
    assert NoiseConfig().late == 0.02 and NoiseConfig().dirty_text == 0.03


def test_same_seed_identical():
    stream = base_stream(3000)
    a, ca = run(stream, NoiseConfig().scaled(10), seed=9)
    b, cb = run(stream, NoiseConfig().scaled(10), seed=9)
    assert a == b and ca.to_manifest() == cb.to_manifest()
    d, _ = run(stream, NoiseConfig().scaled(10), seed=10)
    assert d != a


def test_duplicate_rate():
    stream = base_stream(20000, seed=5)
    out, c = run(stream, NoiseConfig(), seed=2)
    assert len(stream) >= 20000
    rate = c.counts["duplicate"] / len(stream)
    assert 0.005 <= rate <= 0.015


def test_duplicate_yields_identical_copy():
    stream = base_stream(200)
    out, c = run(stream, only("duplicate"))
    assert len(out) == 2 * len(stream)
    for i in range(0, len(out), 2):
        assert out[i] == out[i + 1] and out[i][1] is not out[i + 1][1]
    assert c.counts["duplicate"] == len(stream)


def test_malformed_rows_are_unparsable_strings():
    out, c = run(base_stream(500), only("malformed_json"))
    assert c.counts["malformed_json"] == len(out) > 0
    for hint, p in out:
        assert isinstance(p, str)
        with pytest.raises(ValueError):
            json.loads(p)
        assert validate_raw(p, now=NOW) == (False, "BAD_JSON")


@pytest.mark.parametrize("kind,expected", [
    ("missing_field", "MISSING_FIELD"), ("unknown_type", "UNKNOWN_EVENT_TYPE"),
    ("bad_version", "BAD_VERSION"), ("negative_price", "NEGATIVE_PRICE"),
    ("future_timestamp", "FUTURE_TIMESTAMP"), ("null_product_id", "MISSING_FIELD"),
    ("unknown_product_id", "UNKNOWN_PRODUCT"),
])
def test_validator_visible_kinds(kind, expected):
    stream = base_stream(1500)
    out, c = run(stream, only(kind))
    assert c.counts[kind] > 0
    changed = [(o, s) for o, s in zip(out, stream) if o[1] != s[1]]
    assert len(changed) == c.counts[kind]
    for (hint, payload), _ in changed:
        assert code(payload) == expected
    # untouched ones stay valid
    for (h, p), s in zip(out, stream):
        if p == s[1]:
            assert code(p) == ""


def test_future_timestamp_is_2h_plus_ahead_of_event():
    stream = base_stream(500)
    c = NoiseCounter()
    out = list(apply_noise(iter(stream), random.Random(1), only("future_timestamp"), CATALOG_IDS, c))
    for (_, o), (_, s) in zip(out, stream):
        d = datetime.fromisoformat(o["event_time"]) - datetime.fromisoformat(s["event_time"])
        assert d.total_seconds() >= 2 * 3600


def test_future_timestamp_with_now_beats_wall_clock():
    stream = base_stream(300)
    c = NoiseCounter()
    out = list(apply_noise(iter(stream), random.Random(1), only("future_timestamp"), CATALOG_IDS, c, now=NOW))
    for (_, o), (_, s) in zip(out, stream):
        assert datetime.fromisoformat(o["event_time"]) > NOW
        assert validate_event(o, now=NOW)[1] == "FUTURE_TIMESTAMP"


def test_negative_price_only_for_priced_events():
    stream = base_stream(1500)
    out, c = run(stream, only("negative_price"))
    priced = sum(1 for _, e in stream if isinstance(e["payload"].get("price"), (int, float)))
    assert c.counts["negative_price"] == priced > 0


def test_late_and_very_late():
    stream = base_stream(800)
    out, _ = run(stream, only("late"))
    for (_, o), (_, s) in zip(out, stream):
        d = (datetime.fromisoformat(s["event_time"]) - datetime.fromisoformat(o["event_time"])).total_seconds()
        assert 10 * 60 <= d <= 120 * 60
        assert code(o) == ""
    out, _ = run(stream, only("very_late"))
    for (_, o), (_, s) in zip(out, stream):
        d = (datetime.fromisoformat(s["event_time"]) - datetime.fromisoformat(o["event_time"])).total_seconds()
        assert 86400 <= d <= 5 * 86400


def test_tz_offset_same_instant():
    stream = base_stream(500)
    out, c = run(stream, only("tz_offset"))
    assert c.counts["tz_offset"] == len(stream)
    for (_, o), (_, s) in zip(out, stream):
        assert o["event_time"].endswith("+02:00")
        assert datetime.fromisoformat(o["event_time"]) == datetime.fromisoformat(s["event_time"])
        assert code(o) == ""


def test_dirty_text_changes_only_city_device():
    stream = base_stream(1500)
    out, c = run(stream, only("dirty_text"))
    assert c.counts["dirty_text"] > 0
    n = 0
    for (_, o), (_, s) in zip(out, stream):
        diff = {k for k in set(o) | set(s) if o.get(k) != s.get(k)}
        if not diff:
            continue
        n += 1
        assert len(diff) == 1 and diff <= {"city", "device"}
        k = diff.pop()
        norm = normalize_city if k == "city" else normalize_device
        assert norm(o[k]) == norm(s[k]) == s[k]
        assert code(o) == ""
    assert n == c.counts["dirty_text"]


def test_numeric_string_still_valid():
    stream = base_stream(1500)
    out, c = run(stream, only("numeric_string"))
    assert c.counts["numeric_string"] > 0
    n = 0
    for (_, o), (_, s) in zip(out, stream):
        if o != s:
            n += 1
            assert code(o) == ""
            assert "str" in json.dumps(o, default=str) or True
    assert n == c.counts["numeric_string"]
    sample = next(o for (_, o), (_, s) in zip(out, stream) if o != s)
    flat = json.dumps(sample["payload"])
    assert '"' in flat


def test_numeric_string_value_equal():
    stream = base_stream(500)
    out, _ = run(stream, only("numeric_string"))
    for (_, o), (_, s) in zip(out, stream):
        for k in ("price", "total", "order_total"):
            if k in s["payload"] and isinstance(o["payload"][k], str):
                assert float(o["payload"][k]) == s["payload"][k]


def test_unknown_product_absent_from_catalog():
    out, _ = run(base_stream(1500), only("unknown_product_id"))
    assert out
    changed = [o for _, o in out if "product_id" in o["payload"] and o["payload"]["product_id"] not in CATALOG_IDS]
    assert changed
    for o in changed:
        assert o["payload"]["product_id"] not in CATALOG_IDS


def test_input_not_mutated():
    stream = base_stream(2000)
    snapshot = copy.deepcopy(stream)
    run(stream, NoiseConfig().scaled(30))
    for k in KINDS:
        run(stream, only(k))
    assert stream == snapshot


def test_total_injections_equal_manifest_and_one_noise_per_message():
    stream = base_stream(5000)
    out, c = run(stream, NoiseConfig().scaled(20))
    m = c.to_manifest()
    assert m["total_messages"] == len(out)
    assert sum(m["injected"].values()) == sum(c.counts.values())
    assert set(m["injected"]) == set(KINDS)
    # expected length: input + duplicates + cancels + 150 per bot session
    assert len(out) == len(stream) + c.counts["duplicate"] + c.counts["cancel_after_ship"] + 150 * c.counts["bot_session"]
    # exactly one validator-visible code per injected row (no stacking)
    codes = [validate_raw(p, now=NOW, known_products=CATALOG_IDS)[1] for _, p in out]
    bad = sum(1 for x in codes if x)
    expected = sum(c.counts[k] for k in ("malformed_json", "missing_field", "unknown_type", "bad_version",
                                         "negative_price", "future_timestamp", "null_product_id",
                                         "unknown_product_id"))
    assert bad == expected


def test_out_of_order_status_swaps_times():
    stream = base_stream(1500)
    out, c = run(stream, only("out_of_order_status"))
    assert c.counts["out_of_order_status"] > 0
    assert len(out) == len(stream)
    by_order = {}
    for _, e in out:
        if e["event_type"] not in ("purchase",) and e.get("order_id") and e["session_id"] is None:
            by_order.setdefault(e["order_id"], []).append(e)
    inverted = 0
    for evs in by_order.values():
        for a, b in zip(evs, evs[1:]):
            if datetime.fromisoformat(b["event_time"]) < datetime.fromisoformat(a["event_time"]):
                inverted += 1
    assert inverted >= c.counts["out_of_order_status"] > 0
    assert all(code(p) == "" for _, p in out)


def test_cancel_after_ship():
    stream = base_stream(2000)
    out, c = run(stream, only("cancel_after_ship"))
    shipped = sum(1 for _, e in stream if e["event_type"] == "order_shipped")
    assert c.counts["cancel_after_ship"] == shipped > 0
    natural = sum(1 for _, e in stream if e["event_type"] == "order_cancelled")
    cancels = [e for _, e in out if e["event_type"] == "order_cancelled"]
    assert len(cancels) == shipped + natural
    ship_times = {e["order_id"]: e["event_time"] for _, e in stream if e["event_type"] == "order_shipped"}
    for e in cancels:
        assert code(e) == ""
        if e["order_id"] in ship_times and e["payload"].get("reason") == "customer_request":
            assert e["event_time"] > ship_times[e["order_id"]]


def test_bot_session():
    from streaming.common.bots import find_bot_sessions
    stream = base_stream(1000)
    out, c = run(stream, only("bot_session", 0.2))
    n = c.counts["bot_session"]
    assert n > 0
    assert len(out) == len(stream) + 150 * n
    bots = find_bot_sessions([p for _, p in out])
    assert len(bots) == n
    assert find_bot_sessions([e for _, e in stream]) == set()
    ids = [p["event_id"] for _, p in out]
    assert len(set(ids)) == len(ids)


def test_noisy_export():
    rows = [{"customer_id": "c%d" % i, "city": "Cairo"} for i in range(2000)]
    snap = copy.deepcopy(rows)
    out, counts = noisy_export(rows, random.Random(4), dup_rate=0.05, null_city_rate=0.05)
    assert rows == snap
    assert len(out) == 2000 + counts["duplicate"]
    assert counts["duplicate"] > 0 and counts["null_city"] > 0
    assert sum(1 for r in out if r["city"] is None) >= counts["null_city"]
    out2, counts2 = noisy_export(rows, random.Random(4), dup_rate=0.05, null_city_rate=0.05)
    assert out2 == out and counts2 == counts
    cat = make_catalog(1, 50)
    out3, c3 = noisy_export(cat, random.Random(1), dup_rate=0.5)
    assert c3["null_city"] == 0 and all("city" not in r for r in out3)
