import copy
import json
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from contracts.normalize import CANONICAL_CITIES, normalize_city, normalize_device
from contracts.validate import APP_EVENT_TYPES
from scripts.run_simulator import iter_batch
from simulator.campaigns import CAMPAIGNS
from simulator.catalog import make_catalog, make_customers
from simulator.core import journey
from simulator.noise import NoiseConfig, NoiseCounter, apply_noise
from simulator.orders import make_order
from streaming.common.status import STATUS_RANK
from streaming.silver.schemas import ERROR_CODES, TABLE_COLUMNS
from streaming.silver.transform import transform_message

START = datetime(2026, 9, 27, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 20, tzinfo=timezone.utc)
CATALOG = make_catalog(3)
CATALOG_IDS = {p["product_id"] for p in CATALOG}
CUSTOMERS = make_customers(3, 300)
RECEIVED = datetime(2026, 10, 20, 1, tzinfo=timezone.utc)


def stream(max_events=1500, seed=3):
    return list(iter_batch(random.Random(seed), CUSTOMERS, CATALOG, START, max_events=max_events))


def kts(e):
    return datetime.fromisoformat(e["event_time"]) + timedelta(seconds=1)


def tm(e, topic="app.events", now=NOW, known=CATALOG_IDS, kafka=None):
    raw = e if isinstance(e, str) else json.dumps(e)
    k = kafka or (kts(e) if isinstance(e, dict) else START)
    return transform_message(raw, topic, k, RECEIVED, known_products=known, now=now)


def noisy(kind, seed=1, max_events=1500):
    cfg = NoiseConfig.off()
    setattr(cfg, kind, 1.0)
    c = NoiseCounter()
    base = stream(max_events)
    out = list(apply_noise(iter(base), random.Random(seed), cfg, CATALOG_IDS, c, now=NOW))
    return base, out, c


def codes(rows):
    return [r["error_code"] for t, r in rows if t == "silver_quarantine"]


# ---- group 1 ----
def test_every_app_event_type_yields_one_ordered_silver_events_row():
    rng = random.Random(5)
    seen = set()
    for i in range(80):
        cust = CUSTOMERS[i] if i % 5 else None
        camp = CAMPAIGNS[i % 3] if i % 2 else None
        for e in journey(rng, cust, CATALOG, camp, START):
            rows = tm(e)
            assert len(rows) == 1 and rows[0][0] == "silver_events", (e, rows)
            row = rows[0][1]
            assert list(row) == TABLE_COLUMNS["silver_events"]
            assert row["event_time"].endswith("+00:00")
            assert row["is_bot"] is False and row["campaign_id"] is None
            seen.add(e["event_type"])
    # the simulator never emits these two; build them from a journey event
    base = journey(rng, CUSTOMERS[0], CATALOG, None, START)[0]
    for et, payload in (("product_search", {"search_term": "dress"}),
                        ("remove_from_cart", {"product_id": CATALOG[0]["product_id"], "cart_id": "cart_1"})):
        e = dict(base, event_type=et, payload=payload)
        rows = tm(e)
        assert len(rows) == 1 and rows[0][0] == "silver_events"
        assert list(rows[0][1]) == TABLE_COLUMNS["silver_events"]
        seen.add(et)
    assert seen == APP_EVENT_TYPES


def test_flattened_payload_fields_and_types():
    rng = random.Random(6)
    evs = [e for _ in range(60) for e in journey(rng, CUSTOMERS[0], CATALOG, CAMPAIGNS[0], START)]
    pe = next(e for e in evs if e["event_type"] == "product_view_end")
    row = tm(pe)[0][1]
    assert isinstance(row["dwell_seconds"], int) and row["dwell_seconds"] == pe["payload"]["dwell_seconds"]
    assert row["product_id"] == pe["payload"]["product_id"]
    pv = next(e for e in evs if e["event_type"] == "product_view" and "utm_campaign" in e["payload"])
    row = tm(pv)[0][1]
    assert row["utm_campaign"] == pv["payload"]["utm_campaign"]
    purchase = next(e for e in evs if e["event_type"] == "purchase")
    assert tm(purchase)[0][1]["order_id"] == purchase["payload"]["order_id"]
    ev = copy.deepcopy(pv)
    ev.pop("country")
    assert tm(ev)[0][1]["country"] == "EG"


def test_event_date_is_cairo_local():
    e = journey(random.Random(1), CUSTOMERS[0], CATALOG, None, START)[0]
    e["event_time"] = "2026-10-04T23:30:00+00:00"
    row = tm(e)[0][1]
    assert row["event_time"] == "2026-10-04T23:30:00+00:00"
    assert row["event_date"] == "2026-10-05"
    e["event_time"] = "2026-10-04T12:00:00Z"
    assert tm(e)[0][1]["event_date"] == "2026-10-04"


# ---- group 2 ----
def test_late_flag_boundaries():
    e = journey(random.Random(1), CUSTOMERS[0], CATALOG, None, START)[0]
    t = datetime.fromisoformat(e["event_time"])
    assert tm(e, kafka=t + timedelta(minutes=11))[0][1]["is_late"] is True
    assert tm(e, kafka=t + timedelta(minutes=9))[0][1]["is_late"] is False
    assert tm(e, kafka=t + timedelta(seconds=600))[0][1]["is_late"] is False


# ---- group 3 ----
@pytest.mark.parametrize("kind,code", [
    ("malformed_json", "BAD_JSON"), ("missing_field", "MISSING_FIELD"),
    ("unknown_type", "UNKNOWN_EVENT_TYPE"), ("bad_version", "BAD_VERSION"),
    ("negative_price", "NEGATIVE_PRICE"), ("future_timestamp", "FUTURE_TIMESTAMP"),
    ("null_product_id", "MISSING_FIELD"), ("unknown_product_id", "UNKNOWN_PRODUCT"),
])
def test_quarantine_noise_kinds(kind, code):
    base, out, c = noisy(kind)
    assert c.counts[kind] > 0
    got = []
    for hint, payload in out:
        raw = payload if isinstance(payload, str) else json.dumps(payload)
        rows = transform_message(raw, "app.events", START, RECEIVED, known_products=CATALOG_IDS, now=NOW)
        got.append(rows)
    q = [r for rows in got for t, r in rows if t == "silver_quarantine"]
    assert len(q) == c.counts[kind]
    assert {r["error_code"] for r in q} == {code}
    assert all(r["error_detail"] and r["quarantine_id"] and r["received_at"] == RECEIVED for r in q)
    # everything not touched by this noise kind passes
    assert all(len(rows) >= 1 for rows in got)


@pytest.mark.parametrize("kind", ["late", "very_late"])
def test_late_noise_kept_and_flagged(kind):
    base, out, c = noisy(kind)
    n = 0
    for (hint, orig), (_, e) in zip(base, out):
        rows = transform_message(json.dumps(e), "app.events", datetime.fromisoformat(orig["event_time"]) +
                                 timedelta(seconds=1), RECEIVED, known_products=CATALOG_IDS, now=NOW)
        assert "silver_quarantine" not in [t for t, _ in rows]
        if e["event_type"] in APP_EVENT_TYPES:
            assert rows[0][1]["is_late"] is True
            n += 1
    assert n > 0 and c.counts[kind] == len(base)


def test_tz_offset_normalised_same_instant():
    base, out, c = noisy("tz_offset")
    assert c.counts["tz_offset"] == len(base)
    checked = 0
    for (hint, orig), (_, e) in zip(base, out):
        assert e["event_time"].endswith("+02:00")
        rows = tm(e)
        assert "silver_quarantine" not in [t for t, _ in rows]
        for t, r in rows:
            if t == "silver_events":
                assert r["event_time"].endswith("+00:00")
                assert datetime.fromisoformat(r["event_time"]) == datetime.fromisoformat(orig["event_time"])
                checked += 1
    assert checked > 0


def test_dirty_text_canonical():
    base, out, c = noisy("dirty_text")
    assert c.counts["dirty_text"] > 0
    checked = 0
    for (hint, orig), (_, e) in zip(base, out):
        rows = tm(e)
        assert "silver_quarantine" not in [t for t, _ in rows]
        for t, r in rows:
            if t == "silver_events":
                assert r["city"] == normalize_city(orig["city"]) and r["city"] in CANONICAL_CITIES
                assert r["device"] == normalize_device(orig["device"]) and r["device"] in ("mobile", "desktop", "tablet")
                checked += 1
            if t == "silver_orders":
                assert r["city"] == normalize_city(orig["city"])
    assert checked > 0


def test_numeric_string_kept_and_money_is_decimal():
    base, out, c = noisy("numeric_string")
    assert c.counts["numeric_string"] > 0
    order_checked = 0
    for (hint, orig), (_, e) in zip(base, out):
        rows = tm(e)
        assert "silver_quarantine" not in [t for t, _ in rows]
        for t, r in rows:
            if t == "silver_orders":
                assert isinstance(r["order_total"], Decimal)
                assert r["order_total"] == Decimal(str(orig["payload"]["order_total"])).quantize(Decimal("0.01"))
                order_checked += 1
            if t == "silver_order_items":
                assert isinstance(r["unit_price"], Decimal) and isinstance(r["line_total"], Decimal)
    assert order_checked > 0


def test_duplicate_messages_give_identical_rows():
    base, out, c = noisy("duplicate", max_events=300)
    assert c.counts["duplicate"] == len(base) and len(out) == 2 * len(base)
    for i in range(0, len(out), 2):
        assert out[i][1] == out[i + 1][1]
        assert tm(out[i][1]) == tm(out[i + 1][1])


# ---- group 4 ----
def _orders():
    rng = random.Random(9)
    res = []
    for i in range(120):
        camp = CAMPAIGNS[0] if i % 2 else None
        evs = make_order(rng, CUSTOMERS[i % 300], CATALOG, camp, START + timedelta(hours=i), f"ord_{i:04d}")
        for _, e in evs:
            if e["event_type"] == "order_created":
                e["payload"]["session_id"] = f"sess_{i:04d}"
                e["payload"]["click_id"] = "clk_x" if camp else None
        res.append(evs)
    return res


def test_order_created_rows_match_source():
    n = 0
    for evs in _orders():
        _, e = evs[0]
        rows = tm(e, "orders")
        tables = [t for t, _ in rows]
        assert tables.count("silver_orders") == 1 and tables.count("silver_order_status") == 1
        items = [r for t, r in rows if t == "silver_order_items"]
        assert len(items) == len(e["payload"]["items"])
        order = next(r for t, r in rows if t == "silver_orders")
        status = next(r for t, r in rows if t == "silver_order_status")
        assert order["order_total"] == Decimal(str(e["payload"]["order_total"])).quantize(Decimal("0.01"))
        assert isinstance(order["order_total"], Decimal)
        assert order["session_id"] == e["payload"]["session_id"] and order["channel"] == e["payload"]["channel"]
        assert order["payment_method"] == e["payload"]["payment_method"]
        assert order["order_created_at"].endswith("+00:00") and order["country"] == "EG"
        assert status["status"] == "order_created" and status["status_rank"] == 1
        assert status["event_id"] == e["event_id"] and status["payment_method"] == e["payload"]["payment_method"]
        for i, (src, it) in enumerate(zip(e["payload"]["items"], items), start=1):
            assert it["line_no"] == i and it["currency"] == "EGP" and isinstance(it["quantity"], int)
            assert it["unit_price"] == Decimal(str(src["unit_price"])).quantize(Decimal("0.01"))
            assert it["line_total"] == Decimal(str(src["line_total"])).quantize(Decimal("0.01"))
            assert it["order_time"] == order["order_created_at"] and it["product_id"] == src["product_id"]
        n += 1
    assert n == 120


def test_bad_line_total_quarantines_whole_message():
    e = copy.deepcopy(_orders()[0][0][1])
    e["payload"]["items"][0]["line_total"] = e["payload"]["items"][0]["line_total"] + 5
    rows = tm(e, "orders")
    assert len(rows) == 1 and rows[0][0] == "silver_quarantine"
    assert rows[0][1]["error_code"] == "BAD_VALUE"
    assert rows[0][1]["error_detail"] == "line_total does not match quantity x unit_price"


def test_other_statuses_one_row_with_rank_carrier_reason():
    seen = {}
    for evs in _orders():
        for topic, e in evs[1:]:
            rows = tm(e, topic)
            assert len(rows) == 1 and rows[0][0] == "silver_order_status"
            r = rows[0][1]
            et = e["event_type"]
            assert r["status"] == et and r["status_rank"] == STATUS_RANK[et]
            assert r["order_id"] == e["order_id"] and r["customer_id"] == e["customer_id"]
            if et in ("order_shipped", "out_for_delivery", "delivered"):
                assert r["carrier"] == e["payload"]["carrier"]
            else:
                assert r["carrier"] is None
            if et in ("order_cancelled", "delivery_failed", "order_returned"):
                assert r["reason"] == e["payload"]["reason"]
            else:
                assert r["reason"] is None
            seen[et] = r
    assert {"order_shipped", "delivered", "delivery_failed", "order_completed", "order_cancelled"} <= set(seen)


# ---- group 5 ----
def _mutate(rng, e):
    e = copy.deepcopy(e)
    junk = [None, 7, -3, 1.5, [1, 2], {"a": 1}, "x" * 100000, "\U0001F600\U0001F4A5", "", True, "2026-13-45", float("nan")]
    for _ in range(rng.randint(1, 4)):
        target = e
        if isinstance(e.get("payload"), dict) and rng.random() < 0.5:
            target = e["payload"]
        if rng.random() < 0.4 and isinstance(target, dict) and target:
            target.pop(rng.choice(list(target)), None)
        elif isinstance(target, dict) and target:
            k = rng.choice(list(target))
            if k == "items" and isinstance(target[k], list) and target[k] and rng.random() < 0.7:
                it = target[k][0]
                if isinstance(it, dict) and it:
                    it[rng.choice(list(it))] = rng.choice(junk)
                    continue
            target[k] = rng.choice(junk)
    return e


def test_fuzz_never_raises():
    rng = random.Random(77)
    base = [e for _, e in stream(400)]
    base += [e for evs in _orders() for _, e in evs]
    kept = quarantined = 0
    for _ in range(500):
        e = _mutate(rng, rng.choice(base))
        raw = json.dumps(e)
        rows = transform_message(raw, "app.events", START, RECEIVED, known_products=CATALOG_IDS, now=NOW)
        assert rows
        for table, row in rows:
            assert list(row) == TABLE_COLUMNS[table]
            if table == "silver_quarantine":
                assert row["error_code"] in ERROR_CODES
                quarantined += 1
            else:
                kept += 1
    assert kept > 0 and quarantined > 0
    # junk that is not even a JSON object
    for raw in ["", "[]", "null", "3", "\"s\"", "{", "\ud800", None, b"\xff\xfe"]:
        rows = transform_message(raw, "orders", START, RECEIVED, now=NOW)
        assert codes(rows) and codes(rows)[0] in ERROR_CODES


# ---- group 6 ----
def test_quarantine_ids_deterministic():
    a = transform_message("{bad", "app.events", START, RECEIVED, now=NOW)[0][1]
    b = transform_message("{bad", "app.events", START, RECEIVED, now=NOW)[0][1]
    c = transform_message("{bad2", "app.events", START, RECEIVED, now=NOW)[0][1]
    d = transform_message("{bad", "orders", START, RECEIVED, now=NOW)[0][1]
    assert a["quarantine_id"] == b["quarantine_id"]
    assert len({a["quarantine_id"], c["quarantine_id"], d["quarantine_id"]}) == 3
    import hashlib
    assert a["quarantine_id"] == hashlib.sha1(b"app.events\n{bad").hexdigest()
    assert a["raw_payload"] == "{bad" and a["error_code"] == "BAD_JSON"


# ---- group 8 ----
def test_column_sets_on_mixed_stream():
    cfg = NoiseConfig().scaled(20)
    c = NoiseCounter()
    out = list(apply_noise(iter(stream(2500, seed=11)), random.Random(4), cfg, CATALOG_IDS, c, now=NOW))
    tables = set()
    for hint, payload in out:
        raw = payload if isinstance(payload, str) else json.dumps(payload)
        for table, row in transform_message(raw, "mixed", START, RECEIVED, known_products=CATALOG_IDS, now=NOW):
            assert list(row) == TABLE_COLUMNS[table]
            tables.add(table)
    assert tables == set(TABLE_COLUMNS)


# ---- empty-items ruling ----
def _created_event():
    for hint, e in stream(800):
        if e["event_type"] == "order_created":
            return e
    raise AssertionError("no order_created in stream")


@pytest.mark.parametrize("mode", ["missing", "none", "empty"])
def test_order_created_without_items_is_quarantined_as_bad_value(mode):
    e = copy.deepcopy(_created_event())
    if mode == "missing":
        del e["payload"]["items"]
    elif mode == "none":
        e["payload"]["items"] = None
    else:
        e["payload"]["items"] = []
    rows = tm(e)
    assert [t for t, _ in rows] == ["silver_quarantine"]
    assert rows[0][1]["error_code"] == "BAD_VALUE"
    assert rows[0][1]["error_detail"] == "order has no items"
