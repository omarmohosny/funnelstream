"""Amendment C, step C2: platform in Silver, platform_conversions in the campaign mirror, order_returned."""
import json
import random
from datetime import datetime, timedelta, timezone

from scripts.run_simulator import iter_batch
from simulator.catalog import make_catalog, make_customers
from simulator.orders import make_order
from streaming.common.status import STATUS_RANK
from streaming.silver.orders import split_cancel_after_ship
from streaming.silver.schemas import SILVER_EVENTS_COLUMNS, SILVER_ORDERS_COLUMNS
from streaming.silver.transform import transform_message
from warehouse.publish import PUBLISH_ALLOWLIST
from warehouse.tables import SILVER_MIRROR_TABLES

START = datetime(2026, 9, 27, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 20, tzinfo=timezone.utc)
RECEIVED = datetime(2026, 10, 20, 1, tzinfo=timezone.utc)
CATALOG = make_catalog(3)
CATALOG_IDS = {p["product_id"] for p in CATALOG}
CUSTOMERS = make_customers(3, 300)


def tm(e):
    k = datetime.fromisoformat(e["event_time"]) + timedelta(seconds=1)
    return transform_message(json.dumps(e), "app.events", k, RECEIVED, known_products=CATALOG_IDS, now=NOW)


def stream(n=1500):
    return list(iter_batch(random.Random(3), CUSTOMERS, CATALOG, START, max_events=n))


def _after(cols, a, b):
    return cols.index(b) == cols.index(a) + 1


def test_platform_column_placed_after_channel_everywhere():
    assert _after(SILVER_EVENTS_COLUMNS, "channel", "platform")
    assert _after(SILVER_ORDERS_COLUMNS, "channel", "platform")
    for t in ("silver_events", "silver_orders"):
        mirror = next(x for x in SILVER_MIRROR_TABLES if x.name == t)
        assert _after(mirror.column_names(), "channel", "platform") and mirror.column("platform").nullable
        assert _after(PUBLISH_ALLOWLIST[t], "channel", "platform")


def test_campaign_snapshot_platform_conversions_after_clicks():
    snap = next(x for x in SILVER_MIRROR_TABLES if x.name == "silver_campaign_snapshots")
    assert _after(snap.column_names(), "clicks", "platform_conversions")
    assert snap.column("platform_conversions").type == "bigint" and snap.column("platform_conversions").nullable
    assert _after(PUBLISH_ALLOWLIST["silver_campaign_snapshots"], "clicks", "platform_conversions")


def test_app_event_platform_flattened_and_old_format_is_null():
    seen = set()
    for kind, e in stream():
        if kind != "app":
            continue
        (table, row), = tm(e)
        assert table == "silver_events" and row["platform"] == e.get("platform")
        seen.add(row["platform"])
        old = {k: v for k, v in e.items() if k != "platform"}
        assert tm(old)[0][1]["platform"] is None
    assert seen == {"app", "web"}


def test_order_created_platform_from_payload_and_missing_is_null():
    n = 0
    for kind, e in stream(4000):
        if kind == "order" and e["event_type"] == "order_created":
            n += 1
            rows = dict(tm(e))
            assert rows["silver_orders"]["platform"] == e["payload"].get("platform") in ("app", "web")
            e2 = json.loads(json.dumps(e))
            del e2["payload"]["platform"]
            assert dict(tm(e2))["silver_orders"]["platform"] is None
            e3 = json.loads(json.dumps(e))
            e3["payload"]["platform"] = "tv"
            assert dict(tm(e3))["silver_orders"]["platform"] is None
    assert n > 5


def test_invalid_top_level_platform_is_quarantined_bad_value():
    e = next(e for k, e in stream() if k == "app")
    out = tm({**e, "platform": "tv"})
    assert [t for t, _ in out] == ["silver_quarantine"] and out[0][1]["error_code"] == "BAD_VALUE"


def _returned_event():
    rng = random.Random(1)
    cust = CUSTOMERS[0]
    for i in range(400):
        for topic, e in make_order(rng, cust, CATALOG, None, START + timedelta(days=i % 5), "ord_%d" % i):
            if e["event_type"] == "order_returned":
                return e
    raise AssertionError("no returned order in 400 simulated orders")


def test_order_returned_is_one_status_row_rank_7_with_reason_and_cairo_date():
    e = _returned_event()
    e["event_time"] = "2026-10-02T22:30:00+00:00"  # 01:30 on 3 Oct in Cairo (UTC+3 in October)
    out = transform_message(json.dumps(e), "orders", NOW - timedelta(days=1), RECEIVED, now=NOW)
    assert [t for t, _ in out] == ["silver_order_status"]
    row = out[0][1]
    assert row["status"] == "order_returned" and row["status_rank"] == STATUS_RANK["order_returned"] == 7
    assert row["reason"] == e["payload"]["reason"] == "all_attempts_failed"
    assert row["order_id"] == e["order_id"]
    assert row["event_date"] == "2026-10-03"


def test_split_cancel_after_ship_leaves_returned_alone():
    def r(i, status, h):
        return {"event_id": "e%d" % i, "order_id": "o1", "status": status,
                "status_time": (START + timedelta(hours=h)).isoformat()}
    rows = [r(1, "order_shipped", 1), r(2, "delivery_failed", 2), r(3, "order_returned", 3)]
    kept, quarantined = split_cancel_after_ship(rows)
    assert kept == rows and quarantined == []
