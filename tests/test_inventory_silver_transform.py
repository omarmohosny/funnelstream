import json
from datetime import datetime, timezone

from streaming.silver.schemas import TABLE_COLUMNS
from streaming.silver.transform import transform_message

NOW = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)
KAFKA_TS = datetime(2026, 10, 10, 11, 0, 2, tzinfo=timezone.utc)


def raw(event):
    return json.dumps(event)


def run(event):
    return transform_message(raw(event), "inventory.events", KAFKA_TS, NOW, now=NOW)


SNAP = {"event_id": "evt_s1", "event_type": "stock_snapshot", "event_version": 1,
        "event_time": "2026-10-10T11:00:00+00:00", "store_id": "store_cairo_maadi", "product_id": "prod_0001",
        "payload": {"on_hand": 40, "reorder_point": 10}}
MOVE = {"event_id": "evt_m1", "event_type": "stock_movement", "event_version": 1,
        "event_time": "2026-10-10T11:00:05+00:00", "store_id": "store_cairo_maadi", "product_id": "prod_0001",
        "payload": {"qty_delta": -2, "reason": "sold", "order_id": "ord_1", "requested_qty": 3}}


def test_the_new_tables_have_their_columns():
    assert TABLE_COLUMNS["silver_stock_snapshots"] == [
        "event_id", "store_id", "product_id", "on_hand", "reserved_qty", "reorder_point", "event_time",
        "kafka_timestamp", "event_date"]
    assert TABLE_COLUMNS["silver_stock_movements"] == [
        "event_id", "store_id", "product_id", "qty_delta", "reason", "order_id", "requested_qty", "event_time",
        "kafka_timestamp", "event_date"]


def test_a_snapshot_becomes_one_snapshot_row_with_reserved_defaulting_to_zero():
    [(table, row)] = run(SNAP)
    assert table == "silver_stock_snapshots"
    assert list(row) == TABLE_COLUMNS["silver_stock_snapshots"]
    assert (row["store_id"], row["product_id"], row["on_hand"], row["reserved_qty"], row["reorder_point"]) == \
        ("store_cairo_maadi", "prod_0001", 40, 0, 10)
    assert row["event_time"] == "2026-10-10T11:00:00+00:00" and row["kafka_timestamp"] == KAFKA_TS
    assert row["event_date"] == "2026-10-10"  # the Cairo date of 14:00 local time


def test_a_movement_becomes_one_movement_row():
    [(table, row)] = run(MOVE)
    assert table == "silver_stock_movements"
    assert list(row) == TABLE_COLUMNS["silver_stock_movements"]
    assert (row["qty_delta"], row["reason"], row["order_id"], row["requested_qty"]) == (-2, "sold", "ord_1", 3)


def test_the_cairo_date_follows_local_midnight():
    late = dict(SNAP, event_time="2026-10-09T22:30:00+00:00")  # 01:30 on the 10th in Cairo (before NOW, so not "future")
    [(_, row)] = run(late)
    assert row["event_date"] == "2026-10-10"


def test_an_invalid_stock_event_is_quarantined_not_parsed_as_an_order():
    bad = dict(MOVE, payload={"qty_delta": 0, "reason": "sold", "order_id": "ord_1"})
    [(table, row)] = run(bad)
    assert table == "silver_quarantine" and row["error_code"] == "BAD_VALUE" and row["source_topic"] == "inventory.events"


def test_a_flat_stock_event_is_quarantined():
    flat = {k: v for k, v in SNAP.items() if k != "payload"}
    [(table, row)] = run(flat)
    assert table == "silver_quarantine" and row["error_code"] == "MISSING_FIELD"
