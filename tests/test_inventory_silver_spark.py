"""Spark tests (Docker, RUN_SPARK_TESTS=1): stock events through process_batch."""
import json
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.spark

from streaming.jobs.silver_clean import process_batch  # noqa: E402
from test_silver_clean_spark import NOW, bronze_df, silver, silver_counts  # noqa: E402

T = datetime(2026, 10, 1, 11, 0, tzinfo=timezone.utc)  # before the module's NOW (4 Oct) so nothing is "future"


def snap(i, store, product, on_hand, minute=0):
    return {"event_id": "evt_s%d" % i, "event_type": "stock_snapshot", "event_version": 1,
            "event_time": T.replace(minute=minute).isoformat(), "store_id": store, "product_id": product,
            "payload": {"on_hand": on_hand, "reserved_qty": 0, "reorder_point": 10}}


def move(i, store, product, delta, reason="sold", minute=1):
    payload = {"qty_delta": delta, "reason": reason}
    if reason == "sold":
        payload.update(order_id="ord_%d" % i, requested_qty=-delta)
    return {"event_id": "evt_m%d" % i, "event_type": "stock_movement", "event_version": 1,
            "event_time": T.replace(minute=minute).isoformat(), "store_id": store, "product_id": product,
            "payload": payload}


def messages(events):
    kafka_ts = T.replace(tzinfo=None)
    return [("inventory.events", json.dumps(e), kafka_ts) for e in events]


EVENTS = [snap(1, "store_a", "prod_1", 40), snap(2, "store_a", "prod_2", 30), snap(3, "store_b", "prod_1", 50),
          move(1, "store_a", "prod_1", -2), move(2, "store_b", "prod_1", -1), move(3, "store_a", "prod_2", 60, "received")]


def test_stock_events_land_in_the_two_tables_and_report_their_pairs(spark, tmp_path):
    lake = str(tmp_path / "lake")
    stats = {"collect_minutes": True}
    counts = process_batch(spark, bronze_df(spark, messages(EVENTS)), lake, None, now=NOW, stats=stats)
    assert counts["silver_stock_snapshots"] == 3 and counts["silver_stock_movements"] == 3
    assert silver_counts(spark, lake)["silver_stock_movements"] == 3
    assert stats["stock_pairs"] == [("store_a", "prod_1"), ("store_a", "prod_2"), ("store_b", "prod_1")]
    row = silver(spark, lake, "silver_stock_movements").where("event_id = 'evt_m1'").first()
    assert (row["qty_delta"], row["reason"], row["order_id"], row["requested_qty"]) == (-2, "sold", "ord_1", 2)


def test_a_replay_and_a_duplicate_change_nothing(spark, tmp_path):
    lake = str(tmp_path / "lake")
    df = bronze_df(spark, messages(EVENTS + [EVENTS[3]]))  # one movement delivered twice in the same batch
    process_batch(spark, df, lake, None, now=NOW)
    first = silver_counts(spark, lake)
    process_batch(spark, df, lake, None, now=NOW)  # the batch replayed
    assert silver_counts(spark, lake) == first
    assert first["silver_stock_movements"] == 3


def test_a_bad_stock_event_goes_to_quarantine(spark, tmp_path):
    lake = str(tmp_path / "lake")
    bad = move(9, "store_a", "prod_1", 0)
    counts = process_batch(spark, bronze_df(spark, messages([EVENTS[0], bad])), lake, None, now=NOW)
    assert counts["silver_stock_snapshots"] == 1 and counts["silver_stock_movements"] == 0
    assert counts["silver_quarantine"] == 1
