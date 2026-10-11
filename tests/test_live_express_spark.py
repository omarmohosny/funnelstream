"""Spark tests (Docker, RUN_SPARK_TESTS=1): the live express SLA rows."""
from datetime import datetime, timedelta, timezone

import pytest

pytestmark = pytest.mark.spark

from test_live_stock_spark import write  # noqa: E402
from warehouse import live_express  # noqa: E402

T0 = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
SINCE = T0 - timedelta(hours=3)


def express(i, minute, store="store_cairo_maadi", promised=20):
    t = T0 + timedelta(minutes=minute)
    return {"order_id": "ord_%d" % i, "store_id": store, "promised_minutes": promised, "created_at": t,
            "event_date": t.date()}


def status(i, name, minute, rank, n=1):
    t = T0 + timedelta(minutes=minute)
    return {"event_id": "st_%d_%s_%d" % (i, name, n), "order_id": "ord_%d" % i, "customer_id": "c1", "status": name,
            "status_rank": rank, "status_time": t, "payment_method": "card", "carrier": None, "reason": None,
            "event_date": t.date()}


def rows(spark, lake):
    return {r["order_id"]: r for r in live_express.compute_express_rows(spark, lake, SINCE)}


def test_orders_before_since_are_excluded_and_stores_and_promise_are_carried(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_express_orders", [
        express(1, 0), express(2, 1, store="store_giza_dokki", promised=2), express(3, -4 * 60)])
    write(spark, lake, "silver_order_status", [status(1, "order_created", 0, 1)])
    got = rows(spark, lake)
    assert set(got) == {"ord_1", "ord_2"}
    assert (got["ord_2"]["store_id"], got["ord_2"]["promised_minutes"]) == ("store_giza_dokki", 2)
    assert got["ord_1"]["created_at"] == T0 and got["ord_1"]["delivered_at"] is None
    assert got["ord_1"]["created_at"].tzinfo is not None  # epoch seconds through _utc: an aware UTC datetime
    assert got["ord_1"]["closed_without_delivery"] is False


def test_the_earliest_delivered_time_wins_and_a_duplicate_row_changes_nothing(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_express_orders", [express(1, 0)])
    write(spark, lake, "silver_order_status", [
        status(1, "order_created", 0, 1), status(1, "delivered", 14, 6),
        status(1, "delivered", 19, 6, n=2)])  # a replayed or duplicated delivered row, later
    got = rows(spark, lake)["ord_1"]["delivered_at"]
    assert got == T0 + timedelta(minutes=14) and got.tzinfo is not None


def test_a_cancelled_or_returned_order_without_delivery_is_closed(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_express_orders", [express(1, 0), express(2, 0), express(3, 0)])
    write(spark, lake, "silver_order_status", [
        status(1, "order_created", 0, 1), status(1, "order_cancelled", 3, 2),
        status(2, "order_created", 0, 1), status(2, "order_returned", 5, 7),
        status(3, "order_created", 0, 1)])
    got = rows(spark, lake)
    assert [got[k]["closed_without_delivery"] for k in ("ord_1", "ord_2", "ord_3")] == [True, True, False]
    assert all(got[k]["delivered_at"] is None for k in got)


def test_an_order_delivered_and_later_returned_keeps_its_delivery_and_is_not_closed(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_express_orders", [express(1, 0)])
    write(spark, lake, "silver_order_status", [
        status(1, "order_created", 0, 1), status(1, "delivered", 12, 6), status(1, "order_returned", 30, 7)])
    r = rows(spark, lake)["ord_1"]
    assert r["delivered_at"] == T0 + timedelta(minutes=12) and r["closed_without_delivery"] is False


def test_the_latest_status_is_the_greatest_status_time_and_ties_go_to_the_higher_rank(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_express_orders", [express(1, 0), express(2, 0)])
    write(spark, lake, "silver_order_status", [
        status(1, "order_created", 0, 1), status(1, "order_packed", 4, 3), status(1, "order_cancelled", 4, 2),
        status(2, "order_created", 0, 1), status(2, "order_packed", 3, 3), status(2, "order_cancelled", 4, 2)])
    got = rows(spark, lake)
    assert got["ord_1"]["closed_without_delivery"] is False  # same time: packed (rank 3) beats cancelled (rank 2)
    assert got["ord_2"]["closed_without_delivery"] is True  # cancelled is the later status


def test_without_the_express_table_the_result_is_empty(spark, tmp_path):
    assert live_express.compute_express_rows(spark, str(tmp_path / "empty"), SINCE) == []
