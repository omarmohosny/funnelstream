"""Spark tests (Docker, RUN_SPARK_TESTS=1) for stock now, served demand and the product dimension."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

pytestmark = pytest.mark.spark

from streaming.silver.spark_schemas import SCHEMAS, coerce_row  # noqa: E402
from streaming.silver.writer import PARTITION_COLS, SILVER_KEYS, merge_insert, silver_path  # noqa: E402
from test_g1_live_spark import T0, ev, write_events  # noqa: E402
from warehouse import live_stock  # noqa: E402

NOW = datetime(2026, 10, 8, 13, 0, tzinfo=timezone.utc)
K = ("store_a", "prod_1")


def write(spark, lake, table, rows):
    df = spark.createDataFrame([tuple(coerce_row(table, r).values()) for r in rows], SCHEMAS[table])
    merge_insert(spark, silver_path(lake, table), df, SILVER_KEYS[table], PARTITION_COLS.get(table))


def snap(i, key, on_hand, minute, reserved=0, reorder=10):
    t = T0 + timedelta(minutes=minute)
    return {"event_id": "s%03d" % i, "store_id": key[0], "product_id": key[1], "on_hand": on_hand,
            "reserved_qty": reserved, "reorder_point": reorder, "event_time": t, "kafka_timestamp": t,
            "event_date": t.date()}


def move(i, key, delta, minute, reason="sold"):
    t = T0 + timedelta(minutes=minute)
    return {"event_id": "m%03d" % i, "store_id": key[0], "product_id": key[1], "qty_delta": delta, "reason": reason,
            "order_id": "ord_%d" % i if reason == "sold" else None, "requested_qty": -delta if reason == "sold" else None,
            "event_time": t, "kafka_timestamp": t, "event_date": t.date()}


def by_key(rows):
    return {(r["store_id"], r["product_id"]): r for r in rows}


def test_stock_now_is_the_latest_snapshot_plus_the_movements_after_it(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 40, 0), snap(2, K, 30, 10)])
    write(spark, lake, "silver_stock_movements", [move(1, K, -5, 3), move(2, K, -4, 12), move(3, K, 60, 14, "received")])
    [row] = live_stock.compute_stock_rows(spark, lake, [K], NOW)
    # latest snapshot (minute 10) says 30; only the movements after minute 10 count: -4 and +60
    assert (row["on_hand"], row["reserved_qty"], row["available"], row["reorder_point"]) == (86, 0, 86, 10)
    assert row["updated_at"] == NOW


def test_a_late_movement_older_than_the_snapshot_and_one_at_the_same_time_do_not_count(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 30, 10)])
    write(spark, lake, "silver_stock_movements", [move(1, K, -7, 5), move(2, K, -9, 10), move(3, K, -2, 11)])
    [row] = live_stock.compute_stock_rows(spark, lake, [K], NOW)
    assert row["on_hand"] == 28  # only the movement at minute 11


def test_two_snapshots_at_the_same_time_pick_the_greater_event_id(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 40, 10), snap(2, K, 33, 10)])
    [row] = live_stock.compute_stock_rows(spark, lake, [K], NOW)
    assert row["on_hand"] == 33


def test_available_is_never_negative_and_reserved_is_subtracted(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 4, 0, reserved=1), snap(2, ("store_a", "prod_2"), 10, 0, reserved=3)])
    write(spark, lake, "silver_stock_movements", [move(1, K, -6, 1)])
    rows = by_key(live_stock.compute_stock_rows(spark, lake, [K, ("store_a", "prod_2")], NOW))
    assert (rows[K]["on_hand"], rows[K]["available"]) == (-2, 0)
    assert rows[("store_a", "prod_2")]["available"] == 7


def test_only_the_asked_pairs_are_computed_and_a_pair_without_a_snapshot_is_skipped(spark, tmp_path):
    lake = str(tmp_path / "lake")
    other = ("store_b", "prod_1")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 40, 0), snap(2, other, 50, 0)])
    assert list(by_key(live_stock.compute_stock_rows(spark, lake, [K], NOW))) == [K]
    assert live_stock.compute_stock_rows(spark, lake, [("store_c", "prod_9")], NOW) == []
    assert live_stock.compute_stock_rows(spark, lake, [], NOW) == []


def test_without_the_stock_tables_nothing_is_computed(spark, tmp_path):
    assert live_stock.compute_stock_rows(spark, str(tmp_path / "empty"), [K], NOW) == []


def test_demand_is_counted_per_minute_store_product_and_campaign_for_every_served_event(spark, tmp_path):
    lake = str(tmp_path / "lake")
    rows = [dict(ev(1, 0, "EG", "product_view"), product_id="prod_1", city="Cairo", district="Maadi", utm_campaign="eid_sale_1"),
            dict(ev(2, 0, "EG", "product_view"), product_id="prod_1", city="Cairo", district="Zamalek", utm_campaign=None),
            dict(ev(3, 0, "EG", "add_to_cart"), product_id="prod_1", city="Cairo", district="Maadi", utm_campaign=None),
            dict(ev(4, 0, "EG", "product_view"), product_id="prod_1", city="Assiut", district="Assiut First"),  # not served
            dict(ev(5, 0, "EG", "page_view"), product_id=None, city="Cairo", district="Maadi"),  # not a product event
            dict(ev(6, 0, "EG", "product_view"), product_id="prod_2", city="Cairo", district="Maadi",
                 utm_campaign="mystery_1")]  # a utm_campaign that is not in the campaign list
    write_events(spark, lake, rows)
    out = live_stock.compute_demand_rows(spark, lake, [T0])
    got = {(r["store_id"], r["product_id"], r["campaign_id"]): (r["views"], r["add_to_cart"], r["landing_clicks"]) for r in out}
    assert got == {("store_cairo_maadi", "prod_1", "camp_201"): (1, 0, 1),
                   ("store_cairo_maadi", "prod_1", "direct"): (1, 1, 0),
                   ("store_cairo_maadi", "prod_2", "unknown"): (1, 0, 1)}
    utm = {r["campaign_id"]: r["utm_campaign"] for r in out if r["product_id"] == "prod_1"}
    assert utm == {"camp_201": "eid_sale_1", "direct": ""}  # the utm_campaign is only an attribute
    assert all(r["minute_start"] == T0 and "oos_at_write" not in r for r in out)  # the writer sets the flag, not the compute


def test_the_current_product_dimension_keeps_the_latest_version_of_each_product(spark, tmp_path):
    lake = str(tmp_path / "lake")

    def prod(pid, name, price, at, category="dairy"):
        return {"product_id": pid, "updated_at": at, "name": name, "category": category, "price": Decimal(price),
                "is_active": True}

    write(spark, lake, "silver_products", [prod("prod_1", "Milk 1L", "30.00", T0),
                                           prod("prod_1", "Milk 1L Full Fat", "32.50", T0 + timedelta(hours=1)),
                                           prod("prod_2", "Rice 1kg", "40.00", T0, "grocery")])
    rows = {r["product_id"]: r for r in live_stock.compute_product_rows(spark, lake)}
    assert set(rows) == {"prod_1", "prod_2"}
    assert (rows["prod_1"]["name"], rows["prod_1"]["price"], rows["prod_1"]["category"]) == ("Milk 1L Full Fat", Decimal("32.50"), "dairy")
    assert live_stock.compute_product_rows(spark, str(tmp_path / "empty")) == []
