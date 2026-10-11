"""Spark tests (Docker, RUN_SPARK_TESTS=1) for the daily inventory mart and the daily express table."""
from datetime import date, timedelta
from decimal import Decimal

import pytest

pytestmark = pytest.mark.spark

from test_live_express_spark import express, status  # noqa: E402
from test_live_stock_spark import K, move, snap, write  # noqa: E402
from test_g1_live_spark import T0  # noqa: E402
from warehouse import inventory_daily  # noqa: E402

DAY = date(2026, 10, 8)  # T0 is 2026-10-08 12:00 UTC, which is 15:00 on the 8th in Cairo
NEXT_DAY = date(2026, 10, 9)
MIDNIGHT = 9 * 60  # minutes after T0 at which the Cairo day of the 8th ends (21:00 UTC)
MAADI, GIZA = "store_cairo_maadi", "store_giza_dokki"


def row_of(rows, key=K):
    [r] = [r for r in rows if (r["store_id"], r["product_id"]) == key]
    return r


def test_flows_and_closing_stock_of_one_day(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 40, 0)])
    sold = move(1, K, -5, 3)
    sold["requested_qty"] = 7  # asked for 7, only 5 were there: 2 oversold
    write(spark, lake, "silver_stock_movements",
          [sold, move(2, K, -3, 4), move(3, K, 60, 5, "received"), move(4, K, -2, 6, "shrinkage"),
           move(5, K, 4, 7, "adjustment")])
    r = row_of(inventory_daily.compute_inventory_daily(spark, lake, DAY))
    assert (r["units_sold"], r["units_received"], r["units_shrunk"], r["units_adjusted"]) == (8, 60, 2, 4)
    assert r["oversold_units"] == 2
    assert r["closing_on_hand"] == 40 - 5 - 3 + 60 - 2 + 4 and r["ended_out_of_stock"] is False


def test_a_product_that_runs_out_ends_the_day_out_of_stock_and_never_negative(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 4, 0)])
    write(spark, lake, "silver_stock_movements", [move(1, K, -4, 3)])
    r = row_of(inventory_daily.compute_inventory_daily(spark, lake, DAY))
    assert r["closing_on_hand"] == 0 and r["ended_out_of_stock"] is True
    write(spark, lake, "silver_stock_movements", [move(2, K, -6, 4)])  # a stock error: the raw level would be -6
    r = row_of(inventory_daily.compute_inventory_daily(spark, lake, DAY))
    assert r["closing_on_hand"] == 0


def test_reserved_stock_counts_as_out_of_stock_at_day_end(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 5, 0, reserved=5)])
    r = row_of(inventory_daily.compute_inventory_daily(spark, lake, DAY))
    assert r["closing_on_hand"] == 5 and r["ended_out_of_stock"] is True  # on_hand - reserved <= 0


def test_cairo_midnight_splits_the_days_and_closing_stock_stops_at_the_cut(spark, tmp_path):
    lake = str(tmp_path / "lake")
    late = T0.replace(hour=22, minute=30)  # 01:30 on the 9th in Cairo
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 40, 0)])
    write(spark, lake, "silver_stock_movements", [move(1, K, -5, 3)])
    after_midnight = dict(move(2, K, -7, 0), event_time=late, kafka_timestamp=late, event_date=date(2026, 10, 9))
    write(spark, lake, "silver_stock_movements", [after_midnight])
    day8 = row_of(inventory_daily.compute_inventory_daily(spark, lake, DAY))
    day9 = row_of(inventory_daily.compute_inventory_daily(spark, lake, date(2026, 10, 9)))
    assert (day8["units_sold"], day8["closing_on_hand"]) == (5, 35)
    assert (day9["units_sold"], day9["closing_on_hand"]) == (7, 28)


def test_a_movement_after_21_utc_with_a_utc_event_date_still_belongs_to_the_next_cairo_day(spark, tmp_path):
    lake = str(tmp_path / "lake")
    late = T0.replace(hour=21, minute=30)  # 00:30 on the 9th in Cairo, still the 8th in UTC
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 40, 0)])
    write(spark, lake, "silver_stock_movements",
          [dict(move(1, K, -7, 0), event_time=late, kafka_timestamp=late, event_date=late.date())])
    assert row_of(inventory_daily.compute_inventory_daily(spark, lake, DAY))["units_sold"] == 0
    assert row_of(inventory_daily.compute_inventory_daily(spark, lake, NEXT_DAY))["units_sold"] == 7


def test_a_day_without_movements_still_reports_the_closing_stock(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 40, 0)])
    r = row_of(inventory_daily.compute_inventory_daily(spark, lake, date(2026, 10, 9)))
    assert (r["closing_on_hand"], r["units_sold"], r["oversold_units"]) == (40, 0, 0)


def test_without_the_stock_tables_nothing_is_computed(spark, tmp_path):
    assert inventory_daily.compute_inventory_daily(spark, str(tmp_path / "empty"), DAY) == []


# ---- express_daily -------------------------------------------------------------------------------------------------

def by_store(rows):
    return {r["store_id"]: r for r in rows}


def express_day(spark, lake, now, day=DAY):
    return by_store(inventory_daily.compute_express_daily(spark, lake, day, now=now))


def test_a_mix_of_on_time_late_and_open_orders_over_two_stores_and_a_cairo_midnight(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_express_orders", [
        express(1, 0), express(2, 5), express(3, 10),                       # Maadi: on time, late, still open
        express(4, 20, store=GIZA), express(5, 50, store=GIZA),             # Giza: exactly on the promise, open but young
        express(6, MIDNIGHT + 5)])                                            # created after Cairo midnight: next day
    write(spark, lake, "silver_order_status", [
        status(1, "order_created", 0, 1), status(1, "delivered", 14, 6),     # 14 minutes
        status(2, "order_created", 5, 1), status(2, "delivered", 30, 6),     # 25 minutes
        status(3, "order_created", 10, 1),
        status(4, "order_created", 20, 1), status(4, "delivered", 40, 6),    # 20 minutes: on time
        status(5, "order_created", 50, 1),
        status(6, "order_created", MIDNIGHT + 5, 1)])
    now = T0 + timedelta(minutes=60)  # order 3 is 50 minutes old (breached), order 5 is 10 minutes old
    got = express_day(spark, lake, now)
    assert set(got) == {MAADI, GIZA}
    m, g = got[MAADI], got[GIZA]
    assert (m["report_date"], m["orders"], m["delivered"], m["on_time"], m["breached"]) == (DAY, 3, 2, 1, 2)
    assert (m["avg_delivery_minutes"], m["max_delivery_minutes"]) == (Decimal("19.50"), Decimal("25.00"))
    assert (g["orders"], g["delivered"], g["on_time"], g["breached"]) == (2, 1, 1, 0)
    assert (g["avg_delivery_minutes"], g["max_delivery_minutes"]) == (Decimal("20.00"), Decimal("20.00"))
    nxt = express_day(spark, lake, now, NEXT_DAY)
    assert set(nxt) == {MAADI} and nxt[MAADI]["orders"] == 1 and nxt[MAADI]["breached"] == 0
    assert nxt[MAADI]["avg_delivery_minutes"] is None and nxt[MAADI]["max_delivery_minutes"] is None


def test_the_maximum_is_the_slowest_delivered_order_and_an_open_order_does_not_count_in_it(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_express_orders", [express(1, 0), express(2, 0), express(3, 0), express(4, 0)])
    write(spark, lake, "silver_order_status", [
        status(1, "delivered", 9, 6), status(2, "delivered", 31, 6), status(3, "delivered", 18, 6),
        status(4, "order_created", 0, 1)])  # open for 3 hours, but not delivered: not in the minutes
    [r] = inventory_daily.compute_express_daily(spark, lake, DAY, now=T0 + timedelta(hours=3))
    assert r["max_delivery_minutes"] == Decimal("31.00")
    assert r["avg_delivery_minutes"] == Decimal("19.33")  # (9 + 31 + 18) / 3
    assert (r["orders"], r["delivered"], r["on_time"], r["breached"]) == (4, 3, 2, 2)  # one late, one overdue


def test_a_replayed_status_row_changes_nothing(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_express_orders", [express(1, 0)])
    base = [status(1, "order_created", 0, 1), status(1, "delivered", 14, 6)]
    write(spark, lake, "silver_order_status", base)
    now = T0 + timedelta(hours=2)
    first = inventory_daily.compute_express_daily(spark, lake, DAY, now=now)
    write(spark, lake, "silver_order_status", base + [status(1, "delivered", 14, 6, n=2),   # same delivery again
                                                       status(1, "delivered", 19, 6, n=3),   # a later duplicate
                                                       status(1, "order_created", 0, 1, n=2)])
    assert inventory_daily.compute_express_daily(spark, lake, DAY, now=now) == first
    assert first[0]["delivered"] == 1 and first[0]["max_delivery_minutes"] == Decimal("14.00")


def test_a_day_without_express_orders_gives_no_rows(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_express_orders", [express(1, 0)])
    write(spark, lake, "silver_order_status", [status(1, "order_created", 0, 1)])
    assert inventory_daily.compute_express_daily(spark, lake, date(2026, 10, 1), now=T0) == []
    assert inventory_daily.compute_express_daily(spark, str(tmp_path / "empty"), DAY, now=T0) == []


def test_a_cancelled_or_returned_order_past_its_promise_is_counted_but_not_breached(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_express_orders", [express(1, 0), express(2, 0), express(3, 0)])
    write(spark, lake, "silver_order_status", [
        status(1, "order_created", 0, 1), status(1, "order_cancelled", 3, 2),
        status(2, "order_created", 0, 1), status(2, "order_returned", 5, 7),
        status(3, "order_created", 0, 1)])  # open: the only breach
    [r] = inventory_daily.compute_express_daily(spark, lake, DAY, now=T0 + timedelta(hours=2))
    assert (r["orders"], r["delivered"], r["on_time"], r["breached"]) == (3, 0, 0, 1)
    assert r["avg_delivery_minutes"] is None and r["max_delivery_minutes"] is None


def test_an_order_delivered_late_and_returned_afterwards_is_still_a_late_delivery(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_express_orders", [express(1, 0)])
    write(spark, lake, "silver_order_status", [
        status(1, "order_created", 0, 1), status(1, "delivered", 25, 6), status(1, "order_returned", 40, 7)])
    [r] = inventory_daily.compute_express_daily(spark, lake, DAY, now=T0 + timedelta(hours=2))
    assert (r["delivered"], r["on_time"], r["breached"]) == (1, 0, 1)


def test_an_order_open_at_cairo_midnight_is_breached_by_the_next_build(spark, tmp_path):
    lake = str(tmp_path / "lake")
    created = MIDNIGHT - 5  # 23:55 in Cairo on the 8th
    write(spark, lake, "silver_express_orders", [express(1, created)])
    write(spark, lake, "silver_order_status", [status(1, "order_created", created, 1)])
    midnight = T0 + timedelta(minutes=MIDNIGHT)
    [first] = inventory_daily.compute_express_daily(spark, lake, DAY, now=midnight)
    assert (first["orders"], first["breached"]) == (1, 0)  # only 5 minutes old at 00:00
    [again] = inventory_daily.compute_express_daily(spark, lake, DAY, now=midnight + timedelta(minutes=30))
    assert (again["orders"], again["breached"]) == (1, 1)  # the same day built again later
    [edge] = inventory_daily.compute_express_daily(spark, lake, DAY, now=T0 + timedelta(minutes=created + 21))
    assert edge["breached"] == 0  # exactly the promise plus the one-minute grace is not yet a breach


def test_now_defaults_to_the_current_time(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_express_orders", [express(1, 0)])
    write(spark, lake, "silver_order_status", [status(1, "order_created", 0, 1)])
    [r] = inventory_daily.compute_express_daily(spark, lake, DAY)  # created in 2026-10-08, long ago by now
    assert r["breached"] == 1  # the build time is the current time, long after the 2026-10-08 order
