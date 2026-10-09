"""agg_city_product_daily: Gold SQL step (warehouse/gold/11_agg_city_product_daily.sql) on local Spark with Delta, and
its reference function streaming/gold/city_product.py (run in Docker: RUN_SPARK_TESTS=1).

Hand-built cases use tiny Silver tables dated in October 2026 (named windows, no dependence on the clock). The step
under test reads silver_events and gold.fact_order_lines (built by the real fact_order_lines step).
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

pytestmark = pytest.mark.spark

from pyspark.sql import functions as F  # noqa: E402

from scripts.seed_silver_reference import seed  # noqa: E402
from streaming.gold.bots import mark_bot_sessions  # noqa: E402
from streaming.gold.city_product import AGG_CITY_PRODUCT_DAILY_COLUMNS, build_agg_city_product_daily  # noqa: E402
from streaming.gold.orders import build_fact_order_lines  # noqa: E402
from streaming.jobs.silver_clean import process_batch  # noqa: E402
from streaming.silver.writer import silver_path  # noqa: E402
from tests.test_gold_build_spark import SILVER_FACTS, compare, load, make_env, seed_dims  # noqa: E402
from tests.test_gold_reference import (CATALOG, CUSTOMERS, NOW, bronze_df, ev, order_row,  # noqa: E402
                                       simulated, status_row, t)
from warehouse.gold_build import run_build  # noqa: E402

KEYS = ["city", "product_id", "date_key"]
STEPS = ["dim_customer", "dim_product", "fact_orders", "fact_order_lines", "agg_city_product_daily"]
GOLD_TABLES = ("dim_customer", "dim_product", "dim_campaign", "dim_date", "fact_orders", "fact_order_lines",
               "fact_attribution", "agg_city_product_daily")
MONTH = {"from_date": "2026-10-01", "to_date": "2026-10-31"}
Z = Decimal("0.00")


@pytest.fixture(scope="module", autouse=True)
def small_shuffles(spark):
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    spark.conf.set("spark.databricks.delta.snapshotPartitions", "2")
    yield


@pytest.fixture
def cenv(spark, tmp_path, request):
    import re
    name = re.sub(r"\W", "_", request.node.name)[:30]
    env = make_env(spark, str(tmp_path), "s_" + name, "g_" + name, gold_tables=GOLD_TABLES)
    seed_dims(spark, env, customers=("c1", "c2"), products=("p1", "p2", "p3", "p9"))
    return env


_N = [0]


def E(sid, when, etype, product="p1", city="Cairo", **kw):
    """One silver_events row (event_id unique per call)."""
    _N[0] += 1
    return ev(_N[0], sid, when, etype, product_id=product, city=city, **kw)


def add_events(spark, env, rows):
    load(spark, env.silver, "silver_events", rows)


def put(spark, env, oid, created, city="Cairo", lines=(("p1", 1, "10.00"),), statuses=(("order_created", 0),),
        customer="c1"):
    """An order with one item row per (product_id, quantity, line_total)."""
    total = sum(Decimal(x[2]) for x in lines)
    row = order_row(oid, customer=customer, created=created, total=str(total))
    row["city"] = city
    load(spark, env.silver, "silver_orders", [row])
    load(spark, env.silver, "silver_order_status", [
        status_row(oid, s, created + timedelta(hours=h), customer=customer) for s, h in statuses])
    load(spark, env.silver, "silver_order_items", [
        {"order_id": oid, "line_no": str(i + 1), "product_id": p, "quantity": q, "unit_price": Decimal("10.00"),
         "line_total": Decimal(lt), "currency": "EGP", "order_time": created} for i, (p, q, lt) in enumerate(lines)])


def build(env, steps=STEPS, **window):
    return run_build(env, steps=steps, **(window or MONTH))


def rows(spark, env):
    return {(r["city"], r["product_id"], r["date_key"]): (r["sessions_viewed"], r["add_to_cart"], r["units"],
                                                           r["revenue"])
            for r in spark.table(env.gold + ".agg_city_product_daily").collect()}


def reference(spark, env):
    return build_agg_city_product_daily(mark_bot_sessions(spark.table(env.silver + ".silver_events")),
                                        spark.table(env.gold + ".fact_order_lines"))


def assert_matches_reference(spark, env):
    ref = reference(spark, env)
    cols = ref.columns
    assert cols == AGG_CITY_PRODUCT_DAILY_COLUMNS
    n_sql, n_ref, only_sql, only_ref, mism, samples = compare(
        spark.table(env.gold + ".agg_city_product_daily").select(*cols), ref, KEYS, cols)
    assert (n_sql, only_sql, only_ref) == (n_ref, 0, 0) and sum(mism.values()) == 0, (mism, samples)
    return n_sql


# ------------------------------------------------------------------ event measures
def test_distinct_sessions_not_events_and_view_only_product_has_zero_carts(spark, cenv):
    d = 20261005
    add_events(spark, cenv, (
        [E("s1", t(5, 10, 0, i), "product_view") for i in range(3)]            # same session 3 times: counts once
        + [E("s2", t(5, 11), "product_view"), E("s2", t(5, 11, 1), "add_to_cart"), E("s2", t(5, 11, 2), "add_to_cart")]
        + [E("s3", t(5, 12), "product_view", product="p2")]                     # view-only product
        + [E("s4", t(5, 13), "add_to_cart", product="p3")]                      # cart without a view event
        + [E("s5", t(5, 14), "product_click"), E("s5", t(5, 14, 1), "page_view")]))  # other event types: no row
    build(cenv)
    got = rows(spark, cenv)
    assert got == {("Cairo", "p1", d): (2, 1, 0, Z), ("Cairo", "p2", d): (1, 0, 0, Z),
                   ("Cairo", "p3", d): (0, 1, 0, Z)}
    assert assert_matches_reference(spark, cenv) == 3


def test_a_session_in_two_cities_counts_in_each_and_other_days_are_separate(spark, cenv):
    add_events(spark, cenv, [E("s1", t(5, 10), "product_view", city="Cairo"),
                             E("s1", t(5, 11), "product_view", city="Giza"),
                             E("s1", t(6, 10), "product_view", city="Cairo")])
    build(cenv)
    assert rows(spark, cenv) == {("Cairo", "p1", 20261005): (1, 0, 0, Z), ("Giza", "p1", 20261005): (1, 0, 0, Z),
                                 ("Cairo", "p1", 20261006): (1, 0, 0, Z)}


def test_bots_anonymous_null_product_unknown_city_and_stored_flags(spark, cenv):
    start = t(5, 12)
    bot = [E("bot", start + timedelta(milliseconds=30 * i), "product_view" if i % 2 else "add_to_cart", product="p2")
           for i in range(150)]                                   # 150 events in 4.5 seconds: a bot, never counted
    add_events(spark, cenv, bot + [
        E("ok", t(5, 10), "product_view", product="p2"),
        E(None, t(5, 10), "product_view", product="p2"),                           # no session_id: excluded
        E("n1", t(5, 10), "product_view", product=None),                           # no product_id: ignored
        E("n2", t(5, 10), "product_view", product="p1", city=None),                # NULL city -> unknown
        E("n3", t(5, 10), "product_view", product="p1", city=""),                  # empty city -> unknown
        E("n4", t(5, 10), "product_view", product="p3", is_bot=True),              # stored flag is not trusted
        E("n5", t(5, 10), "product_view", product="p3", is_late=True),             # late rows are included
    ])
    build(cenv)
    d = 20261005
    assert rows(spark, cenv) == {("Cairo", "p2", d): (1, 0, 0, Z), ("unknown", "p1", d): (2, 0, 0, Z),
                                 ("Cairo", "p3", d): (2, 0, 0, Z)}
    assert assert_matches_reference(spark, cenv) == 3


# ------------------------------------------------------------------ order measures
def test_order_lines_sum_cancelled_included_and_one_sided_rows_have_zeros(spark, cenv):
    d3, d4 = 20261003, 20261004
    put(spark, cenv, "o1", t(3, 10), lines=(("p1", 2, "20.00"), ("p1", 1, "10.50"), ("p3", 1, "7.00")))
    put(spark, cenv, "o2", t(3, 11), lines=(("p1", 3, "30.00"),),
        statuses=(("order_created", 0), ("order_cancelled", 1)))             # cancelled: still counted
    put(spark, cenv, "o3", t(4, 10), city="Alexandria", lines=(("p9", 1, "5.00"),))
    put(spark, cenv, "o4", t(4, 11), lines=(("p1", 1, "10.00"),))
    add_events(spark, cenv, [E("s1", t(3, 9), "product_view", product="p1"),
                             E("s2", t(3, 9), "product_view", product="p2")])
    build(cenv, steps=STEPS[:4])
    # a line whose city is NULL: set after the lines step, then the step under test runs on top of it
    spark.sql("UPDATE %s.fact_order_lines SET city = NULL WHERE order_id = 'o4'" % cenv.gold)
    build(cenv, steps=["agg_city_product_daily"])
    assert rows(spark, cenv) == {
        ("Cairo", "p1", d3): (1, 0, 6, Decimal("60.50")),     # 2 + 1 + 3 units, 20.00 + 10.50 + 30.00 incl. cancelled
        ("Cairo", "p3", d3): (0, 0, 1, Decimal("7.00")),      # order lines only: sessions 0
        ("Cairo", "p2", d3): (1, 0, 0, Z),                    # views only: units 0 and revenue 0.00
        ("Alexandria", "p9", d4): (0, 0, 1, Decimal("5.00")),
        ("unknown", "p1", d4): (0, 0, 1, Decimal("10.00"))}   # NULL city of a line
    assert assert_matches_reference(spark, cenv) == 5


def test_lines_without_product_are_ignored(spark, cenv):
    put(spark, cenv, "o1", t(3, 10), lines=(("p1", 1, "10.00"), (None, 4, "40.00")))
    build(cenv)
    assert rows(spark, cenv) == {("Cairo", "p1", 20261003): (0, 0, 1, Decimal("10.00"))}


# ------------------------------------------------------------------ window, late data, idempotence, deletes
def test_a_session_crossing_midnight_counts_once_on_each_day_and_both_days_are_recomputed(spark, cenv):
    add_events(spark, cenv, [E("sm", t(5, 20, 0), "product_view", event_date=date(2026, 10, 5)),
                             E("sm", t(5, 22, 30), "product_view", event_date=date(2026, 10, 6))])
    build(cenv, from_date="2026-10-06", to_date="2026-10-06")     # window holds only the second day
    assert rows(spark, cenv) == {("Cairo", "p1", 20261005): (1, 0, 0, Z), ("Cairo", "p1", 20261006): (1, 0, 0, Z)}


def test_a_bot_judged_over_both_days_is_removed_from_the_earlier_day_too(spark, cenv):
    start = t(5, 21, 59, 50)
    burst = ([E("bot", start + timedelta(milliseconds=50 * i), "product_view", event_date=date(2026, 10, 5))
              for i in range(60)]
             + [E("bot", start + timedelta(milliseconds=3000 + 50 * i), "product_view",
                  event_date=date(2026, 10, 6)) for i in range(60)])        # 120 events in 6 seconds over 2 days
    add_events(spark, cenv, burst + [E("ok", t(5, 10), "product_view")])
    build(cenv, from_date="2026-10-06", to_date="2026-10-06")
    assert rows(spark, cenv) == {("Cairo", "p1", 20261005): (1, 0, 0, Z)}   # only the normal session, day 5 recomputed


def test_a_late_event_updates_its_old_day_only_when_its_kafka_date_is_in_the_window(spark, cenv):
    add_events(spark, cenv, [E("s1", t(1, 10), "product_view")])
    build(cenv)
    assert rows(spark, cenv) == {("Cairo", "p1", 20261001): (1, 0, 0, Z)}
    add_events(spark, cenv, [E("late1", t(1, 11), "product_view", kafka_timestamp=t(2, 11), is_late=True)])
    build(cenv, from_date="2026-10-08", to_date="2026-10-08")      # Kafka date 2 Oct: outside the window, not seen
    assert rows(spark, cenv) == {("Cairo", "p1", 20261001): (1, 0, 0, Z)}
    add_events(spark, cenv, [E("late2", t(1, 12), "product_view", kafka_timestamp=t(8, 11), is_late=True)])
    build(cenv, from_date="2026-10-08", to_date="2026-10-08")      # Kafka date 8 Oct: inside the window
    assert rows(spark, cenv) == {("Cairo", "p1", 20261001): (3, 0, 0, Z)}  # late1 comes along: the day is recomputed
    assert assert_matches_reference(spark, cenv) == 1


def test_a_late_status_row_recomputes_the_old_order_day(spark, cenv):
    put(spark, cenv, "o_old", t(3, 10), lines=(("p1", 1, "10.00"),))
    put(spark, cenv, "o_other", t(4, 10), lines=(("p2", 1, "10.00"),))
    build(cenv)
    before = rows(spark, cenv)
    spark.sql("UPDATE %s.silver_order_items SET quantity = 5, line_total = 50.00 WHERE order_id = 'o_old'" % cenv.silver)
    spark.sql("UPDATE %s.silver_order_items SET quantity = 9, line_total = 90.00 WHERE order_id = 'o_other'"
              % cenv.silver)
    load(spark, cenv.silver, "silver_order_status", [status_row("o_old", "delivered", t(20, 9), customer="c1")])
    build(cenv, from_date="2026-10-20", to_date="2026-10-20")
    after = rows(spark, cenv)
    assert after[("Cairo", "p1", 20261003)] == (0, 0, 5, Decimal("50.00"))        # its status row is in the window
    assert after[("Cairo", "p2", 20261004)] == before[("Cairo", "p2", 20261004)]  # the other order is not touched


def test_rerun_changes_nothing_vanished_keys_are_deleted_inside_the_window_only(spark, cenv):
    add_events(spark, cenv, [E("s1", t(5, 10), "product_view", product="p1"),
                             E("s2", t(5, 10), "product_view", product="p2"),
                             E("s3", t(20, 10), "product_view", product="p2")])
    put(spark, cenv, "o1", t(3, 10), lines=(("p3", 2, "20.00"),))
    put(spark, cenv, "o2", t(25, 10), lines=(("p3", 1, "10.00"),))
    build(cenv)
    first = rows(spark, cenv)
    assert len(first) == 5
    build(cenv)
    assert rows(spark, cenv) == first
    # both views of day 5 vanish; the order lines of day 3 vanish
    spark.sql("DELETE FROM %s.silver_events WHERE event_date = DATE'2026-10-05'" % cenv.silver)
    spark.sql("DELETE FROM %s.fact_order_lines WHERE order_id = 'o1'" % cenv.gold)
    build(cenv, steps=["agg_city_product_daily"], from_date="2026-10-10", to_date="2026-10-31")
    assert rows(spark, cenv) == {k: v for k, v in first.items()}   # days 3 and 5 are outside the window: untouched
    build(cenv, steps=["agg_city_product_daily"], from_date="2026-10-04", to_date="2026-10-05")
    second = rows(spark, cenv)
    assert ("Cairo", "p1", 20261005) not in second and ("Cairo", "p2", 20261005) not in second
    assert ("Cairo", "p3", 20261003) in second                     # day 3 still outside this window
    build(cenv, steps=["agg_city_product_daily"], from_date="2026-10-01", to_date="2026-10-03")
    third = rows(spark, cenv)
    assert ("Cairo", "p3", 20261003) not in third
    assert third == {("Cairo", "p2", 20261020): (1, 0, 0, Z), ("Cairo", "p3", 20261025): (0, 0, 1, Decimal("10.00"))}
    assert assert_matches_reference(spark, cenv) == 2


# ------------------------------------------------------------------ parity on the simulator dataset
@pytest.fixture(scope="module")
def parity(spark, tmp_path_factory):
    base = tmp_path_factory.mktemp("agg_city_product")
    lake = str(base / "lake")
    msgs, _ = simulated(30_000, seed=11)
    process_batch(spark, bronze_df(spark, msgs), lake, None, now=NOW)
    seed(spark, lake, datetime(2025, 1, 1, tzinfo=timezone.utc), CATALOG, CUSTOMERS)
    store = make_env(spark, str(base / "wh"), "silver_cp", "gold_cp", gold_tables=GOLD_TABLES)
    for table in SILVER_FACTS:
        load(spark, "silver_cp", table, df=spark.read.format("delta").load(silver_path(lake, table)))
    result = run_build(store, full=True, steps=["dim_customer", "dim_product", "fact_orders", "fact_order_lines",
                                                "agg_city_product_daily"])
    return {"store": store, "result": result}


def test_sql_equals_reference_on_the_simulator_dataset(spark, parity):
    events = mark_bot_sessions(spark.table("silver_cp.silver_events"))
    dim_c = spark.table("gold_cp.dim_customer").select("customer_id", "customer_key", "valid_from", "valid_to")
    dim_p = spark.table("gold_cp.dim_product").select("product_id", "product_key", "valid_from", "valid_to")
    lines = build_fact_order_lines(spark.table("silver_cp.silver_orders"), spark.table("silver_cp.silver_order_items"),
                                   dim_c, dim_p)
    ref = build_agg_city_product_daily(events, lines)
    cols = ref.columns
    assert cols == AGG_CITY_PRODUCT_DAILY_COLUMNS
    n_sql, n_ref, only_sql, only_ref, mism, samples = compare(
        spark.table("gold_cp.agg_city_product_daily").select(*cols), ref, KEYS, cols)
    g = spark.table("gold_cp.agg_city_product_daily")
    bots = events.where("is_bot").select("session_id").distinct().count()
    print("\nPARITY agg_city_product_daily rows_sql=%d rows_ref=%d only_sql=%d only_ref=%d columns=%d "
          "mismatches=%d cells_compared=%d bot_sessions=%d event_only_rows=%d order_only_rows=%d both_rows=%d "
          "sum_sessions_viewed=%d sum_add_to_cart=%d sum_units=%d" % (
              n_sql, n_ref, only_sql, only_ref, len(cols), sum(mism.values()), n_sql * len(cols), bots,
              g.where("units = 0 AND sessions_viewed + add_to_cart > 0").count(),
              g.where("units > 0 AND sessions_viewed = 0 AND add_to_cart = 0").count(),
              g.where("units > 0 AND sessions_viewed + add_to_cart > 0").count(),
              g.agg(F.sum("sessions_viewed")).first()[0], g.agg(F.sum("add_to_cart")).first()[0],
              g.agg(F.sum("units")).first()[0]))
    assert n_sql == n_ref >= 100
    assert only_sql == only_ref == 0
    assert sum(mism.values()) == 0, (mism, samples)
    assert g.where("units = 0 AND sessions_viewed > 0").count() > 0
    assert g.where("units > 0").count() > 0
    assert g.where("revenue IS NULL OR units IS NULL OR sessions_viewed IS NULL OR add_to_cart IS NULL").count() == 0
    qty = spark.table("gold_cp.fact_order_lines").where("product_id IS NOT NULL").agg(F.sum("quantity")).first()[0]
    assert g.agg(F.sum("units")).first()[0] == qty   # every line is on exactly one row


def test_rebuild_of_the_dataset_is_identical_and_step_result_has_rows(spark, parity):
    store = parity["store"]
    assert parity["result"]["steps"]["agg_city_product_daily"]["status"] == "ok"
    before = sorted(map(tuple, spark.table("gold_cp.agg_city_product_daily").collect()), key=repr)
    result = run_build(store, full=True, steps=["agg_city_product_daily"])
    after = sorted(map(tuple, spark.table("gold_cp.agg_city_product_daily").collect()), key=repr)
    print("\nIDEMPOTENCE agg_city_product_daily rows=%d" % len(after))
    assert before == after and result["steps"]["agg_city_product_daily"]["rows"] == len(after)
