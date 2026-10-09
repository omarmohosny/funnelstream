"""agg_campaign_daily: Gold SQL step (warehouse/gold/10_agg_campaign_daily.sql) on local Spark with Delta, and its
reference function streaming/gold/agg_campaign.py (run in Docker: RUN_SPARK_TESTS=1).

Hand-built cases use tiny Silver tables; orders are dated RELATIVE to the real clock (open_rate depends on
current_timestamp()), the window is wide enough to hold them. gold.fact_attribution rows are written by hand (the
attribution step has its own tests); the step under test reads fact_orders (built by the real fact_orders step) and
fact_attribution.
"""
import random
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

pytestmark = pytest.mark.spark

from pyspark.sql import functions as F  # noqa: E402

from scripts.seed_silver_reference import seed  # noqa: E402
from simulator.campaigns import CAMPAIGNS  # noqa: E402
from streaming.gold.agg_campaign import AGG_CAMPAIGN_DAILY_COLUMNS, build_agg_campaign_daily  # noqa: E402
from streaming.gold.attribution import build_clicks, build_fact_attribution  # noqa: E402
from streaming.gold.orders import build_fact_orders  # noqa: E402
from streaming.jobs.silver_clean import process_batch  # noqa: E402
from streaming.silver.writer import silver_path  # noqa: E402
from tests.test_gold_build_spark import SILVER_FACTS, compare, load, make_env, put_order, seed_dims  # noqa: E402
from tests.test_gold_reference import CATALOG, CUSTOMERS, NOW, bronze_df, simulated, status_row  # noqa: E402
from warehouse.gold_build import run_build  # noqa: E402

KEYS = ["campaign_id", "report_date", "attribution_model"]
STEPS = ["dim_customer", "dim_product", "fact_orders", "agg_campaign_daily"]
GOLD_TABLES = ("dim_customer", "dim_product", "dim_campaign", "dim_date", "fact_orders", "fact_order_lines",
               "fact_attribution", "agg_campaign_daily")
SNAP_SCHEMA = ("campaign_id string, report_date date, pulled_at timestamp, utm_campaign string, ad_platform string, "
               "impressions long, clicks long, platform_conversions long, spend decimal(12,4), currency string")


@pytest.fixture(scope="module", autouse=True)
def small_shuffles(spark):
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    spark.conf.set("spark.databricks.delta.snapshotPartitions", "2")
    yield


def days_ago(n, hh=10):
    """UTC datetime at hh:00 of (today_utc - n days). Cairo has the same calendar date for hh in 3..20."""
    return datetime.combine(datetime.now(timezone.utc).date() - timedelta(days=n), time(hh), tzinfo=timezone.utc)


def day(n):
    return days_ago(n).date()


def wide():
    today = datetime.now(timezone.utc).date()
    return {"from_date": (today - timedelta(days=90)).isoformat(), "to_date": (today + timedelta(days=1)).isoformat()}


@pytest.fixture
def cenv(spark, tmp_path, request):
    import re
    name = re.sub(r"\W", "_", request.node.name)[:30]
    env = make_env(spark, str(tmp_path), "s_" + name, "g_" + name, gold_tables=GOLD_TABLES)
    seed_dims(spark, env, customers=("c1", "c2", "c3"))
    return env


def add_snaps(spark, env, rows):
    """rows: (campaign_id, report_date, pulled_at, impressions, clicks, platform_conversions, spend, ad_platform)."""
    data = [(c, d, p, "utm", plat, i, k, pc, None if sp is None else Decimal(str(sp)), "EGP")
            for c, d, p, i, k, pc, sp, plat in rows]
    spark.createDataFrame(data, SNAP_SCHEMA).write.format("delta").mode("append").insertInto(
        "%s.silver_campaign_snapshots" % env.silver)


def S(campaign, report_date, pulled, impressions, clicks, conversions, spend, platform="google"):
    return (campaign, report_date, pulled, impressions, clicks, conversions, spend, platform)


def attribute(spark, env, order_id, campaign, total, model="last_touch"):
    camp = "NULL" if campaign is None else "'%s'" % campaign
    spark.sql("INSERT INTO %s.fact_attribution VALUES ('%s', '%s', %s, NULL, CAST(%s AS DECIMAL(12,2)))"
              % (env.gold, order_id, model, camp, total))


def order(spark, env, oid, customer, created, campaign, total, statuses=(("order_created", 0),), first_touch="camp_203"):
    put_order(spark, env, oid, customer, created, statuses, total=total)
    attribute(spark, env, oid, campaign, total)
    attribute(spark, env, oid, first_touch if campaign else None, total, model="first_touch")


def build(env, **kw):
    return run_build(env, steps=STEPS, **(kw or wide()))


def rows(spark, env):
    return {(r["campaign_id"], r["report_date"]): r.asDict() for r in spark.table(env.gold + ".agg_campaign_daily").collect()}


def reference(spark, env, as_of=None):
    """The reference function over the Gold tables of env (fact_orders and fact_attribution as built there)."""
    return build_agg_campaign_daily(spark.table(env.gold + ".fact_orders"), spark.table(env.gold + ".fact_attribution"),
                                    spark.table(env.silver + ".silver_campaign_snapshots"),
                                    as_of or datetime.now(timezone.utc))


def assert_matches_reference(spark, env):
    ref = reference(spark, env)
    cols = ref.columns
    assert cols == AGG_CAMPAIGN_DAILY_COLUMNS
    n_sql, n_ref, only_sql, only_ref, mism, samples = compare(
        spark.table(env.gold + ".agg_campaign_daily").select(*cols), ref, KEYS, cols)
    assert (n_sql, only_sql, only_ref) == (n_ref, 0, 0) and sum(mism.values()) == 0, (mism, samples)
    return n_sql


DELIVERED = (("order_created", 0), ("order_shipped", 2), ("delivered", 50))
CANCELLED = (("order_created", 0), ("order_cancelled", 1))
RETURNED = (("order_created", 0), ("order_shipped", 2), ("delivery_failed", 30), ("order_returned", 60))


# ------------------------------------------------------------------ ad measures
def test_latest_pulled_at_wins_earlier_pulls_are_not_summed_and_ties_are_deterministic(spark, cenv):
    d = day(20)
    add_snaps(spark, cenv, [
        S("camp_201", d, days_ago(20, 8), 1000, 20, 1, "50.5000"),
        S("camp_201", d, days_ago(20, 23), 4000, 90, 4, "200.2500"),          # latest pull: this one counts
        S("camp_201", d, days_ago(20, 12), 2000, 40, 2, "100.0000"),
        # tie on pulled_at: greatest impressions wins, whatever the insert order
        S("camp_202", d, days_ago(20, 9), 500, 5, 0, "10.0000", "meta"),
        S("camp_202", d, days_ago(20, 9), 700, 3, 1, "9.0000", "meta"),
        S("camp_202", d, days_ago(20, 9), 600, 9, 0, "99.0000", "meta"),
        # ignored: NULL campaign_id, NULL report_date
        S(None, d, days_ago(20, 9), 1, 1, 1, "1.0000"),
        S("camp_203", None, days_ago(20, 9), 1, 1, 1, "1.0000")])
    build(cenv)
    got = rows(spark, cenv)
    assert set(got) == {("camp_201", d), ("camp_202", d)}
    a = got[("camp_201", d)]
    assert (a["impressions"], a["clicks"], a["platform_conversions"], a["spend"], a["ad_platform"]) == (
        4000, 90, 4, Decimal("200.2500"), "google")
    assert a["attribution_model"] == "last_touch"
    b = got[("camp_202", d)]
    assert (b["impressions"], b["clicks"], b["spend"], b["ad_platform"]) == (700, 3, Decimal("9.0000"), "meta")
    # a NEW later pull for an old day in the window replaces the earlier numbers
    add_snaps(spark, cenv, [S("camp_201", d, days_ago(19, 8), 5000, 100, 5, "250.0000")])
    build(cenv)
    assert rows(spark, cenv)[("camp_201", d)]["impressions"] == 5000
    assert_matches_reference(spark, cenv)


def test_ad_row_without_orders_has_zero_counts_and_null_rates(spark, cenv):
    d = day(20)
    add_snaps(spark, cenv, [S("camp_201", d, days_ago(20, 8), 1000, 20, 1, "50.0000")])
    build(cenv)
    r = rows(spark, cenv)[("camp_201", d)]
    assert (r["orders_placed"], r["orders_delivered"], r["orders_cancelled"], r["orders_returned"],
            r["orders_with_failed_attempt"], r["new_customer_orders"]) == (0, 0, 0, 0, 0, 0)
    assert (r["revenue_placed"], r["revenue_delivered"]) == (Decimal("0.00"), Decimal("0.00"))
    assert r["spend"] == Decimal("50.0000") and r["clicks"] == 20
    for c in ("cancel_rate", "failed_attempt_rate", "return_rate", "open_rate", "aov", "cpa",
              "cost_per_delivered_order"):
        assert r[c] is None, c
    assert r["ctr"] == Decimal("0.0200") and r["conversion_rate"] == Decimal("0.0000")
    assert r["roas_placed"] == Decimal("0.00") and r["roas_delivered"] == Decimal("0.00")  # spend > 0, revenue 0


def test_orders_without_an_ad_row_have_null_ad_columns_and_direct_and_unknown_are_excluded(spark, cenv):
    d = day(20)
    order(spark, cenv, "o1", "c1", days_ago(20, 10), "camp_201", "100.00", DELIVERED)
    order(spark, cenv, "o2", "c2", days_ago(20, 11), "camp_201", "50.00")
    order(spark, cenv, "o_direct", "c3", days_ago(20, 12), None, "70.00")
    order(spark, cenv, "o_unknown", "c3", days_ago(20, 13), "unknown", "80.00")
    order(spark, cenv, "o_other", "c3", days_ago(21, 13), "camp_202", "30.00")
    build(cenv)
    got = rows(spark, cenv)
    assert set(got) == {("camp_201", d), ("camp_202", day(21))}
    r = got[("camp_201", d)]
    for c in ("ad_platform", "impressions", "clicks", "platform_conversions", "spend", "ctr", "conversion_rate",
              "cpa", "cost_per_delivered_order", "roas_placed", "roas_delivered"):
        assert r[c] is None, c
    assert (r["orders_placed"], r["orders_delivered"], r["revenue_placed"], r["revenue_delivered"]) == (
        2, 1, Decimal("150.00"), Decimal("100.00"))
    assert r["aov"] == Decimal("75.00") and r["cancel_rate"] == Decimal("0.0000")
    assert (r["new_customer_orders"], r["open_rate"]) == (2, Decimal("0.5000"))  # o1 delivered, o2 open and old
    assert_matches_reference(spark, cenv)


def test_every_rate_and_ratio_on_one_campaign_day_and_zero_denominators(spark, cenv):
    d = day(20)
    order(spark, cenv, "o1", "c1", days_ago(20, 10), "camp_201", "100.00", DELIVERED)   # delivered on d + 2
    order(spark, cenv, "o2", "c2", days_ago(20, 11), "camp_201", "200.00", CANCELLED)
    order(spark, cenv, "o3", "c1", days_ago(20, 12), "camp_201", "300.50", RETURNED)    # failed attempt, returned
    order(spark, cenv, "o4", "c2", days_ago(20, 13), "camp_201", "400.25")              # open (20 days old)
    add_snaps(spark, cenv, [S("camp_201", d, days_ago(20, 23), 10000, 250, 7, "500.0000")])
    # camp_202: spend 0 and zero clicks and impressions, no orders. camp_203: orders but nothing delivered
    add_snaps(spark, cenv, [S("camp_202", d, days_ago(20, 23), 0, 0, 0, "0.0000", "meta"),
                            S("camp_203", d, days_ago(20, 23), 3000, 30, 1, "90.0000", "meta")])
    order(spark, cenv, "o5", "c3", days_ago(20, 14), "camp_203", "60.00")
    build(cenv)
    got = rows(spark, cenv)
    a = got[("camp_201", d)]
    want = {"orders_placed": 4, "orders_delivered": 1, "orders_cancelled": 1, "orders_returned": 1,
            "orders_with_failed_attempt": 1, "new_customer_orders": 2,  # o1 (c1) and o2 (c2); o3, o4 are repeats
            "revenue_placed": Decimal("1000.75"), "revenue_delivered": Decimal("100.00"),
            "cancel_rate": Decimal("0.2500"), "failed_attempt_rate": Decimal("0.2500"),
            "return_rate": Decimal("0.2500"), "open_rate": Decimal("0.2500"), "aov": Decimal("250.19"),
            "ctr": Decimal("0.0250"), "conversion_rate": Decimal("0.0160"), "cpa": Decimal("125.00"),
            "cost_per_delivered_order": Decimal("500.00"), "roas_placed": Decimal("2.00"),
            "roas_delivered": Decimal("0.20"), "impressions": 10000, "clicks": 250, "platform_conversions": 7,
            "spend": Decimal("500.0000"), "ad_platform": "google"}
    for c, v in want.items():
        assert a[c] == v, (c, a[c], v)
    # delivered revenue sits on the ORDER day: there is no row for the delivery day (d + 2)
    assert not any(k[1] == d + timedelta(days=2) for k in got)
    # zero denominators: spend 0 and no clicks / impressions / orders
    z = got[("camp_202", d)]
    assert z["spend"] == Decimal("0.0000") and z["orders_placed"] == 0
    for c in ("ctr", "conversion_rate", "cpa", "cost_per_delivered_order", "roas_placed", "roas_delivered", "aov",
              "cancel_rate", "failed_attempt_rate", "return_rate", "open_rate"):
        assert z[c] is None, c
    # orders but nothing delivered: cost per delivered order is NULL, roas_delivered is 0.00
    n = got[("camp_203", d)]
    assert n["cost_per_delivered_order"] is None and n["roas_delivered"] == Decimal("0.00")
    assert (n["cpa"], n["roas_placed"], n["aov"], n["open_rate"]) == (
        Decimal("90.00"), Decimal("0.67"), Decimal("60.00"), Decimal("1.0000"))
    # spend 0 with orders: spend-based ratios NULL, order ratios still there
    add_snaps(spark, cenv, [S("camp_203", d, days_ago(19, 1), 3000, 30, 1, "0.0000", "meta")])
    build(cenv)
    n = rows(spark, cenv)[("camp_203", d)]
    assert (n["cpa"], n["roas_placed"], n["roas_delivered"], n["cost_per_delivered_order"]) == (None,) * 4
    assert n["aov"] == Decimal("60.00") and n["conversion_rate"] == Decimal("0.0333")
    assert assert_matches_reference(spark, cenv) == 3


def test_open_rate_counts_orders_older_than_14_days_only(spark, cenv):
    order(spark, cenv, "old", "c1", days_ago(15), "camp_201", "10.00")
    order(spark, cenv, "new", "c2", days_ago(13), "camp_201", "10.00")
    order(spark, cenv, "old_done", "c3", days_ago(15, 11), "camp_202", "10.00", DELIVERED)
    order(spark, cenv, "old_cancelled", "c3", days_ago(15, 12), "camp_202", "10.00", CANCELLED)
    build(cenv)
    got = rows(spark, cenv)
    assert got[("camp_201", day(15))]["open_rate"] == Decimal("1.0000")
    assert got[("camp_201", day(13))]["open_rate"] == Decimal("0.0000")
    assert got[("camp_202", day(15))]["open_rate"] == Decimal("0.0000")  # delivered and cancelled are not open
    # the reference with an as_of 2 days earlier no longer sees the 15 day old order as open
    ref = {(r["campaign_id"], r["report_date"]): r["open_rate"]
           for r in reference(spark, cenv, as_of=datetime.now(timezone.utc) - timedelta(days=2)).collect()}
    assert ref[("camp_201", day(15))] == Decimal("0.0000")
    assert_matches_reference(spark, cenv)


# ------------------------------------------------------------------ window, idempotence, deletes
def test_rerun_changes_nothing_late_status_updates_its_day_and_vanished_days_are_deleted(spark, cenv):
    d = day(20)
    order(spark, cenv, "o1", "c1", days_ago(20, 10), "camp_201", "100.00")
    order(spark, cenv, "o2", "c2", days_ago(20, 11), "camp_201", "200.00", DELIVERED)
    order(spark, cenv, "o3", "c1", days_ago(25, 10), "camp_202", "30.00")
    add_snaps(spark, cenv, [S("camp_201", d, days_ago(20, 23), 1000, 10, 1, "40.0000"),
                            S("camp_203", day(5), days_ago(5, 23), 100, 1, 0, "5.0000", "meta")])
    build(cenv)
    first = rows(spark, cenv)
    assert set(first) == {("camp_201", d), ("camp_202", day(25)), ("camp_203", day(5))}
    build(cenv)
    assert rows(spark, cenv) == first  # twice: nothing changes
    assert (first[("camp_201", d)]["orders_delivered"], first[("camp_201", d)]["revenue_delivered"]) == (
        1, Decimal("200.00"))

    # a late delivered row for o1 (placed 20 days ago) arrives today; the window is only today
    today = datetime.now(timezone.utc).date()
    load(spark, cenv.silver, "silver_order_status",
              [status_row("o1", "delivered", datetime.combine(today, time(9), tzinfo=timezone.utc), customer="c1")])
    run_build(cenv, steps=STEPS, from_date=today.isoformat(), to_date=today.isoformat())
    second = rows(spark, cenv)
    a = second[("camp_201", d)]
    assert (a["orders_delivered"], a["revenue_delivered"], a["roas_delivered"], a["cost_per_delivered_order"]) == (
        2, Decimal("300.00"), Decimal("7.50"), Decimal("20.00"))
    assert {k: v for k, v in second.items() if k != ("camp_201", d)} == {
        k: v for k, v in first.items() if k != ("camp_201", d)}  # only the order day moved
    assert_matches_reference(spark, cenv)

    # camp_203 / day 5 vanishes (its snapshot is gone): deleted when the window covers the day ...
    spark.sql("DELETE FROM %s.silver_campaign_snapshots WHERE campaign_id = 'camp_203'" % cenv.silver)
    run_build(cenv, steps=STEPS, from_date=day(20).isoformat(), to_date=day(10).isoformat())
    assert ("camp_203", day(5)) in rows(spark, cenv)  # ... but not when the day is outside the window
    run_build(cenv, steps=STEPS, from_date=day(6).isoformat(), to_date=today.isoformat())
    third = rows(spark, cenv)
    assert ("camp_203", day(5)) not in third
    # the order-only row of day 25 is outside both windows and stays
    assert ("camp_202", day(25)) in third
    # an order that loses its attribution inside the window: the campaign-day disappears
    spark.sql("DELETE FROM %s.fact_attribution WHERE order_id = 'o3'" % cenv.gold)
    run_build(cenv, steps=["agg_campaign_daily"], from_date=day(25).isoformat(), to_date=day(25).isoformat())
    assert ("camp_202", day(25)) not in rows(spark, cenv)


# ------------------------------------------------------------------ parity on the simulator dataset
@pytest.fixture(scope="module")
def parity(spark, tmp_path_factory):
    base = tmp_path_factory.mktemp("agg_campaign")
    lake = str(base / "lake")
    msgs, _ = simulated(30_000, seed=11)
    process_batch(spark, bronze_df(spark, msgs), lake, None, now=NOW)
    seed(spark, lake, datetime(2025, 1, 1, tzinfo=timezone.utc), CATALOG, CUSTOMERS)
    store = make_env(spark, str(base / "wh"), "silver_cmp", "gold_cmp", gold_tables=GOLD_TABLES)
    for table in SILVER_FACTS:
        load(spark, "silver_cmp", table, df=spark.read.format("delta").load(silver_path(lake, table)))
    run_build(store, full=True, steps=["dim_customer", "dim_product", "dim_campaign", "dim_date", "fact_orders",
                                       "fact_order_lines", "fact_attribution"])
    # snapshots: the three simulator campaigns, every order day plus 3 days before and after, 2 to 3 pulls a day
    order_days = sorted({r[0] for r in spark.sql(
        "SELECT DISTINCT to_date(CAST(date_key AS STRING), 'yyyyMMdd') FROM gold_cmp.fact_orders").collect()})
    rng = random.Random(11)
    with_orders = sorted((r[0], r[1]) for r in spark.sql(
        "SELECT a.campaign_id, to_date(CAST(f.date_key AS STRING), 'yyyyMMdd') FROM gold_cmp.fact_orders f "
        "JOIN gold_cmp.fact_attribution a ON a.order_id = f.order_id AND a.model = 'last_touch' "
        "WHERE a.campaign_id IS NOT NULL GROUP BY 1, 2").collect())
    no_ad = with_orders[0]  # this campaign-day has orders but no ad row at all
    lo, hi = order_days[0] - timedelta(days=3), order_days[-1] + timedelta(days=3)
    data, d = [], lo
    while d <= hi:
        for c in CAMPAIGNS:
            if (c["campaign_id"], d) == no_ad or rng.random() < 0.1:
                continue  # some campaign-days have no ad row
            base_i = rng.randint(500, 20000)
            for k in range(rng.choice((2, 3))):
                frac = (k + 1) / 3.0
                imp = int(base_i * frac)
                clicks = int(imp * rng.uniform(0.01, 0.05))
                pulled = datetime.combine(d, time(6 + 5 * k), tzinfo=timezone.utc)
                data.append(S(c["campaign_id"], d, pulled, imp, clicks, int(clicks * 0.04),
                              round(imp * rng.uniform(0.01, 0.03), 4), c["platform"]))
        d += timedelta(days=1)
    tie_campaign = next(c["campaign_id"] for c in CAMPAIGNS if c["campaign_id"] != no_ad[0])
    data.append(S(tie_campaign, order_days[0], datetime.combine(order_days[0], time(6), tzinfo=timezone.utc), 1, 1, 0,
                  "1.0000"))  # a tie on pulled_at with an earlier generated pull at 06:00 (smaller impressions loses)
    data.append(S(None, order_days[0], datetime(2026, 1, 1, tzinfo=timezone.utc), 1, 1, 1, "1.0000"))
    add_snaps(spark, store, data)
    result = run_build(store, full=True, steps=["agg_campaign_daily"])
    return {"store": store, "result": result, "snapshots": len(data), "order_days": order_days}


def test_sql_equals_reference_on_the_simulator_dataset(spark, parity):
    store = parity["store"]
    dim = spark.table("gold_cmp.dim_customer").select("customer_id", "customer_key", "valid_from", "valid_to")
    fo = build_fact_orders(spark.table("silver_cmp.silver_orders"), spark.table("silver_cmp.silver_order_status"), dim)
    att = build_fact_attribution(
        spark.table("silver_cmp.silver_orders").join(fo.select("order_id"), "order_id", "left_semi"),
        build_clicks(spark.table("silver_cmp.silver_events"), spark.table("gold_cmp.dim_campaign")))
    fo = build_fact_orders(spark.table("silver_cmp.silver_orders"), spark.table("silver_cmp.silver_order_status"), dim,
                           attribution=att)
    as_of = datetime.now(timezone.utc)
    ref = build_agg_campaign_daily(fo, att, spark.table("silver_cmp.silver_campaign_snapshots"), as_of)
    cols = ref.columns
    n_sql, n_ref, only_sql, only_ref, mism, samples = compare(
        spark.table("gold_cmp.agg_campaign_daily").select(*cols), ref, KEYS, cols)
    print("\nPARITY agg_campaign_daily rows_sql=%d rows_ref=%d only_sql=%d only_ref=%d columns=%d mismatches=%d "
          "snapshot_rows=%d order_days=%d" % (n_sql, n_ref, only_sql, only_ref, len(cols), sum(mism.values()),
                                              parity["snapshots"], len(parity["order_days"])))
    assert n_sql == n_ref >= 15  # the dataset has ONE order day (see order_days in the print); 3 campaigns x 7 days
    assert only_sql == only_ref == 0
    assert sum(mism.values()) == 0, (mism, samples)
    g = spark.table("gold_cmp.agg_campaign_daily")
    assert g.where("spend IS NULL AND orders_placed > 0").count() > 0      # orders without an ad row
    assert g.where("spend IS NOT NULL AND orders_placed = 0").count() > 0  # ad rows without orders
    assert g.where("spend IS NOT NULL AND orders_placed > 0").count() > 0
    attributed = spark.table("gold_cmp.fact_attribution").where(
        "model = 'last_touch' AND campaign_id IS NOT NULL AND campaign_id <> 'unknown'").count()
    assert g.agg(F.sum("orders_placed")).first()[0] == attributed  # every attributed order is on exactly one row
    assert g.agg(F.sum("orders_placed")).first()[0] > 0


def test_rebuild_of_the_dataset_is_identical_and_step_result_has_rows(spark, parity):
    store = parity["store"]
    assert parity["result"]["steps"]["agg_campaign_daily"]["status"] == "ok"
    before = sorted(map(tuple, spark.table("gold_cmp.agg_campaign_daily").collect()), key=repr)
    result = run_build(store, full=True, steps=["agg_campaign_daily"])
    after = sorted(map(tuple, spark.table("gold_cmp.agg_campaign_daily").collect()), key=repr)
    print("\nIDEMPOTENCE agg_campaign_daily rows=%d" % len(after))
    assert before == after and result["steps"]["agg_campaign_daily"]["rows"] == len(after)
