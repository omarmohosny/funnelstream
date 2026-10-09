"""Attribution Gold step on local Spark with Delta (run in Docker: RUN_SPARK_TESTS=1).

The rendered warehouse/gold/07_fact_attribution.sql runs through warehouse.gold_build.run_build against Delta
tables and is compared with the reference streaming/gold/attribution.py:
  * the 30,000-event simulator dataset (seed 11, noise on): full outer join, zero mismatches;
  * hand-built cases with explicit expected values (own session first, 7-day customer window, anonymous orders,
    bots, ties, unresolved UTM, ...), checked against the expected table AND against the reference;
  * reruns, late click rows and vanished orders.
"""
import itertools
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

pytestmark = pytest.mark.spark

from pyspark.sql import functions as F  # noqa: E402

from scripts.seed_silver_reference import seed  # noqa: E402
from streaming.gold.attribution import (build_clicks, build_fact_attribution,  # noqa: E402
                                        report_unresolved_clicks)
from streaming.gold.bots import mark_bot_sessions  # noqa: E402
from streaming.gold.orders import build_fact_orders  # noqa: E402
from streaming.jobs.silver_clean import process_batch  # noqa: E402
from streaming.silver.writer import silver_path  # noqa: E402
from tests.test_gold_build_spark import (NO_CHECKS as NO_STEPS_CHECKS, SILVER_FACTS, compare, ev, load, make_env,  # noqa: E402
                                         small_shuffles)  # noqa: F401  (small_shuffles is an autouse fixture)
from tests.test_gold_reference import CATALOG, CUSTOMERS, NOW, bronze_df, order_row, simulated, status_row, t  # noqa: E402
from warehouse.gold_build import run_build  # noqa: E402

ATT_TABLES = ("dim_customer", "dim_campaign", "fact_orders", "fact_attribution")
ATT_STEPS = ["dim_campaign", "fact_orders", "fact_attribution"]
ATT_COLS = ["order_id", "model", "campaign_id", "click_id", "order_total"]


def att_rows(spark, schema):
    return {(r["order_id"], r["model"]): (r["campaign_id"], r["click_id"], r["order_total"])
            for r in spark.table("%s.fact_attribution" % schema).collect()}


def fo_campaign(spark, schema):
    return {r["order_id"]: r["campaign_id"] for r in spark.table("%s.fact_orders" % schema).collect()}


def reference(spark, schema_silver, schema_gold, dim_customer=None):
    """(clicks, fact_attribution, fact_orders) of the reference builders over the Silver mirror."""
    orders, status = spark.table(schema_silver + ".silver_orders"), spark.table(schema_silver + ".silver_order_status")
    events = spark.table(schema_silver + ".silver_events")
    base = build_fact_orders(orders, status, dim_customer)
    clicks = build_clicks(events, spark.table(schema_gold + ".dim_campaign"))
    att = build_fact_attribution(orders.join(base.select("order_id"), "order_id", "left_semi"), clicks)
    return clicks, att, build_fact_orders(orders, status, dim_customer, attribution=att)


# ------------------------------------------------------------------ the 30,000-event dataset
@pytest.fixture(scope="module")
def e2e(spark, tmp_path_factory):
    base = tmp_path_factory.mktemp("attribution")
    lake = str(base / "lake")
    msgs, _ = simulated(30_000)
    process_batch(spark, bronze_df(spark, msgs), lake, None, now=NOW)
    seed(spark, lake, datetime(2025, 1, 1, tzinfo=timezone.utc), CATALOG, CUSTOMERS)
    store = make_env(spark, str(base / "wh"), "silver_att", "gold_att")
    for table in SILVER_FACTS:
        load(spark, "silver_att", table, df=spark.read.format("delta").load(silver_path(lake, table)))
    result = run_build(store, full=True, steps=NO_STEPS_CHECKS)
    return {"store": store, "result": result}


def test_attribution_equals_reference_column_by_column_on_the_dataset(spark, e2e):
    dim = dim_frame(spark, "dim_customer", "customer_key", "customer_id") if False else \
        spark.table("gold_att.dim_customer").select("customer_id", "customer_key", "valid_from", "valid_to")
    clicks, ref_att, ref_fo = reference(spark, "silver_att", "gold_att", dim)
    gold = spark.table("gold_att.fact_attribution")
    n_sql, n_ref, only_sql, only_ref, mism, samples = compare(gold.select(*ATT_COLS), ref_att.select(*ATT_COLS),
                                                              ["order_id", "model"], ATT_COLS)
    n_orders = spark.table("gold_att.fact_orders").count()
    print("\nPARITY fact_attribution rows_sql=%d rows_ref=%d only_sql=%d only_ref=%d columns=%d mismatches=%d "
          "orders=%d" % (n_sql, n_ref, only_sql, only_ref, len(ATT_COLS), sum(mism.values()), n_orders))
    assert n_sql == n_ref == 2 * n_orders > 300
    assert only_sql == only_ref == 0
    assert sum(mism.values()) == 0, (mism, samples)
    assert gold.columns == ATT_COLS
    per_order = gold.groupBy("order_id").agg(F.count("*").alias("n"), F.countDistinct("model").alias("m"))
    assert per_order.where("n <> 2 OR m <> 2").count() == 0
    assert set(r["model"] for r in gold.select("model").distinct().collect()) == {"last_touch", "first_touch"}
    # fact_orders.campaign_id is the last_touch campaign (full row parity with the reference that applies it)
    cols = ref_fo.columns
    fo = compare(spark.table("gold_att.fact_orders").select(*cols), ref_fo, ["order_id"], cols)
    print("PARITY fact_orders(with campaign_id) rows_sql=%d rows_ref=%d only_sql=%d only_ref=%d mismatches=%d" % (
        fo[0], fo[1], fo[2], fo[3], sum(fo[4].values())))
    assert fo[0] == fo[1] == n_orders and fo[2] == fo[3] == 0 and sum(fo[4].values()) == 0, (fo[4], fo[5])
    lt = {r["order_id"]: r["campaign_id"] for r in gold.where("model = 'last_touch'").collect()}
    assert fo_campaign(spark, "gold_att") == lt


def test_dataset_shares_are_measured_and_the_reported_counts_match_the_reference(spark, e2e):
    clicks, ref_att, _ = reference(spark, "silver_att", "gold_att")
    gold = spark.table("gold_att.fact_attribution")
    fo = {r["order_id"]: r["click_id"] for r in spark.table("gold_att.fact_orders").collect()}
    last = {r["order_id"]: r for r in gold.where("model = 'last_touch'").collect()}
    first = {r["order_id"]: r for r in gold.where("model = 'first_touch'").collect()}
    n = len(last)
    direct = sum(r["campaign_id"] is None for r in last.values())
    own = sum(r["click_id"] is not None and r["click_id"] == fo[o] for o, r in last.items())
    fallback = n - direct - own
    differ = sum(last[o]["click_id"] != first[o]["click_id"] for o in last)
    n_clicks = clicks.where("campaign_id IS NOT NULL").count()
    bots = mark_bot_sessions(spark.table("silver_att.silver_events")).where("is_bot").select("session_id")         .distinct().count()
    print("\nSHARES orders=%d direct=%d (%.1f%%) attributed=%d (%.1f%%) own_session=%d customer_fallback=%d "
          "first_vs_last_differ=%d clicks=%d unresolved=%d bot_sessions=%d" % (
              n, direct, 100.0 * direct / n, n - direct, 100.0 * (n - direct) / n, own, fallback, differ, n_clicks,
              report_unresolved_clicks(clicks), bots))
    assert n > 150 and direct < n and own > 0
    assert all(first[o]["campaign_id"] is None for o, r in last.items() if r["campaign_id"] is None)  # same direct set
    rep = e2e["result"]["reported"]
    assert rep["orders_direct"] == direct
    assert rep["orders_without_click"] == n - own
    assert rep["unresolved_clicks"] == report_unresolved_clicks(clicks)
    assert e2e["result"]["steps"]["fact_attribution"]["rows"] == 2 * n


def test_second_full_build_and_a_wide_window_build_change_nothing(spark, e2e):
    store = e2e["store"]

    def snap():
        return {n: sorted(map(tuple, spark.table("gold_att.%s" % n).collect()), key=repr)
                for n in ("fact_orders", "fact_attribution")}

    before = snap()
    run_build(store, full=True, steps=NO_STEPS_CHECKS)
    assert snap() == before
    run_build(store, from_date="2026-07-01", to_date="2026-12-31", steps=["fact_orders", "fact_attribution"])
    assert snap() == before
    run_build(store, from_date="2026-07-01", to_date="2026-12-31", steps=["fact_attribution"])
    assert snap() == before
    assert len(before["fact_attribution"]) == 2 * len(before["fact_orders"])


# ------------------------------------------------------------------ hand-built cases
_ids = itertools.count(1000)
T0 = t(10, 12)
EID, RAM, RET = "eid_sale_1", "ramadan_offer_1", "retarget_cart_1"
CAMP = {EID: "camp_201", RAM: "camp_202", RET: "camp_203"}


def click(session, when, customer, utm=EID, click_id=None, etype="page_view", **kw):
    return ev(next(_ids), session, when, etype, customer_id=customer, utm_campaign=utm, click_id=click_id, **kw)


def plain(session, when, customer, etype="add_to_cart"):
    return ev(next(_ids), session, when, etype, customer_id=customer)


def put_order(spark, schema, oid, customer, created, session, click_id, total="100.00"):
    row = order_row(oid, customer=customer, created=created, total=total, session=session)
    row["click_id"] = click_id
    load(spark, schema, "silver_orders", [row])
    load(spark, schema, "silver_order_status", [status_row(oid, "order_created", created, customer=customer)])


def small_env(spark, tmp_path, name):
    return make_env(spark, str(tmp_path), "s_" + name, "g_" + name,
                    gold_tables=("dim_customer", "dim_product", "dim_campaign", "fact_orders", "fact_attribution"))


D = timedelta


def cases():
    """(events, orders, expected). orders: oid -> (customer, session, order click_id); every order is at T0.
    expected: oid -> (last campaign, last click, first campaign, first click)."""
    e, o, x = [], {}, {}

    def order(oid, cust, session, clk, exp):
        o[oid] = (cust, session, clk)
        x[oid] = exp

    # own session click wins over an OLDER click (A) and a NEWER click (C) of the same customer
    e += [click("sA", T0 - D(hours=30), "c_own", EID, "clk_A"), click("sB", T0 - D(hours=5), "c_own", RAM, "clk_B"),
          click("sC", T0 - D(hours=1), "c_own", RET, "clk_C")]
    order("o_own", "c_own", "sB", "clk_B", ("camp_202", "clk_B", "camp_202", "clk_B"))
    # no own click: a click 6 days 23 hours earlier counts, 7 days 1 hour earlier does not
    e += [click("s_in", T0 - D(days=6, hours=23), "c_in", EID, "clk_in"),
          click("s_out", T0 - D(days=7, hours=1), "c_out", EID, "clk_out")]
    order("o_in", "c_in", "s_in_order", None, ("camp_201", "clk_in", "camp_201", "clk_in"))
    order("o_out", "c_out", "s_out_order", None, (None, None, None, None))
    # window edges: exactly 7 days before counts (inclusive), exactly at the order time does not (exclusive)
    e += [click("s_edge7", T0 - D(days=7), "c_edge7", RAM, "clk_edge7"),
          click("s_edge0", T0, "c_edge0", RAM, "clk_edge0")]
    order("o_edge7", "c_edge7", "s_edge7_order", None, ("camp_202", "clk_edge7", "camp_202", "clk_edge7"))
    order("o_edge0", "c_edge0", "s_edge0_order", None, (None, None, None, None))
    # anonymous orders: never step 2 (the click of another anonymous session is not theirs), but step 1 works
    e += [click("s_anon_other", T0 - D(hours=1), None, EID, "clk_anon_other"),
          click("s_anon_own", T0 - D(hours=2), None, RET, "clk_anon_own")]
    order("o_anon", None, "s_anon_order", None, (None, None, None, None))
    order("o_anon_own", None, "s_anon_own", "clk_anon_own", ("camp_203", "clk_anon_own", "camp_203", "clk_anon_own"))
    # first touch = earliest click in the window, last touch = latest
    e += [click("s_m1", T0 - D(days=5), "c_multi", EID, "clk_m1"), click("s_m2", T0 - D(days=3), "c_multi", RAM, "clk_m2"),
          click("s_m3", T0 - D(days=1), "c_multi", RET, "clk_m3"), click("s_m0", T0 - D(days=9), "c_multi", RAM, "clk_m0")]
    order("o_multi", "c_multi", "s_multi_order", None, ("camp_203", "clk_m3", "camp_201", "clk_m1"))
    # a click in the future of the order is ignored
    e += [click("s_fut", T0 + D(hours=1), "c_fut", EID, "clk_fut")]
    order("o_fut", "c_fut", "s_fut_order", None, (None, None, None, None))
    # a bot session's click is ignored: alone -> direct, with a clean older click -> that click is the fallback
    bot1 = [click("s_bot1", T0 - D(hours=2), "c_bot1", RET, "clk_bot1")]
    bot1 += [plain("s_bot1", (T0 - D(hours=2)).replace(microsecond=0) + D(microseconds=1000 * i), "c_bot1", "page_view")
             for i in range(1, 101)]
    bot2 = [click("s_bot2", T0 - D(days=1), "c_bot2", RET, "clk_bot2"),
            click("s_ok2", T0 - D(days=3), "c_bot2", EID, "clk_ok2")]
    bot2 += [plain("s_bot2", (T0 - D(days=1)) + D(microseconds=1000 * i), "c_bot2", "page_view") for i in range(1, 101)]
    e += bot1 + bot2
    order("o_bot1", "c_bot1", "s_bot1", "clk_bot1", (None, None, None, None))
    order("o_bot2", "c_bot2", "s_bot2", "clk_bot2", ("camp_201", "clk_ok2", "camp_201", "clk_ok2"))
    # two clicks at the same time: the smaller click_id wins in both models
    e += [click("s_t1", T0 - D(hours=2), "c_tie", RAM, "clk_b"), click("s_t2", T0 - D(hours=2), "c_tie", RET, "clk_a")]
    order("o_tie", "c_tie", "s_tie_order", None, ("camp_203", "clk_a", "camp_203", "clk_a"))
    # an unresolved utm_campaign is not a click, a populated silver_events.campaign_id resolves it, and only a
    # page_view or product_view can be the click
    e += [click("s_un", T0 - D(hours=1), "c_un", "mystery_cmp", "clk_un"),
          click("s_cid", T0 - D(hours=1), "c_cid", "mystery_two", "clk_cid", campaign_id="camp_202"),
          click("s_ty", T0 - D(hours=3), "c_ty", RET, "clk_ty", etype="add_to_cart"),
          click("s_pv", T0 - D(hours=4), "c_pv", EID, "clk_pv", etype="product_view")]
    order("o_un", "c_un", "s_un", "clk_un", (None, None, None, None))
    order("o_cid", "c_cid", "s_cid", "clk_cid", ("camp_202", "clk_cid", "camp_202", "clk_cid"))
    order("o_ty", "c_ty", "s_ty", "clk_ty", (None, None, None, None))
    order("o_pv", "c_pv", "s_pv", "clk_pv", ("camp_201", "clk_pv", "camp_201", "clk_pv"))
    # the click of a session is its FIRST utm event: the later product_view of another campaign does not count
    e += [click("s_f", T0 - D(hours=6), "c_f", EID, "clk_f1"),
          click("s_f", T0 - D(hours=5), "c_f", RAM, "clk_f2", etype="product_view")]
    order("o_f", "c_f", "s_f", "clk_f1", ("camp_201", "clk_f1", "camp_201", "clk_f1"))
    # the order's click_id points to nothing: fall back to the customer's clicks
    e += [click("s_g1", T0 - D(days=2), "c_g", RAM, "clk_g1")]
    order("o_g", "c_g", "s_g_order", "clk_missing", ("camp_202", "clk_g1", "camp_202", "clk_g1"))
    return e, o, x


@pytest.fixture(scope="module")
def case_env(spark, tmp_path_factory):
    store = small_env(spark, tmp_path_factory.mktemp("cases"), "cases")
    events, orders, expected = cases()
    load(spark, store.silver, "silver_events", events)
    for oid, (cust, session, clk) in orders.items():
        put_order(spark, store.silver, oid, cust, T0, session, clk)
    result = run_build(store, full=True, steps=ATT_STEPS)
    return {"store": store, "orders": orders, "expected": expected, "result": result}


def test_hand_built_cases_have_the_expected_attribution_in_both_models(spark, case_env):
    store, expected = case_env["store"], case_env["expected"]
    got = att_rows(spark, store.gold)
    assert len(got) == 2 * len(expected)
    wrong = {}
    for oid, (lc, lk, fc, fk) in expected.items():
        for model, want in (("last_touch", (lc, lk)), ("first_touch", (fc, fk))):
            if got[(oid, model)][:2] != want:
                wrong[(oid, model)] = (got[(oid, model)][:2], want)
    assert not wrong, wrong
    assert all(v[2] == Decimal("100.00") for v in got.values())  # order_total copied


def test_hand_built_cases_equal_the_reference_and_fact_orders_gets_the_last_touch_campaign(spark, case_env):
    store, expected = case_env["store"], case_env["expected"]
    _, ref_att, _ = reference(spark, store.silver, store.gold)
    n_sql, n_ref, only_sql, only_ref, mism, samples = compare(
        spark.table(store.gold + ".fact_attribution").select(*ATT_COLS), ref_att.select(*ATT_COLS),
        ["order_id", "model"], ATT_COLS)
    assert n_sql == n_ref == 2 * len(expected) and only_sql == only_ref == 0 and sum(mism.values()) == 0, (
        mism, samples)
    assert fo_campaign(spark, store.gold) == {oid: x[0] for oid, x in expected.items()}


def test_hand_built_reported_counts(spark, case_env):
    store, expected, orders = case_env["store"], case_env["expected"], case_env["orders"]
    rep = case_env["result"]["reported"]
    assert rep["orders_direct"] == sum(x[0] is None for x in expected.values())
    assert rep["orders_without_click"] == sum(x[1] is None or x[1] != orders[o][2] for o, x in expected.items())
    assert rep["unresolved_clicks"] == 1  # mystery_cmp only: the bot sessions and the add_to_cart are not clicks
    clicks = build_clicks(spark.table(store.silver + ".silver_events"), spark.table(store.gold + ".dim_campaign"))
    assert report_unresolved_clicks(clicks) == 1
    assert clicks.where("session_id LIKE 's_bot%'").count() == 0 and clicks.where("session_id = 's_ty'").count() == 0
    assert clicks.where("session_id = 's_f'").first()["click_id"] == "clk_f1"


def test_rerunning_the_step_and_the_whole_build_changes_nothing(spark, case_env):
    store = case_env["store"]

    def snap():
        return {n: sorted(map(tuple, spark.table("%s.%s" % (store.gold, n)).collect()), key=repr)
                for n in ("fact_orders", "fact_attribution")}

    before = snap()
    run_build(store, full=True, steps=ATT_STEPS)
    run_build(store, full=True, steps=ATT_STEPS)
    assert snap() == before
    run_build(store, from_date="2026-10-10", to_date="2026-10-10", steps=ATT_STEPS)
    assert snap() == before
    # the fact_orders step recomputes the orders but never overwrites the campaign_id the attribution step set
    run_build(store, full=True, steps=["fact_orders"])
    assert fo_campaign(spark, store.gold) == {oid: x[0] for oid, x in case_env["expected"].items()}
    assert snap() == before


# ------------------------------------------------------------------ late data and vanished orders
def three_orders(spark, store):
    load(spark, store.silver, "silver_customers", [{
        "customer_id": c, "updated_at": datetime(2025, 1, 1), "country": "EG", "city": "Cairo", "district": "Maadi",
        "signup_date": date(2025, 1, 1), "primary_device": "mobile", "marketing_consent": True} for c in ("c1", "c2")])
    put_order(spark, store.silver, "o_a", "c1", t(1, 10), "sa", "clk_a")
    put_order(spark, store.silver, "o_b", "c2", t(2, 10), "sb", "clk_b")
    put_order(spark, store.silver, "o_c", "c1", t(3, 10), "sc", None)
    run_build(store, full=True, steps=["dim_customer"] + ATT_STEPS)


def changed(before, after):
    return sorted({k[0] for k in after if after.get(k) != before.get(k)} | {k[0] for k in before if k not in after})


def test_a_late_click_row_for_an_old_session_changes_only_that_order_when_the_window_covers_it(spark, tmp_path):
    store = small_env(spark, tmp_path, "late_own")
    three_orders(spark, store)
    before = att_rows(spark, store.gold)
    assert all(v[:2] == (None, None) for v in before.values())  # no click rows at all: everything is direct
    # the click event of order o_b's session arrives on Oct 6 (kafka_timestamp) with an event_time of Oct 2
    load(spark, store.silver, "silver_events", [
        click("sb", t(2, 8), "c2", EID, "clk_b", kafka=t(6, 9))])
    run_build(store, from_date="2026-10-20", to_date="2026-10-21", steps=["fact_orders", "fact_attribution"])
    assert att_rows(spark, store.gold) == before  # a window that does not cover it does not see it
    run_build(store, from_date="2026-10-06", to_date="2026-10-06", steps=["fact_orders", "fact_attribution"])
    after = att_rows(spark, store.gold)
    assert changed(before, after) == ["o_b"]
    assert after[("o_b", "last_touch")][:2] == ("camp_201", "clk_b") == after[("o_b", "first_touch")][:2]
    assert fo_campaign(spark, store.gold) == {"o_a": None, "o_b": "camp_201", "o_c": None}
    snap = att_rows(spark, store.gold)
    run_build(store, from_date="2026-10-06", to_date="2026-10-06", steps=["fact_orders", "fact_attribution"])
    assert att_rows(spark, store.gold) == snap
    # a later fact_orders run over the order's day keeps the campaign_id
    run_build(store, from_date="2026-10-02", to_date="2026-10-02", steps=["fact_orders"])
    assert fo_campaign(spark, store.gold)["o_b"] == "camp_201"


def test_a_late_click_of_another_session_of_the_customer_changes_only_the_later_orders_in_its_week(spark, tmp_path):
    store = small_env(spark, tmp_path, "late_fb")
    three_orders(spark, store)
    before = att_rows(spark, store.gold)
    # c1 clicked in another session on Oct 2 (after order o_a, before order o_c); the row arrives on Oct 6
    load(spark, store.silver, "silver_events", [click("s_late", t(2, 20), "c1", RAM, "clk_late", kafka=t(6, 9))])
    run_build(store, from_date="2026-10-06", to_date="2026-10-06", steps=["fact_orders", "fact_attribution"])
    after = att_rows(spark, store.gold)
    assert changed(before, after) == ["o_c"]
    assert after[("o_c", "last_touch")][:2] == ("camp_202", "clk_late")
    assert fo_campaign(spark, store.gold) == {"o_a": None, "o_b": None, "o_c": "camp_202"}


def test_a_session_that_turns_into_a_bot_with_late_events_loses_its_click(spark, tmp_path):
    store = small_env(spark, tmp_path, "late_bot")
    three_orders(spark, store)
    load(spark, store.silver, "silver_events", [click("sb", t(2, 8), "c2", EID, "clk_b", kafka=t(2, 8))])
    run_build(store, full=True, steps=ATT_STEPS)
    assert att_rows(spark, store.gold)[("o_b", "last_touch")][:2] == ("camp_201", "clk_b")
    burst = [ev(next(_ids), "sb", t(2, 8, 0, 1) + D(microseconds=1000 * i), "page_view", customer_id="c2",
                kafka=t(6, 9)) for i in range(120)]  # 120 events in 0.12 s arrive late on Oct 6
    load(spark, store.silver, "silver_events", burst)
    run_build(store, from_date="2026-10-06", to_date="2026-10-06", steps=ATT_STEPS)
    after = att_rows(spark, store.gold)
    assert after[("o_b", "last_touch")][:2] == (None, None) and after[("o_b", "first_touch")][:2] == (None, None)
    assert fo_campaign(spark, store.gold)["o_b"] is None


def test_an_order_that_left_fact_orders_loses_both_attribution_rows_and_nothing_else_changes(spark, tmp_path):
    store = small_env(spark, tmp_path, "gone")
    three_orders(spark, store)
    load(spark, store.silver, "silver_events", [click("sb", t(2, 8), "c2", EID, "clk_b")])
    run_build(store, full=True, steps=ATT_STEPS)
    before = att_rows(spark, store.gold)
    assert len(before) == 6
    spark.sql("DELETE FROM %s.fact_orders WHERE order_id = 'o_b'" % store.gold)
    run_build(store, from_date="2026-10-20", to_date="2026-10-20", steps=["fact_attribution"])
    after = att_rows(spark, store.gold)
    assert sorted(after) == [("o_a", "first_touch"), ("o_a", "last_touch"), ("o_c", "first_touch"),
                             ("o_c", "last_touch")]
    assert {k: v for k, v in after.items()} == {k: v for k, v in before.items() if k[0] != "o_b"}


def test_an_order_with_no_attribution_row_is_picked_up_by_any_window(spark, tmp_path):
    store = small_env(spark, tmp_path, "missing")
    three_orders(spark, store)
    load(spark, store.silver, "silver_events", [click("sb", t(2, 8), "c2", EID, "clk_b")])
    run_build(store, full=True, steps=ATT_STEPS)
    spark.sql("DELETE FROM %s.fact_attribution" % store.gold)  # fact_orders exists, fact_attribution is empty
    spark.sql("UPDATE %s.fact_orders SET campaign_id = NULL" % store.gold)
    assert att_rows(spark, store.gold) == {}
    run_build(store, from_date="2026-10-20", to_date="2026-10-20", steps=["fact_attribution"])
    got = att_rows(spark, store.gold)
    assert len(got) == 6 and got[("o_b", "last_touch")][:2] == ("camp_201", "clk_b")
    assert fo_campaign(spark, store.gold)["o_b"] == "camp_201"
