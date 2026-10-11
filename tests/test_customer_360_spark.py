"""customer_360: Gold SQL step (warehouse/gold/13_customer_360.sql) on local Spark with Delta (run in Docker:
RUN_SPARK_TESTS=1).

One hand-built world with hand-computed expectations: three customers, three products in two categories, a bot session,
a cancelled order, a tie between two categories and a customer with no activity at all. The step reads silver_events,
gold.fact_orders, gold.fact_order_lines and gold.dim_customer / gold.dim_product (built by the real steps).
"""
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

pytestmark = pytest.mark.spark

from tests.test_gold_build_spark import load, make_env  # noqa: E402
from tests.test_gold_reference import ev, order_row, status_row, t  # noqa: E402
from warehouse.gold_build import run_build  # noqa: E402

STEPS = ["dim_customer", "dim_product", "fact_orders", "fact_order_lines", "customer_360"]
GOLD_TABLES = ("dim_customer", "dim_product", "dim_campaign", "dim_date", "fact_orders", "fact_order_lines",
               "fact_attribution", "customer_360")
MONTH = {"from_date": "2026-10-01", "to_date": "2026-10-31"}
UPDATED = datetime(2025, 1, 1)


@pytest.fixture(scope="module", autouse=True)
def small_shuffles(spark):
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    spark.conf.set("spark.databricks.delta.snapshotPartitions", "2")
    yield


_N = [0]


def E(customer, sid, when, etype, **kw):
    _N[0] += 1
    return ev(_N[0], sid, when, etype, customer_id=customer, **kw)


def put(spark, env, oid, created, customer, lines, statuses, payment="cod"):
    """An order with item rows (product_id, quantity, line_total) and status rows (status, hours after creation)."""
    total = sum(Decimal(x[2]) for x in lines)
    row = order_row(oid, customer=customer, created=created, total=str(total), payment=payment)
    load(spark, env.silver, "silver_orders", [row])
    load(spark, env.silver, "silver_order_status",
         [status_row(oid, s, created + timedelta(hours=h), customer=customer) for s, h in statuses])
    load(spark, env.silver, "silver_order_items", [
        {"order_id": oid, "line_no": str(i + 1), "product_id": p, "quantity": q, "unit_price": Decimal("10.00"),
         "line_total": Decimal(lt), "currency": "EGP", "order_time": created} for i, (p, q, lt) in enumerate(lines)])


@pytest.fixture
def world(spark, tmp_path, request):
    name = re.sub(r"\W", "_", request.node.name)[:30]
    env = make_env(spark, str(tmp_path), "s_" + name, "g_" + name, gold_tables=GOLD_TABLES)
    load(spark, env.silver, "silver_customers", [
        {"customer_id": c, "updated_at": UPDATED, "country": "EG", "city": city, "district": district,
         "signup_date": date(2025, 1, 1), "primary_device": "mobile", "marketing_consent": True}
        for c, city, district in (("c1", "Cairo", "Maadi"), ("c2", "Giza", "Dokki"), ("c3", "Cairo", "Nasr City"))])
    load(spark, env.silver, "silver_products", [
        {"product_id": p, "updated_at": UPDATED, "name": "n" + p, "category": cat, "price": Decimal("10.00"),
         "is_active": True} for p, cat in (("p1", "dairy"), ("p2", "dairy"), ("p3", "snacks"))])
    events = (
        # c1: two sessions; session s1 has 3 views, 1 cart, 1 checkout; session s2 has 1 view
        [E("c1", "s1", t(5, 9, 0, i), "product_view", product_id="p1") for i in range(3)]
        + [E("c1", "s1", t(5, 9, 1), "add_to_cart", product_id="p1"), E("c1", "s1", t(5, 9, 2), "checkout_started")]
        + [E("c1", "s2", t(7, 8), "product_view", product_id="p3")]
        # c2: a normal session with 2 views and a BOT session (150 page_view events in 5 seconds)
        + [E("c2", "s4", t(8, 9, 0, i), "product_view", product_id="p3") for i in range(2)]
        + [E("c2", "bot", t(8, 12, 0, 0) + timedelta(seconds=5 * i / 150), "page_view") for i in range(150)]
        # an anonymous visitor: no customer, no row effect
        + [E(None, "anon", t(5, 10), "product_view", product_id="p1")])
    load(spark, env.silver, "silver_events", events)
    put(spark, env, "o1", t(5, 10), "c1", [("p1", 2, "20.00")], [("order_created", 0), ("delivered", 2)], payment="cod")
    put(spark, env, "o2", t(6, 10), "c1", [("p3", 1, "30.00")], [("order_created", 0), ("order_cancelled", 1)],
        payment="card")
    put(spark, env, "o3", t(7, 10), "c1", [("p2", 1, "10.00")], [("order_created", 0)], payment="cod")
    put(spark, env, "o4", t(8, 10), "c2", [("p3", 1, "30.00")], [("order_created", 0), ("delivered", 3)], payment="card")
    return env


def customers(spark, env):
    return {r["customer_id"]: r for r in spark.table(env.gold + ".customer_360").collect()}


def test_counts_spend_dates_and_text_columns_of_a_hand_built_world(spark, world):
    result = run_build(world, steps=STEPS, **MONTH)
    assert result["steps"]["customer_360"]["status"] == "ok" and result["steps"]["customer_360"]["rows"] == 3
    got = customers(spark, world)
    assert set(got) == {"c1", "c2", "c3"}

    c1 = got["c1"]
    assert (c1["country"], c1["city"], c1["district"]) == ("EG", "Cairo", "Maadi")
    assert (c1["total_sessions"], c1["product_views"], c1["add_to_cart_count"], c1["checkouts_count"]) == (2, 4, 1, 1)
    assert c1["last_seen_at"].replace(tzinfo=timezone.utc) == t(7, 8)
    assert (c1["orders_count"], c1["delivered_count"], c1["cancelled_count"]) == (3, 1, 1)
    assert c1["total_spend"] == Decimal("30.00")        # o1 20.00 and o3 10.00; the cancelled o2 does not count
    assert c1["avg_order_value"] == Decimal("15.00")
    assert c1["first_order_at"].replace(tzinfo=timezone.utc) == t(5, 10)
    assert c1["last_order_at"].replace(tzinfo=timezone.utc) == t(7, 10)
    assert c1["is_repeat_buyer"] is True
    assert c1["preferred_payment_method"] == "cod"      # 2 cod against 1 card
    assert c1["top_category"] == "dairy"                # dairy 30.00 ties snacks 30.00 (cancelled line counts): alphabetical
    assert c1["days_since_last_order"] >= 0

    c2 = got["c2"]
    assert (c2["city"], c2["district"]) == ("Giza", "Dokki")
    assert (c2["total_sessions"], c2["product_views"]) == (1, 2), "the bot session is excluded"
    assert c2["last_seen_at"].replace(tzinfo=timezone.utc) == t(8, 9, 0, 1)
    assert (c2["orders_count"], c2["delivered_count"], c2["cancelled_count"]) == (1, 1, 0)
    assert (c2["total_spend"], c2["avg_order_value"]) == (Decimal("30.00"), Decimal("30.00"))
    assert (c2["is_repeat_buyer"], c2["preferred_payment_method"], c2["top_category"]) == (False, "card", "snacks")


def test_a_customer_without_events_or_orders_has_zero_counts_and_null_text(spark, world):
    run_build(world, steps=STEPS, **MONTH)
    c3 = customers(spark, world)["c3"]
    assert (c3["total_sessions"], c3["product_views"], c3["add_to_cart_count"], c3["checkouts_count"]) == (0, 0, 0, 0)
    assert (c3["orders_count"], c3["delivered_count"], c3["cancelled_count"]) == (0, 0, 0)
    assert c3["total_spend"] == Decimal("0.00")
    assert c3["is_repeat_buyer"] is False
    for column in ("avg_order_value", "first_order_at", "last_order_at", "last_seen_at", "days_since_last_order",
                   "top_category", "preferred_payment_method", "last_campaign_id"):
        assert c3[column] is None, column


def test_the_last_campaign_is_the_one_of_the_newest_order(spark, world):
    run_build(world, steps=STEPS, **MONTH)
    orders = {r["order_id"]: r["campaign_id"] for r in spark.table(world.gold + ".fact_orders").collect()}
    assert customers(spark, world)["c1"]["last_campaign_id"] == orders["o3"]


def test_days_since_last_order_counts_whole_days_to_today(spark, world):
    run_build(world, steps=STEPS, **MONTH)
    expected = (datetime.now(timezone.utc).date() - date(2026, 10, 7)).days
    assert abs(customers(spark, world)["c1"]["days_since_last_order"] - expected) <= 1  # a midnight between two calls


def test_rerunning_changes_nothing_and_a_new_order_updates_the_row(spark, world):
    run_build(world, steps=STEPS, **MONTH)
    before = {k: r.asDict() for k, r in customers(spark, world).items()}
    run_build(world, steps=["customer_360"], **MONTH)
    assert {k: r.asDict() for k, r in customers(spark, world).items()} == before
    put(spark, world, "o5", t(9, 10), "c2", [("p1", 1, "10.00")], [("order_created", 0), ("delivered", 1)], payment="cod")
    run_build(world, steps=["fact_orders", "fact_order_lines", "customer_360"], **MONTH)
    c2 = customers(spark, world)["c2"]
    assert (c2["orders_count"], c2["total_spend"], c2["is_repeat_buyer"]) == (2, Decimal("40.00"), True)
    assert c2["avg_order_value"] == Decimal("20.00") and c2["last_order_at"].replace(tzinfo=timezone.utc) == t(9, 10)
    assert c2["preferred_payment_method"] == "card"  # 1 card against 1 cod: a tie goes to the first alphabetically
