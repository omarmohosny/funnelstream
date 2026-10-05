"""Spark tests for the Gold reference builders and local live metrics (run in Docker: RUN_SPARK_TESTS=1)."""
import json
import random
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import lru_cache

import pytest

pytestmark = pytest.mark.spark

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (IntegerType, LongType, StringType, StructField, StructType,  # noqa: E402
                               TimestampType)

from scripts.run_simulator import iter_batch  # noqa: E402
from simulator.catalog import make_catalog, make_customers  # noqa: E402
from simulator.noise import NoiseConfig, NoiseCounter, apply_noise  # noqa: E402
from simulator.publish import route  # noqa: E402
from streaming.common.bots import find_bot_sessions  # noqa: E402
from streaming.common.status import STATUS_RANK, resolve_current_status_timed  # noqa: E402
from streaming.gold.bots import mark_bot_sessions  # noqa: E402
from streaming.gold.build import build_all  # noqa: E402
from streaming.gold.agg_orders import build_agg_orders_daily  # noqa: E402
from streaming.gold.funnel import build_agg_funnel_daily  # noqa: E402
from streaming.gold.live import build_live_metrics_minute, refresh_live_metrics  # noqa: E402
from streaming.gold.orders import (build_fact_order_lines, build_fact_orders,  # noqa: E402
                                   report_cancel_then_shipped, report_negative_lead_time,
                                   report_orders_without_created_at)
from streaming.jobs.silver_clean import process_batch  # noqa: E402
from streaming.silver.spark_schemas import SCHEMAS, coerce_row  # noqa: E402
from streaming.silver.writer import merge_insert, silver_path  # noqa: E402

START = datetime(2026, 8, 1, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 4, tzinfo=timezone.utc)
SEED = 11
CATALOG = make_catalog(SEED)
CATALOG_IDS = {p["product_id"] for p in CATALOG}
CUSTOMERS = make_customers(SEED, 1000)
BRONZE_SCHEMA = StructType([
    StructField("raw_value", StringType()), StructField("kafka_topic", StringType()),
    StructField("kafka_partition", IntegerType()), StructField("kafka_offset", LongType()),
    StructField("kafka_timestamp", TimestampType()), StructField("ingest_time", TimestampType()),
])


# ---------------------------------------------------------------- helpers
@lru_cache(maxsize=None)
def simulated(max_events, bots_only=False, seed=SEED):
    """(messages, manifest). bots_only: NoiseConfig with bot_session=1.0 and every other rate 0."""
    rng, noise_rng = random.Random(seed), random.Random(seed * 1_000_003 + 7)
    counter = NoiseCounter()
    if bots_only:
        cfg = NoiseConfig.off()
        cfg.bot_session = 1.0
    else:
        cfg = NoiseConfig()
    stream = iter_batch(rng, CUSTOMERS, CATALOG, START, max_events=max_events)
    msgs = []
    for hint, payload in apply_noise(stream, noise_rng, cfg, CATALOG_IDS, counter, now=NOW):
        topic, _ = route(hint, payload)
        if isinstance(payload, str):
            raw, kts = payload, START
        else:
            raw = json.dumps(payload)
            try:
                kts = datetime.fromisoformat(payload["event_time"]) + timedelta(seconds=1)
            except Exception:
                kts = START
        msgs.append((topic, raw, kts.astimezone(timezone.utc).replace(tzinfo=None)))
    return tuple(msgs), counter.to_manifest()


def bronze_df(spark, msgs):
    ing = NOW.replace(tzinfo=None)
    rows = [(raw, topic, 0, i, kts, ing) for i, (topic, raw, kts) in enumerate(msgs)]
    return spark.createDataFrame(rows, BRONZE_SCHEMA).withColumn("ingest_date", F.to_date("ingest_time"))


def silver(spark, lake, table):
    return spark.read.format("delta").load(silver_path(str(lake), table))


def mk(spark, table, rows):
    """DataFrame in the Silver schema of table from partial row dicts (missing columns are NULL)."""
    return spark.createDataFrame([tuple(coerce_row(table, r).values()) for r in rows], SCHEMAS[table])


def t(day, hh=0, mm=0, ss=0):
    return datetime(2026, 10, day, hh, mm, ss, tzinfo=timezone.utc)


def order_row(oid, customer="c1", created=None, total="100.00", payment="cod", session="s1", platform="app",
              channel="paid_search"):
    created = created or t(1, 10)
    return {"order_id": oid, "customer_id": customer, "session_id": session, "channel": channel,
            "platform": platform,
            "utm_source": "google", "utm_campaign": "camp", "click_id": "clk", "payment_method": payment,
            "city": "Cairo", "country": "EG", "order_total": Decimal(total), "order_created_at": created,
            "event_date": created.date()}


_COUNTER = [0]


def status_row(oid, status, when, carrier=None, customer="c1"):
    _COUNTER[0] += 1
    return {"event_id": "st%d" % _COUNTER[0], "order_id": oid, "customer_id": customer, "status": status,
            "status_rank": STATUS_RANK[status], "status_time": when, "carrier": carrier,
            "event_date": when.date()}


def fo(spark, orders, statuses):
    df = build_fact_orders(mk(spark, "silver_orders", orders), mk(spark, "silver_order_status", statuses))
    return {r["order_id"]: r for r in df.collect()}


@pytest.fixture(scope="module")
def e2e(spark, tmp_path_factory):
    """Noisy 30,000-event simulation processed through process_batch into a Silver lake."""
    msgs, manifest = simulated(30_000)
    lake = str(tmp_path_factory.mktemp("gold_lake") / "lake")
    process_batch(spark, bronze_df(spark, msgs), lake, None, now=NOW)
    return {"lake": lake, "events": len(msgs), "manifest": manifest}


def silver_frames(spark, lake):
    return {t_: silver(spark, lake, t_) for t_ in
            ("silver_events", "silver_orders", "silver_order_status", "silver_order_items")}


# ---------------------------------------------------------------- group 1: fact_orders on hand-built frames
def test_fact_orders_out_of_order_arrival_still_delivered(spark):
    o = order_row("o1", created=t(1, 10))
    rows = [status_row("o1", "delivered", t(2, 12)),  # listed first: arrived before earlier statuses
            status_row("o1", "order_created", t(1, 10)),
            status_row("o1", "order_shipped", t(1, 18), carrier="Bosta"),
            status_row("o1", "out_for_delivery", t(2, 9))]
    r = fo(spark, [o], rows)["o1"]
    assert r["current_status"] == "delivered" and r["status_rank"] == 6
    assert r["is_delivered"] is True and r["is_cancelled"] is False
    assert r["carrier"] == "Bosta" and r["payment_method"] == "cod" and r["campaign_id"] is None
    assert r["delivered_at"].replace(tzinfo=None) == t(2, 12).replace(tzinfo=None)


def test_fact_orders_cancel_after_ship_keeps_shipped(spark):
    rows = [status_row("o1", "order_created", t(1, 10)), status_row("o1", "order_shipped", t(1, 12), carrier="Aramex"),
            status_row("o1", "order_cancelled", t(1, 13))]
    r = fo(spark, [order_row("o1")], rows)["o1"]
    assert r["current_status"] == "order_shipped"
    assert r["is_cancelled"] is False and r["cancelled_at"] is None


def test_fact_orders_cancel_before_ship_is_cancelled(spark):
    rows = [status_row("o1", "order_created", t(1, 10)), status_row("o1", "order_cancelled", t(1, 11)),
            status_row("o1", "payment_authorised", t(1, 10, 5))]
    r = fo(spark, [order_row("o1")], rows)["o1"]
    assert r["current_status"] == "order_cancelled" and r["status_rank"] == 2
    assert r["is_cancelled"] is True
    assert r["cancelled_at"].replace(tzinfo=None) == t(1, 11).replace(tzinfo=None)


def test_fact_orders_failed_attempt_then_delivered_and_lead_hours(spark):
    created = t(1, 10)
    rows = [status_row("o1", "order_created", created), status_row("o1", "order_shipped", t(1, 20), carrier="Bosta"),
            status_row("o1", "out_for_delivery", t(2, 9)), status_row("o1", "delivery_failed", t(2, 14)),
            status_row("o1", "out_for_delivery", t(3, 9)), status_row("o1", "delivered", t(3, 11, 30))]
    r = fo(spark, [order_row("o1", created=created)], rows)["o1"]
    assert r["delivery_failed_count"] == 1 and r["first_attempt_delivered"] is False
    assert r["is_delivered"] is True and r["current_status"] == "delivered"
    assert r["delivery_lead_hours"] == Decimal("49.50")  # 1 Oct 10:00 -> 3 Oct 11:30


def test_fact_orders_first_attempt_delivered_true(spark):
    rows = [status_row("o1", "order_created", t(1, 10)), status_row("o1", "order_shipped", t(1, 20)),
            status_row("o1", "out_for_delivery", t(2, 9)), status_row("o1", "delivered", t(2, 12))]
    r = fo(spark, [order_row("o1")], rows)["o1"]
    assert r["first_attempt_delivered"] is True and r["delivery_failed_count"] == 0
    assert r["delivery_lead_hours"] == Decimal("26.00")


def test_fact_orders_tie_between_out_for_delivery_and_failed_takes_latest(spark):
    rows = [status_row("o1", "order_shipped", t(1, 20)), status_row("o1", "out_for_delivery", t(2, 9)),
            status_row("o1", "delivery_failed", t(2, 14))]
    assert fo(spark, [order_row("o1")], rows)["o1"]["current_status"] == "delivery_failed"
    rows = [status_row("o2", "order_shipped", t(1, 20)), status_row("o2", "delivery_failed", t(2, 9)),
            status_row("o2", "out_for_delivery", t(3, 9))]
    assert fo(spark, [order_row("o2")], rows)["o2"]["current_status"] == "out_for_delivery"


def test_fact_orders_order_seq_and_single_first_order(spark):
    orders = [order_row("oB", customer="cx", created=t(2, 10)), order_row("oA", customer="cx", created=t(1, 10)),
              order_row("oC", customer="cy", created=t(3, 10))]
    rows = [status_row(o["order_id"], "order_created", o["order_created_at"], customer=o["customer_id"])
            for o in orders]
    res = fo(spark, orders, rows)
    assert (res["oA"]["order_seq"], res["oB"]["order_seq"], res["oC"]["order_seq"]) == (1, 2, 1)
    assert res["oA"]["is_first_order"] is True and res["oB"]["is_first_order"] is False
    assert sum(1 for r in res.values() if r["customer_id"] == "cx" and r["is_first_order"]) == 1


def test_fact_orders_date_key_uses_cairo_date(spark):
    created = datetime(2026, 10, 4, 23, 30, tzinfo=timezone.utc)  # 02:30 on 5 Oct in Cairo
    r = fo(spark, [order_row("o1", created=created)], [status_row("o1", "order_created", created)])["o1"]
    assert r["date_key"] == 20261005
    lines = build_fact_order_lines(
        mk(spark, "silver_orders", [order_row("o1", created=created)]),
        mk(spark, "silver_order_items", [
            {"order_id": "o1", "line_no": "1", "product_id": "p1", "quantity": 1, "unit_price": Decimal("100.00"),
             "line_total": Decimal("100.00"), "currency": "EGP", "order_time": created},
            {"order_id": "ghost", "line_no": "1", "product_id": "p1", "quantity": 1, "unit_price": Decimal("1.00"),
             "line_total": Decimal("1.00"), "currency": "EGP", "order_time": created}])).collect()
    assert len(lines) == 1 and lines[0]["date_key"] == 20261005 and lines[0]["order_id"] == "o1"
    assert lines[0]["city"] == "Cairo" and lines[0]["customer_id"] == "c1"


def test_fact_orders_without_delivered_has_null_lead_and_orders_without_status_dropped(spark):
    rows = [status_row("o1", "order_created", t(1, 10)), status_row("o1", "order_shipped", t(1, 12))]
    res = fo(spark, [order_row("o1"), order_row("o2")], rows)
    assert set(res) == {"o1"}
    assert res["o1"]["delivery_lead_hours"] is None and res["o1"]["delivered_at"] is None
    assert res["o1"]["is_delivered"] is False
    assert report_negative_lead_time(build_fact_orders(
        mk(spark, "silver_orders", [order_row("o1")]), mk(spark, "silver_order_status", rows))) == 0


def test_negative_lead_time_is_kept_and_counted(spark):
    rows = [status_row("o1", "order_created", t(1, 10)), status_row("o1", "delivered", t(1, 8))]
    df = build_fact_orders(mk(spark, "silver_orders", [order_row("o1", created=t(1, 10))]),
                           mk(spark, "silver_order_status", rows))
    assert df.collect()[0]["delivery_lead_hours"] == Decimal("-2.00")
    assert report_negative_lead_time(df) == 1


# ---------------------------------------------------------------- group 2: status parity on the end-to-end dataset
def _naive_utc(dt):
    return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo is not None else dt


def test_fact_orders_current_status_matches_resolve_current_status_timed(spark, e2e):
    """Item 6 parity: Spark current_status equals the timed Python rule on the 30,000-event noisy dataset."""
    f = silver_frames(spark, e2e["lake"])
    fact = {r["order_id"]: r["current_status"]
            for r in build_fact_orders(f["silver_orders"], f["silver_order_status"]).collect()}
    by_order = {}
    for r in f["silver_order_status"].collect():
        by_order.setdefault(r["order_id"], []).append((r["status"], _naive_utc(r["status_time"])))
    expected = {o: resolve_current_status_timed(rows) for o, rows in by_order.items() if o in fact}
    mismatches = [o for o in fact if fact[o] != expected[o]]
    kinds = {}
    for v in fact.values():
        kinds[v] = kinds.get(v, 0) + 1
    print("\nG2 events=%d fact_orders=%d compared=%d mismatches=%d current_status=%s" % (
        e2e["events"], len(fact), len(expected), len(mismatches), dict(sorted(kinds.items()))))
    assert len(fact) == len(expected) > 100
    assert mismatches == []


# ---------------------------------------------------------------- group 3: invariants on the end-to-end dataset
def test_gold_invariants_on_end_to_end_dataset(spark, e2e):
    f = silver_frames(spark, e2e["lake"])
    fact_df = build_fact_orders(f["silver_orders"], f["silver_order_status"]).cache()
    lines_df = build_fact_order_lines(f["silver_orders"], f["silver_order_items"]).cache()
    n_orders = f["silver_orders"].count()
    no_status = f["silver_orders"].join(f["silver_order_status"].select("order_id").distinct(),
                                        "order_id", "left_anti").count()
    n_fact = fact_df.count()
    assert n_fact == n_orders - no_status
    assert n_fact == fact_df.select("order_id").distinct().count()
    sums = lines_df.groupBy("order_id").agg(F.sum("line_total").alias("s"))
    mismatch = (fact_df.join(sums, "order_id", "left")
                .where(F.abs(F.col("order_total") - F.col("s")) > Decimal("0.01")).count())
    shipped = f["silver_order_status"].where("status = 'order_shipped'").select("order_id").distinct()
    cancelled_with_ship = (fact_df.where("current_status = 'order_cancelled'")
                           .join(shipped, "order_id", "left_semi").count())
    delivered_no_at = fact_df.where("is_delivered AND delivered_at IS NULL").count()
    not_delivered_with_lead = fact_df.where("NOT is_delivered AND delivery_lead_hours IS NOT NULL").count()
    neg = report_negative_lead_time(fact_df)
    n_delivered = fact_df.where("is_delivered").count()
    print("\nG3 events=%d silver_orders=%d orders_without_status=%d fact_orders=%d fact_order_lines=%d "
          "line_total_mismatch=%d cancelled_with_ship=%d delivered=%d negative_lead_time=%d "
          "injected_out_of_order=%d injected_cancel_after_ship=%d" % (
              e2e["events"], n_orders, no_status, n_fact, lines_df.count(), mismatch, cancelled_with_ship,
              n_delivered, neg, e2e["manifest"]["injected"]["out_of_order_status"],
              e2e["manifest"]["injected"]["cancel_after_ship"]))
    assert mismatch == 0
    assert cancelled_with_ship == 0
    assert delivered_no_at == 0 and not_delivered_with_lead == 0
    assert isinstance(neg, int) and neg >= 0
    assert n_delivered > 0
    cols = fact_df.columns
    assert cols == ["order_id", "customer_key", "customer_id", "session_id", "channel", "platform", "utm_source",
                    "utm_campaign", "click_id", "campaign_id", "payment_method", "carrier", "city", "country",
                    "order_total", "current_status", "status_rank", "order_created_at", "delivered_at",
                    "cancelled_at", "returned_at", "delivery_lead_hours", "delivery_failed_count",
                    "has_failed_attempt", "first_attempt_delivered", "is_cancelled", "is_delivered",
                    "is_returned", "order_seq", "is_first_order", "date_key"]
    assert str(fact_df.schema["customer_key"].dataType) == "LongType()"
    assert str(fact_df.schema["order_total"].dataType) == "DecimalType(12,2)"
    assert str(fact_df.schema["delivery_lead_hours"].dataType) == "DecimalType(8,2)"


# ---------------------------------------------------------------- group 4: mark_bot_sessions
def ev(i, sid, when, etype="page_view", **kw):
    row = {"event_id": "e%s_%d" % (sid, i), "event_type": etype, "event_time": when, "kafka_timestamp": when,
           "is_late": False, "is_bot": False, "event_version": "1", "session_id": sid, "country": "EG",
           "channel": "organic", "event_date": when.date()}
    row.update(kw)
    return row


def burst(sid, n, span, start=None):
    start = start or t(4, 12)
    return [ev(i, sid, start + timedelta(seconds=span * i / n)) for i in range(n)]


def test_mark_bot_sessions_hand_built_and_parity(spark):
    rows = (burst("bot", 150, 5) + burst("ok", 12, 60) + burst("slow", 150, 600)
            + [ev(i, None, t(4, 12) + timedelta(milliseconds=i)) for i in range(500)])
    df = mk(spark, "silver_events", rows)
    out = mark_bot_sessions(df)
    assert out.columns == df.columns and out.count() == len(rows)
    flagged = {r["session_id"] for r in out.where("is_bot").select("session_id").distinct().collect()}
    assert flagged == {"bot"}
    per = {r["session_id"]: (r["n"], r["b"]) for r in out.where("session_id IS NOT NULL").groupBy("session_id")
           .agg(F.count("*").alias("n"), F.sum(F.col("is_bot").cast("int")).alias("b")).collect()}
    assert per["bot"] == (150, 150) and per["ok"][1] == 0 and per["slow"][1] == 0  # every row of the bot session
    assert out.where("session_id IS NULL AND is_bot").count() == 0
    py = find_bot_sessions([{"session_id": r["session_id"], "event_time": r["event_time"].isoformat()}
                            for r in df.collect()])
    assert py == flagged
    # is_bot is recomputed over the whole input: a stale True is cleared, a stale False is set
    stale = mk(spark, "silver_events", [dict(r, is_bot=(r["session_id"] == "ok")) for r in rows])
    again = {r["session_id"] for r in mark_bot_sessions(stale).where("is_bot").select("session_id").distinct().collect()}
    assert again == {"bot"}
    # parameters
    small = {r["session_id"] for r in mark_bot_sessions(df, max_events=10, window_seconds=60)
             .where("is_bot").select("session_id").distinct().collect()}
    assert small == {py_s for py_s in find_bot_sessions(
        [{"session_id": r["session_id"], "event_time": r["event_time"].isoformat()} for r in df.collect()],
        max_events=10, window_seconds=60)}


def test_mark_bot_sessions_parity_and_injected_counts_on_bot_simulation(spark, tmp_path):
    msgs, manifest = simulated(1_500, bots_only=True)
    injected = manifest["injected"]["bot_session"]
    lake = str(tmp_path / "lake")
    process_batch(spark, bronze_df(spark, msgs), lake, None, now=NOW)
    events = silver(spark, lake, "silver_events")
    marked = mark_bot_sessions(events)
    flagged = {r["session_id"] for r in marked.where("is_bot").select("session_id").distinct().collect()}
    py = find_bot_sessions([{"session_id": r["session_id"], "event_time": r["event_time"].isoformat()}
                            for r in events.collect()])
    print("\nG4 messages=%d silver_events=%d injected_bot_sessions=%d flagged_sessions=%d parity_with_python=%s" % (
        len(msgs), events.count(), injected, len(flagged), flagged == py))
    assert injected > 0
    assert len(flagged) == injected
    assert flagged == py
    # non-bot rows of a normal dataset: only injected replays are flagged
    nb = marked.where(~F.col("is_bot")).groupBy("session_id").count().where("count > 100").count()
    assert nb == 0


# ---------------------------------------------------------------- group 5: agg_funnel_daily
def funnel(spark, rows):
    return build_agg_funnel_daily(mk(spark, "silver_events", rows), mk(spark, "silver_orders", []))


def test_agg_funnel_daily_rules(spark):
    d = t(4, 12)
    rows = []
    # s1: app session, full funnel, two views of one product
    rows += [ev(0, "s1", d, "product_view", product_id="p1"), ev(1, "s1", d + timedelta(seconds=5), "product_view",
                                                                 product_id="p1"),
             ev(2, "s1", d + timedelta(seconds=10), "add_to_cart", product_id="p1"),
             ev(3, "s1", d + timedelta(seconds=20), "checkout_started"),
             ev(4, "s1", d + timedelta(seconds=30), "purchase", order_id="o1")]
    # s6: cart, no order (EG/organic, NULL platform); s7: cart + purchase without a silver_orders row
    rows += [ev(0, "s6", d, "add_to_cart"), ev(0, "s7", d, "add_to_cart"), ev(1, "s7", d, "purchase")]
    rows += [ev(0, "s8", d, "add_to_cart", channel="paid_search")]  # separate group below
    # s2: views only, country SA (denominators 0 -> NULL rates)
    rows += [ev(0, "s2", d, "product_view", country="SA", channel="direct")]
    # s3 bot, excluded entirely; s4 late event is included; s5 null session excluded
    rows += [ev(0, "s3", d, "product_view", is_bot=True, country="XX", channel="bot")]
    rows += [ev(0, "s4", d, "product_view", is_late=True, country="LATE", channel="x")]
    rows += [ev(0, None, d, "product_view", country="NOSESSION", channel="x")]
    for r in rows:
        if r["session_id"] == "s1":
            r["channel"] = "paid_search"
            r["platform"] = "app"
    df = funnel(spark, rows)
    assert "payment_method" not in df.columns
    assert df.columns == ["event_date", "country", "channel", "platform", "sessions", "product_views",
                          "cart_sessions", "checkout_sessions", "orders", "cart_abandon_rate",
                          "checkout_abandon_rate"]
    out = {(r["country"], r["channel"], r["platform"]): r for r in df.collect()}
    assert not any(k[0] in ("XX", "NOSESSION") for k in out)  # bot and null session excluded
    assert ("LATE", "x", "unknown") in out and out[("LATE", "x", "unknown")]["sessions"] == 1  # late included
    g = out[("EG", "paid_search", "app")]
    assert (g["sessions"], g["product_views"], g["cart_sessions"], g["checkout_sessions"], g["orders"]) == (1, 1, 1, 1, 1)
    assert g["cart_abandon_rate"] == 0.0 and g["checkout_abandon_rate"] == 0.0
    assert g["event_date"] == date(2026, 10, 4)
    u = out[("EG", "organic", "unknown")]
    assert (u["sessions"], u["cart_sessions"], u["orders"]) == (2, 2, 1)
    assert u["cart_abandon_rate"] == pytest.approx(0.5) and u["checkout_abandon_rate"] is None
    p = out[("EG", "paid_search", "unknown")]  # s8
    assert p["cart_abandon_rate"] == pytest.approx(1.0)
    n = out[("SA", "direct", "unknown")]
    assert n["cart_sessions"] == 0 and n["cart_abandon_rate"] is None and n["checkout_abandon_rate"] is None
    for r in out.values():
        for c in ("cart_abandon_rate", "checkout_abandon_rate"):
            assert r[c] is None or 0.0 <= r[c] <= 1.0


def test_agg_funnel_daily_platform_split_adds_up_and_null_platform_is_unknown(spark):
    d = t(4, 12)
    rows = []
    for i in range(7):
        rows.append(ev(0, "a%d" % i, d, "product_view", platform="app"))
    for i in range(5):
        rows.append(ev(0, "w%d" % i, d, "product_view", platform="web"))
    for i in range(2):
        rows.append(ev(0, "n%d" % i, d, "product_view"))  # NULL platform
    out = funnel(spark, rows).collect()
    by_platform = {}
    for r in out:
        by_platform[r["platform"]] = by_platform.get(r["platform"], 0) + r["sessions"]
    assert by_platform == {"app": 7, "web": 5, "unknown": 2}
    assert sum(by_platform.values()) == 14 == len({r["session_id"] for r in rows})


def test_agg_funnel_daily_session_attributes_come_from_earliest_event_ties_by_event_id(spark):
    d = t(4, 12)
    rows = [ev(1, "s", d + timedelta(seconds=9), "product_view", platform="web", channel="direct", country="SA"),
            ev(2, "s", d, "add_to_cart", platform="app", channel="paid_search", country="EG", event_id="e_b"),
            ev(3, "s", d, "product_view", platform="web", channel="organic", country="AE", event_id="e_a")]
    out = funnel(spark, rows).collect()
    assert len(out) == 1
    r = out[0]  # earliest time ties between e_a and e_b: the smaller event_id e_a wins
    assert (r["country"], r["channel"], r["platform"]) == ("AE", "organic", "web")
    assert (r["sessions"], r["product_views"], r["cart_sessions"]) == (1, 1, 1)


def test_agg_funnel_daily_orders_are_distinct_sessions_with_a_purchase(spark):
    d = t(4, 12)
    rows = [ev(0, "a", d, "purchase"), ev(1, "a", d + timedelta(seconds=1), "purchase"),
            ev(0, "b", d, "add_to_cart"), ev(1, "b", d + timedelta(seconds=1), "add_to_cart"),
            ev(2, "b", d + timedelta(seconds=2), "purchase")]
    (r,) = funnel(spark, rows).collect()
    assert r["sessions"] == 2 and r["orders"] == 2 and r["cart_sessions"] == 1  # a: two purchases count once


def test_agg_funnel_daily_clamps_orders_above_cart(spark):
    d = t(4, 12)
    rows = [ev(0, "a", d, "purchase"), ev(0, "b", d, "purchase"), ev(0, "b", d, "add_to_cart")]
    r = funnel(spark, rows).collect()
    assert len(r) == 1 and r[0]["orders"] == 2 and r[0]["cart_sessions"] == 1
    assert r[0]["cart_abandon_rate"] == 0.0  # 1 - 2/1 clamped to 0


# ---------------------------------------------------------------- group 6: live metrics
def test_live_metrics_minute_counts_and_exclusions(spark):
    d = t(4, 10, 0, 5)
    rows = [ev(0, "a", d, "product_view"), ev(1, "a", d + timedelta(seconds=30), "add_to_cart"),
            ev(0, "b", d + timedelta(seconds=40), "product_view"), ev(1, "b", d + timedelta(seconds=50), "purchase"),
            ev(0, "c", d, "product_view", is_late=True), ev(0, "d", d, "product_view", is_bot=True),
            ev(0, "a", t(4, 10, 1, 0), "page_view"),  # exactly on the next minute
            ev(0, "e", d, "product_view", country="SA")]
    out = {(r["minute_start"].replace(tzinfo=None), r["country"]): r
           for r in build_live_metrics_minute(mk(spark, "silver_events", rows)).collect()}
    assert set(out) == {(datetime(2026, 10, 4, 10, 0), "EG"), (datetime(2026, 10, 4, 10, 1), "EG"),
                        (datetime(2026, 10, 4, 10, 0), "SA")}
    m = out[(datetime(2026, 10, 4, 10, 0), "EG")]
    assert (m["events"], m["active_sessions"], m["product_views"], m["add_to_cart"], m["orders_placed"]) == (4, 2, 2, 1, 1)
    assert out[(datetime(2026, 10, 4, 10, 1), "EG")]["events"] == 1
    assert all(k[0].second == 0 for k in out)


def test_refresh_live_metrics_keeps_last_hour_and_matches_builder(spark, tmp_path):
    lake = str(tmp_path / "lake")
    now = t(4, 12, 0, 30)
    rows = []
    for i in range(0, 150, 5):  # an event every 5 minutes, 150 minutes back from 12:00
        rows.append(ev(i, "s%d" % (i % 3), now - timedelta(minutes=i, seconds=20), "product_view"))
    rows.append(ev(999, "late", now - timedelta(minutes=2), "product_view", is_late=True))
    df = mk(spark, "silver_events", rows)
    merge_insert(spark, silver_path(lake, "silver_events"), df, ["event_id"], "event_date")
    path = refresh_live_metrics(spark, lake_root=lake, minutes=60, now=now)  # returns the table path
    assert path == lake + "/live/live_metrics_minute" or path.replace("\\", "/").endswith("/live/live_metrics_minute")
    table = spark.read.format("delta").load(path)
    cutoff = (now - timedelta(minutes=60)).replace(tzinfo=None)
    recent = mk(spark, "silver_events", [r for r in rows if r["event_time"].replace(tzinfo=None) >= cutoff])
    expected = build_live_metrics_minute(mark_bot_sessions(recent))
    key = lambda r: (r["minute_start"], r["country"], r["events"], r["active_sessions"], r["product_views"],  # noqa: E731
                     r["add_to_cart"], r["orders_placed"])
    got, want = sorted(table.collect(), key=key), sorted(expected.collect(), key=key)
    kept = {r["minute_start"] for r in got}
    print("\nG6 silver_events=%d kept_minutes=%d live_rows=%d oldest=%s" % (
        len(rows), len(kept), len(got), min(kept)))
    assert [tuple(r) for r in got] == [tuple(r) for r in want]
    assert len(got) == 12 and min(kept) >= cutoff - timedelta(minutes=1)
    # a second refresh with a later now overwrites, it does not append
    refresh_live_metrics(spark, lake_root=lake, minutes=10, now=now)
    assert spark.read.format("delta").load(path).count() == 2


# ---------------------------------------------------------------- group 7: build_all
def test_build_all_writes_four_tables_and_reads_back(spark, e2e, tmp_path):
    out_root = str(tmp_path / "gold_ref")
    res = build_all(spark, lake_root=e2e["lake"], out_root=out_root)
    assert set(res) == {"fact_orders", "fact_order_lines", "agg_orders_daily", "agg_funnel_daily"}
    counts = {}
    for name, df in res.items():
        back = spark.read.format("delta").load("%s/%s" % (out_root, name))
        counts[name] = (df.count(), back.count())
        assert counts[name][0] == counts[name][1] > 0, name
    print("\nG7 events=%d (built, read_back)=%s" % (e2e["events"], counts))
    assert build_all(spark, lake_root=e2e["lake"]).keys() == res.keys()  # out_root=None writes nothing


# ---------------------------------------------------------------- review fixes
def test_lead_hours_rounds_half_up_to_two_decimals(spark):
    created = t(1, 10)
    # 3618 s = 1.005 h -> 1.01 (half up); 3636 s = 1.01 h exactly
    rows = [status_row("o1", "delivered", created + timedelta(seconds=3618)),
            status_row("o2", "delivered", created + timedelta(seconds=3636), customer="c2")]
    orders = [order_row("o1", created=created), order_row("o2", customer="c2", created=created)]
    res = fo(spark, orders, rows)
    assert res["o1"]["delivery_lead_hours"] == Decimal("1.01") and res["o2"]["delivery_lead_hours"] == Decimal("1.01")


def _edge(n, span, sid="x"):
    return [ev(i, sid, t(4, 12) + timedelta(microseconds=int(span * 1_000_000 * i / (n - 1)))) for i in range(n)]


def _both(spark, rows):
    df = mk(spark, "silver_events", rows)
    spark_flag = bool(mark_bot_sessions(df).where("is_bot").count())
    py_flag = bool(find_bot_sessions([{"session_id": r["session_id"], "event_time": r["event_time"].isoformat()}
                                      for r in df.collect()]))
    return spark_flag, py_flag


def test_bot_window_edge_101_events_in_exactly_10s_flagged_by_both(spark):
    assert _both(spark, _edge(101, 10.0)) == (True, True)


def test_bot_window_edge_101_events_over_10_001s_flagged_by_neither(spark):
    assert _both(spark, _edge(101, 10.001)) == (False, False)


def test_bot_window_edge_100_events_in_10s_not_flagged(spark):
    assert _both(spark, _edge(100, 10.0)) == (False, False)


def _refresh(spark, tmp_path, rows, now, **kw):
    lake = str(tmp_path / "lake")
    merge_insert(spark, silver_path(lake, "silver_events"), mk(spark, "silver_events", rows), ["event_id"],
                 "event_date")
    path = refresh_live_metrics(spark, lake_root=lake, minutes=60, now=now, **kw)
    return spark.read.format("delta").load(path).collect()


def test_live_refresh_burst_before_window_excludes_that_session_and_keeps_normal(spark, tmp_path):
    now = t(4, 12, 0, 0)
    rows = [ev(i, "bot", now - timedelta(minutes=90) + timedelta(seconds=5 * i / 150)) for i in range(150)]
    rows += [ev(1000 + i, "bot", now - timedelta(minutes=30 - i)) for i in range(5)]
    rows += [ev(i, "ok", now - timedelta(minutes=20 - i)) for i in range(3)]
    res = _refresh(spark, tmp_path, rows, now)
    assert sum(r["events"] for r in res) == 3 and sum(r["active_sessions"] for r in res) == 3


def test_live_refresh_burst_straddling_cutoff_is_caught(spark, tmp_path):
    now = t(4, 12, 0, 0)
    cutoff = now - timedelta(minutes=60)
    # 101 events within 10 s, 60 before the cutoff and 41 after; neither side alone exceeds 100
    rows = [ev(i, "bot", cutoff - timedelta(seconds=6) + timedelta(milliseconds=100 * i)) for i in range(101)]
    rows += [ev(500 + i, "ok", now - timedelta(minutes=10 - i)) for i in range(2)]
    res = _refresh(spark, tmp_path, rows, now)
    assert sum(r["events"] for r in res) == 2


def test_live_refresh_burst_older_than_lookback_is_not_detected(spark, tmp_path):
    now = t(4, 12, 0, 0)
    rows = [ev(i, "bot", now - timedelta(hours=8) + timedelta(seconds=5 * i / 150)) for i in range(150)]
    rows += [ev(900 + i, "bot", now - timedelta(minutes=10 - i)) for i in range(2)]
    assert sum(r["events"] for r in _refresh(spark, tmp_path / "a", rows, now)) == 2  # documented limitation
    assert sum(r["events"] for r in _refresh(spark, tmp_path / "b", rows, now, bot_lookback_hours=12)) == 0


def test_show_live_missing_table_prints_message_and_returns_1(spark, tmp_path, capsys):
    from scripts.show_live import MISSING, show_live

    assert show_live(str(tmp_path / "nothing" / "live_metrics_minute"), 10, spark=spark) == 1
    assert MISSING in capsys.readouterr().out
    lake = str(tmp_path / "lake")
    merge_insert(spark, silver_path(lake, "silver_events"),
                 mk(spark, "silver_events", [ev(0, "a", t(4, 12), "product_view")]), ["event_id"], "event_date")
    path = refresh_live_metrics(spark, lake_root=lake, minutes=100000, now=t(4, 13))
    assert show_live(path, 5, spark=spark) == 0


# ---------------------------------------------------------------- C3: timed cancel, returned, keys
def test_cancel_before_ship_with_later_shipped_row_counted_cancel_wins_and_is_reported(spark):
    rows = [status_row("o1", "order_created", t(1, 10)), status_row("o1", "order_cancelled", t(1, 11)),
            status_row("o1", "order_shipped", t(1, 12), carrier="Bosta"),
            status_row("o1", "out_for_delivery", t(2, 9))]
    r = fo(spark, [order_row("o1")], rows)["o1"]
    assert r["current_status"] == "order_cancelled" and r["is_cancelled"] is True
    assert r["cancelled_at"].replace(tzinfo=None) == t(1, 11).replace(tzinfo=None)
    assert report_cancel_then_shipped(mk(spark, "silver_order_status", rows)) == 1
    ok = [status_row("o2", "order_created", t(1, 10)), status_row("o2", "order_shipped", t(1, 12)),
          status_row("o2", "order_cancelled", t(1, 13))]  # cancel after ship: ignored, not reported
    assert report_cancel_then_shipped(mk(spark, "silver_order_status", ok)) == 0
    assert resolve_current_status_timed([(x["status"], x["status_time"]) for x in rows]) == "order_cancelled"


def test_cancel_after_ship_is_ignored_by_the_timed_rule(spark):
    rows = [status_row("o1", "order_created", t(1, 10)), status_row("o1", "order_shipped", t(1, 12)),
            status_row("o1", "order_cancelled", t(1, 13)), status_row("o1", "out_for_delivery", t(2, 9))]
    r = fo(spark, [order_row("o1")], rows)["o1"]
    assert r["current_status"] == "out_for_delivery" and r["is_cancelled"] is False and r["cancelled_at"] is None


def test_completed_versus_returned_tie_is_broken_by_latest_time(spark):
    base = [("order_created", t(1, 10)), ("order_shipped", t(1, 12)), ("out_for_delivery", t(2, 9))]
    r1 = [status_row("o1", s_, w) for s_, w in base] + [status_row("o1", "order_completed", t(3, 9)),
                                                         status_row("o1", "order_returned", t(4, 9))]
    r2 = [status_row("o2", s_, w) for s_, w in base] + [status_row("o2", "order_returned", t(3, 9)),
                                                         status_row("o2", "order_completed", t(4, 9))]
    res = fo(spark, [order_row("o1"), order_row("o2", customer="c2")], r1 + r2)
    assert res["o1"]["current_status"] == "order_returned" and res["o1"]["status_rank"] == 7
    assert res["o2"]["current_status"] == "order_completed" and res["o2"]["status_rank"] == 7
    assert res["o1"]["is_returned"] is True and res["o2"]["is_returned"] is True  # a returned row exists
    assert res["o1"]["returned_at"].replace(tzinfo=None) == t(4, 9).replace(tzinfo=None)


def test_returned_order_after_failed_attempts_flags(spark):
    rows = [status_row("o1", "order_created", t(1, 10)), status_row("o1", "order_packed", t(1, 11)),
            status_row("o1", "order_shipped", t(1, 12), carrier="Bosta"),
            status_row("o1", "out_for_delivery", t(2, 9)), status_row("o1", "delivery_failed", t(2, 14)),
            status_row("o1", "out_for_delivery", t(3, 9)), status_row("o1", "delivery_failed", t(3, 14)),
            status_row("o1", "order_returned", t(4, 9))]
    r = fo(spark, [order_row("o1")], rows)["o1"]
    assert r["current_status"] == "order_returned"
    assert r["is_returned"] is True and r["has_failed_attempt"] is True and r["delivery_failed_count"] == 2
    assert r["is_delivered"] is False and r["is_cancelled"] is False and r["delivery_lead_hours"] is None
    assert r["returned_at"].replace(tzinfo=None) == t(4, 9).replace(tzinfo=None)
    assert r["platform"] == "app"


def test_platform_comes_from_silver_orders_and_null_stays_null(spark):
    orders = [order_row("o1", platform="web"), order_row("o2", customer="c2", platform=None)]
    rows = [status_row("o1", "order_created", t(1, 10)), status_row("o2", "order_created", t(1, 10))]
    res = fo(spark, orders, rows)
    assert res["o1"]["platform"] == "web" and res["o2"]["platform"] is None
    assert res["o1"]["has_failed_attempt"] is False and res["o1"]["is_returned"] is False


def dim_rows(spark, table, rows):
    return mk(spark, table, rows)


def test_customer_key_unknown_member_and_versioned_lookup(spark):
    schema = StructType([StructField("customer_id", StringType()), StructField("customer_key", LongType()),
                         StructField("valid_from", TimestampType()), StructField("valid_to", TimestampType())])
    far = datetime(9999, 12, 31)
    dim = spark.createDataFrame([("c1", 11, datetime(1900, 1, 1), datetime(2026, 10, 2)),
                                 ("c1", 12, datetime(2026, 10, 2), far)], schema)
    orders = [order_row("early", customer="c1", created=t(1, 10)), order_row("late", customer="c1", created=t(3, 10)),
              order_row("new", customer="cnew", created=t(1, 10))]
    rows = [status_row(o["order_id"], "order_created", o["order_created_at"]) for o in orders]
    df = build_fact_orders(mk(spark, "silver_orders", orders), mk(spark, "silver_order_status", rows), dim)
    res = {r["order_id"]: r for r in df.collect()}
    assert (res["early"]["customer_key"], res["late"]["customer_key"], res["new"]["customer_key"]) == (11, 12, -1)
    assert res["new"]["customer_id"] == "cnew"  # the natural id stays so a later build repairs the -1
    assert df.count() == 3  # the dimension join never duplicates an order
    nodim = {r["order_id"]: r["customer_key"] for r in build_fact_orders(
        mk(spark, "silver_orders", orders), mk(spark, "silver_order_status", rows)).collect()}
    assert set(nodim.values()) == {-1}
    pdim = spark.createDataFrame([("p1", 5, datetime(1900, 1, 1), far)], StructType([
        StructField("product_id", StringType()), StructField("product_key", LongType()),
        StructField("valid_from", TimestampType()), StructField("valid_to", TimestampType())]))
    items = [{"order_id": "early", "line_no": "1", "product_id": "p1", "quantity": 1, "unit_price": Decimal("1.00"),
              "line_total": Decimal("1.00"), "currency": "EGP", "order_time": t(1, 10)},
             {"order_id": "early", "line_no": "2", "product_id": "pX", "quantity": 1, "unit_price": Decimal("1.00"),
              "line_total": Decimal("1.00"), "currency": "EGP", "order_time": t(1, 10)}]
    lines = {r["line_no"]: r for r in build_fact_order_lines(
        mk(spark, "silver_orders", orders), mk(spark, "silver_order_items", items), dim, pdim).collect()}
    assert (lines[1]["customer_key"], lines[1]["product_key"]) == (11, 5)
    assert (lines[2]["customer_key"], lines[2]["product_key"], lines[2]["product_id"]) == (11, -1, "pX")
    assert [f.name for f in build_fact_order_lines(mk(spark, "silver_orders", orders),
                                                   mk(spark, "silver_order_items", items)).schema.fields] == [
        "order_id", "line_no", "customer_key", "customer_id", "product_key", "product_id", "date_key", "city",
        "quantity", "line_total"]


# ---------------------------------------------------------------- C3: agg_orders_daily
def test_agg_orders_daily_hand_built_measures(spark):
    d1, d2 = t(1, 10), t(2, 10)
    orders = [order_row("o1", customer="a", created=d1, total="100.00", payment="cod"),
              order_row("o2", customer="b", created=d1, total="50.00", payment="cod"),
              order_row("o3", customer="a", created=d1, total="20.00", payment="cod"),
              order_row("o4", customer="c", created=d1, total="10.00", payment="card", platform="web"),
              order_row("o5", customer="d", created=d2, total="7.00", payment="cod")]
    rows = [status_row("o1", "order_created", d1), status_row("o1", "order_shipped", t(1, 12)),
            status_row("o1", "delivery_failed", t(2, 9)), status_row("o1", "delivered", t(3, 9)),
            status_row("o2", "order_created", d1), status_row("o2", "order_cancelled", t(1, 11)),
            status_row("o3", "order_created", d1), status_row("o3", "order_shipped", t(1, 12)),
            status_row("o3", "delivery_failed", t(2, 9)), status_row("o3", "order_returned", t(3, 9)),
            status_row("o4", "order_created", d1), status_row("o4", "payment_authorised", t(1, 11)),
            status_row("o5", "order_created", d2)]
    fact = build_fact_orders(mk(spark, "silver_orders", orders), mk(spark, "silver_order_status", rows))
    out = {(r["date_key"], r["channel"], r["payment_method"], r["platform"]): r
           for r in build_agg_orders_daily(fact).collect()}
    assert set(out) == {(20261001, "paid_search", "cod", "app"), (20261001, "paid_search", "card", "web"),
                        (20261002, "paid_search", "cod", "app")}
    g = out[(20261001, "paid_search", "cod", "app")]
    assert (g["orders_placed"], g["orders_delivered"], g["orders_cancelled"], g["orders_returned"],
            g["orders_with_failed_attempt"], g["new_customer_orders"]) == (3, 1, 1, 1, 2, 2)
    assert g["revenue_placed"] == Decimal("170.00") and g["revenue_delivered"] == Decimal("100.00")
    c = out[(20261001, "paid_search", "card", "web")]
    assert (c["orders_placed"], c["orders_delivered"], c["revenue_delivered"]) == (1, 0, Decimal("0.00"))
    assert str(build_agg_orders_daily(fact).schema["revenue_placed"].dataType) == "DecimalType(12,2)"
    assert build_agg_orders_daily(fact).columns == [
        "date_key", "channel", "payment_method", "platform", "orders_placed", "orders_delivered",
        "orders_cancelled", "orders_returned", "orders_with_failed_attempt", "revenue_placed",
        "revenue_delivered", "new_customer_orders"]


def test_agg_orders_daily_invariants_on_end_to_end_dataset(spark, e2e):
    f = silver_frames(spark, e2e["lake"])
    fact = build_fact_orders(f["silver_orders"], f["silver_order_status"]).cache()
    agg = build_agg_orders_daily(fact).cache()
    tot = agg.agg(F.sum("orders_placed").alias("placed"), F.sum("orders_delivered").alias("deliv"),
                  F.sum("orders_cancelled").alias("canc"), F.sum("orders_returned").alias("ret"),
                  F.sum("orders_with_failed_attempt").alias("failed"), F.sum("revenue_placed").alias("rev"),
                  F.sum("revenue_delivered").alias("rev_d"), F.sum("new_customer_orders").alias("new")).first()
    n_fact = fact.count()
    fact_total = fact.agg(F.sum("order_total")).first()[0]
    opened = tot["placed"] - tot["deliv"] - tot["canc"] - tot["ret"]
    overlap = fact.where("(is_delivered AND is_returned) OR (is_delivered AND is_cancelled) "
                         "OR (is_returned AND is_cancelled)").count()
    cod_auth = (f["silver_order_status"].where("status = 'payment_authorised'")
                .join(f["silver_orders"].where("payment_method = 'cod'").select("order_id"), "order_id", "left_semi")
                .count())
    methods = {r["payment_method"] for r in agg.select("payment_method").distinct().collect()}
    platforms = {r["platform"] for r in agg.select("platform").distinct().collect()}
    print("\nC3 agg_orders_daily rows=%d placed=%d fact_orders=%d revenue_placed=%s order_total=%s "
          "delivered=%d cancelled=%d returned=%d open=%d with_failed_attempt=%d new_customer=%d overlap=%d "
          "cod_payment_authorised=%d methods=%s platforms=%s" % (
              agg.count(), tot["placed"], n_fact, tot["rev"], fact_total, tot["deliv"], tot["canc"], tot["ret"],
              opened, tot["failed"], tot["new"], overlap, cod_auth, sorted(methods), sorted(platforms)))
    assert tot["placed"] == n_fact > 100
    assert tot["rev"] == fact_total
    assert tot["rev_d"] == fact.where("is_delivered").agg(F.sum("order_total")).first()[0]
    assert tot["deliv"] == fact.where("is_delivered").count() > 0
    assert tot["ret"] == fact.where("is_returned").count()
    assert tot["failed"] == fact.where("delivery_failed_count > 0").count()
    assert opened >= 0
    assert cod_auth == 0
    assert methods <= {"cod", "fawry", "vodafone_cash", "instapay", "card"} and "cod" in methods
    assert platforms <= {"app", "web", "unknown"}
    keys = agg.groupBy("date_key", "channel", "payment_method", "platform").count().where("count > 1").count()
    assert keys == 0


def test_returned_orders_and_failed_attempts_on_end_to_end_dataset(spark, e2e):
    f = silver_frames(spark, e2e["lake"])
    fact = build_fact_orders(f["silver_orders"], f["silver_order_status"]).cache()
    returned = fact.where("is_returned")
    n_ret, n_failed = returned.count(), fact.where("has_failed_attempt").count()
    bad = returned.where("NOT has_failed_attempt OR is_delivered OR current_status <> 'order_returned'").count()
    cancel_then_ship = report_cancel_then_shipped(f["silver_order_status"])
    print("\nC3 orders=%d returned=%d has_failed_attempt=%d returned_inconsistent=%d cancel_then_shipped=%d" % (
        fact.count(), n_ret, n_failed, bad, cancel_then_ship))
    assert n_ret > 0 and n_failed >= n_ret
    assert bad == 0


def test_funnel_platform_shares_on_end_to_end_dataset(spark, e2e):
    f = silver_frames(spark, e2e["lake"])
    events = mark_bot_sessions(f["silver_events"])
    agg = build_agg_funnel_daily(events, f["silver_orders"]).cache()
    by_platform = {r["platform"]: r["n"] for r in agg.groupBy("platform").agg(F.sum("sessions").alias("n")).collect()}
    total = sum(by_platform.values())
    expected = (events.where(F.col("session_id").isNotNull() & ~F.col("is_bot"))
                .select("event_date", "session_id").distinct().count())
    print("\nC3 funnel rows=%d sessions=%d platform_sessions=%s shares=%s" % (
        agg.count(), total, dict(sorted(by_platform.items())),
        {k: round(v / total, 3) for k, v in sorted(by_platform.items())}))
    assert total == expected > 0
    assert set(by_platform) <= {"app", "web", "unknown"} and {"app", "web"} <= set(by_platform)
    assert "payment_method" not in agg.columns


# ---------------------------------------------------------------- C4 cleanup
def test_counted_cancel_and_later_delivered_row_overlap_is_pinned_and_reported(spark):
    rows = [status_row("o1", "order_created", t(1, 10)), status_row("o1", "order_cancelled", t(1, 11)),
            status_row("o1", "order_shipped", t(1, 12), carrier="Bosta"),
            status_row("o1", "delivered", t(2, 9))]
    r = fo(spark, [order_row("o1")], rows)["o1"]
    assert r["current_status"] == "order_cancelled"
    assert r["is_cancelled"] is False and r["is_delivered"] is True
    rep = report_cancel_then_shipped(mk(spark, "silver_order_status", rows))
    assert rep == 1 and rep.delivered_after_cancel == 1
    # a delivered row BEFORE the cancel is not counted; a plain cancel-then-ship has no delivered row
    plain = [status_row("o2", "order_created", t(1, 10)), status_row("o2", "order_cancelled", t(1, 11)),
             status_row("o2", "order_shipped", t(1, 12))]
    rep2 = report_cancel_then_shipped(mk(spark, "silver_order_status", plain))
    assert rep2 == 1 and rep2.delivered_after_cancel == 0
    clean = [status_row("o3", "order_created", t(1, 10)), status_row("o3", "order_shipped", t(1, 12)),
             status_row("o3", "delivered", t(2, 9)), status_row("o3", "order_cancelled", t(3, 9))]
    rep3 = report_cancel_then_shipped(mk(spark, "silver_order_status", clean))
    assert rep3 == 0 and rep3.delivered_after_cancel == 0


def _cust_dim(spark, rows):
    schema = StructType([StructField("customer_id", StringType()), StructField("customer_key", LongType()),
                         StructField("valid_from", TimestampType()), StructField("valid_to", TimestampType())])
    return spark.createDataFrame(rows, schema)


def _keys(spark, dim, orders):
    st = [status_row(o["order_id"], "order_created", o["order_created_at"]) for o in orders]
    df = build_fact_orders(mk(spark, "silver_orders", orders), mk(spark, "silver_order_status", st), dim)
    return df.count(), {r["order_id"]: r["customer_key"] for r in df.collect()}


def test_versioned_lookup_boundary_instants_valid_from_inclusive_valid_to_exclusive(spark):
    cut = datetime(2026, 10, 2, 10)  # naive UTC, as the session timezone is UTC
    dim = _cust_dim(spark, [("c1", 11, datetime(1900, 1, 1), cut), ("c1", 12, cut, datetime(9999, 12, 31))])
    orders = [order_row("at_cut", created=t(2, 10)), order_row("just_before", customer="c1", created=t(2, 9, 59, 59))]
    n, keys = _keys(spark, dim, orders)
    assert n == 2
    assert keys["at_cut"] == 12  # exactly the previous valid_to: the NEW version (exclusive upper bound)
    assert keys["just_before"] == 11
    only_new = _cust_dim(spark, [("c1", 12, cut, datetime(9999, 12, 31))])
    assert _keys(spark, only_new, [order_row("at_from", created=t(2, 10))])[1]["at_from"] == 12  # inclusive


def test_versioned_lookup_overlapping_versions_never_duplicate_orders(spark):
    far = datetime(9999, 12, 31)
    dim = _cust_dim(spark, [("c1", 11, datetime(1900, 1, 1), far), ("c1", 12, datetime(2026, 10, 1), far),
                            ("c1", 13, datetime(2026, 10, 1), far)])
    orders = [order_row("a", created=t(2, 10)), order_row("b", customer="c2", created=t(2, 10))]
    n, keys = _keys(spark, dim, orders)
    assert n == 2
    assert keys["a"] == 13 and keys["b"] == -1  # latest valid_from, then the higher key; no dim row stays unknown
    items = [{"order_id": "a", "line_no": str(i), "product_id": "p1", "quantity": 1, "unit_price": Decimal("1.00"),
              "line_total": Decimal("1.00"), "currency": "EGP", "order_time": t(2, 10)} for i in (1, 2)]
    lines = build_fact_order_lines(mk(spark, "silver_orders", orders), mk(spark, "silver_order_items", items), dim)
    assert lines.count() == 2


def test_orders_without_created_at_are_left_out_of_fact_and_reported(spark):
    orders = [order_row("ok", created=t(1, 10)), order_row("nodate", customer="c2", created=t(1, 10))]
    orders[1]["order_created_at"] = None
    st = [status_row("ok", "order_created", t(1, 10)), status_row("nodate", "order_created", t(1, 10))]
    so, ss = mk(spark, "silver_orders", orders), mk(spark, "silver_order_status", st)
    fact = build_fact_orders(so, ss)
    assert [r["order_id"] for r in fact.collect()] == ["ok"]
    assert report_orders_without_created_at(so, ss) == 1
    agg = build_agg_orders_daily(fact).collect()
    assert [r["date_key"] for r in agg] == [20261001] and agg[0]["orders_placed"] == 1
    assert report_orders_without_created_at(mk(spark, "silver_orders", orders[:1]), ss) == 0
