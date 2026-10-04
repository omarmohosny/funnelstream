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
from streaming.common.status import STATUS_RANK, resolve_current_status  # noqa: E402
from streaming.gold.bots import mark_bot_sessions  # noqa: E402
from streaming.gold.build import build_all  # noqa: E402
from streaming.gold.funnel import build_fact_funnel_daily  # noqa: E402
from streaming.gold.live import build_live_metrics_minute, refresh_live_metrics  # noqa: E402
from streaming.gold.orders import (build_fact_order_lines, build_fact_orders,  # noqa: E402
                                   report_negative_lead_time)
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


def order_row(oid, customer="c1", created=None, total="100.00", payment="cod", session="s1"):
    created = created or t(1, 10)
    return {"order_id": oid, "customer_id": customer, "session_id": session, "channel": "paid_search",
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
    assert r["delivery_lead_hours"] == pytest.approx(49.5, abs=0.001)  # 1 Oct 10:00 -> 3 Oct 11:30


def test_fact_orders_first_attempt_delivered_true(spark):
    rows = [status_row("o1", "order_created", t(1, 10)), status_row("o1", "order_shipped", t(1, 20)),
            status_row("o1", "out_for_delivery", t(2, 9)), status_row("o1", "delivered", t(2, 12))]
    r = fo(spark, [order_row("o1")], rows)["o1"]
    assert r["first_attempt_delivered"] is True and r["delivery_failed_count"] == 0
    assert r["delivery_lead_hours"] == pytest.approx(26.0, abs=0.001)


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
    assert df.collect()[0]["delivery_lead_hours"] == pytest.approx(-2.0)
    assert report_negative_lead_time(df) == 1


# ---------------------------------------------------------------- group 2: status parity on the end-to-end dataset
def test_fact_orders_current_status_matches_resolve_current_status(spark, e2e):
    f = silver_frames(spark, e2e["lake"])
    fact = {r["order_id"]: r["current_status"]
            for r in build_fact_orders(f["silver_orders"], f["silver_order_status"]).collect()}
    by_order = {}
    # latest status_time first, so Python max() breaks the rank-5 tie the way Spark does
    for r in f["silver_order_status"].orderBy(F.col("status_time").desc()).collect():
        by_order.setdefault(r["order_id"], []).append(r["status"])
    expected = {o: resolve_current_status(s) for o, s in by_order.items() if o in fact}
    mismatches = [o for o in fact if fact[o] != expected[o]]
    print("\nG2 events=%d fact_orders=%d compared=%d mismatches=%d" % (
        e2e["events"], len(fact), len(expected), len(mismatches)))
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
    assert cols == ["order_id", "customer_id", "session_id", "channel", "utm_source", "utm_campaign", "click_id",
                    "campaign_id", "payment_method", "carrier", "city", "country", "order_total", "current_status",
                    "status_rank", "order_created_at", "delivered_at", "cancelled_at", "delivery_lead_hours",
                    "delivery_failed_count", "first_attempt_delivered", "is_cancelled", "is_delivered",
                    "order_seq", "is_first_order", "date_key"]
    assert str(fact_df.schema["order_total"].dataType) == "DecimalType(12,2)"


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


# ---------------------------------------------------------------- group 5: fact_funnel_daily
def test_fact_funnel_daily_rules(spark):
    d = t(4, 12)
    rows = []
    # s1: card order, full funnel, two views of one product
    rows += [ev(0, "s1", d, "product_view", product_id="p1"), ev(1, "s1", d + timedelta(seconds=5), "product_view",
                                                                 product_id="p1"),
             ev(2, "s1", d + timedelta(seconds=10), "add_to_cart", product_id="p1"),
             ev(3, "s1", d + timedelta(seconds=20), "checkout_started"),
             ev(4, "s1", d + timedelta(seconds=30), "purchase", order_id="o1")]
    # s6: cart, no order (EG/organic, unknown payment); s7: cart + purchase without silver_orders row
    rows += [ev(0, "s6", d, "add_to_cart"), ev(0, "s7", d, "add_to_cart"), ev(1, "s7", d, "purchase")]
    rows += [ev(0, "s8", d, "add_to_cart", channel="paid_search")]  # separate group below
    # s2: views only, country SA (denominators 0 -> NULL rates)
    rows += [ev(0, "s2", d, "product_view", country="SA", channel="direct")]
    # s3 bot, excluded entirely; s4 late event is included; s5 null session excluded
    rows += [ev(0, "s3", d, "product_view", is_bot=True, country="XX", channel="bot")]
    rows += [ev(0, "s4", d, "product_view", is_late=True, country="LATE", channel="x")]
    rows += [ev(0, None, d, "product_view", country="NOSESSION", channel="x")]
    orders = [order_row("o1", session="s1", payment="card", created=d)]
    for r in rows:
        if r["session_id"] == "s1":
            r["channel"] = "paid_search"
    out = {(r["country"], r["channel"], r["payment_method"]): r for r in build_fact_funnel_daily(
        mk(spark, "silver_events", rows), mk(spark, "silver_orders", orders)).collect()}
    assert not any(k[0] in ("XX", "NOSESSION") for k in out)  # bot and null session excluded
    assert ("LATE", "x", "unknown") in out and out[("LATE", "x", "unknown")]["sessions"] == 1  # late included
    g = out[("EG", "paid_search", "card")]
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


def test_fact_funnel_daily_clamps_orders_above_cart(spark):
    d = t(4, 12)
    rows = [ev(0, "a", d, "purchase"), ev(0, "b", d, "purchase"), ev(0, "b", d, "add_to_cart")]
    r = build_fact_funnel_daily(mk(spark, "silver_events", rows), mk(spark, "silver_orders", [])).collect()
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
def test_build_all_writes_three_tables_and_reads_back(spark, e2e, tmp_path):
    out_root = str(tmp_path / "gold_ref")
    res = build_all(spark, lake_root=e2e["lake"], out_root=out_root)
    assert set(res) == {"fact_orders", "fact_order_lines", "fact_funnel_daily"}
    counts = {}
    for name, df in res.items():
        back = spark.read.format("delta").load("%s/%s" % (out_root, name))
        counts[name] = (df.count(), back.count())
        assert counts[name][0] == counts[name][1] > 0, name
    print("\nG7 events=%d (built, read_back)=%s" % (e2e["events"], counts))
    assert build_all(spark, lake_root=e2e["lake"]).keys() == res.keys()  # out_root=None writes nothing
