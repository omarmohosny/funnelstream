"""Spark tests for the Silver job (run in Docker: RUN_SPARK_TESTS=1). No Kafka."""
import copy
import json
import random
import shutil
from datetime import datetime, timedelta, timezone
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
from simulator.orders import make_order  # noqa: E402
from simulator.publish import route  # noqa: E402
from streaming.jobs.bronze_ingest import BRONZE_TABLES, bronze_path  # noqa: E402
from streaming.jobs.silver_clean import SILVER_TABLES, load_known_products, process_batch, run  # noqa: E402
from streaming.silver.schemas import TABLE_COLUMNS  # noqa: E402
from streaming.silver.spark_schemas import SCHEMAS, coerce_row  # noqa: E402
from streaming.silver.writer import SILVER_KEYS, silver_path  # noqa: E402

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
MANIFEST_KINDS = ["malformed_json", "missing_field", "unknown_type", "bad_version", "negative_price",
                  "future_timestamp", "null_product_id"]


@lru_cache(maxsize=None)
def simulated(max_events, noise=True, seed=SEED, start=START):
    """(messages, manifest): messages are (topic, raw_value, kafka_timestamp) tuples."""
    rng, noise_rng = random.Random(seed), random.Random(seed * 1_000_003 + 7)
    counter = NoiseCounter()
    cfg = NoiseConfig() if noise else NoiseConfig.off()
    stream = iter_batch(rng, CUSTOMERS, CATALOG, start, max_events=max_events)
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


@lru_cache(maxsize=None)
def clean_failed_order_ids(max_events=30_000):
    """Order ids that have a delivery_failed event in the noise-free stream (same base stream as the noisy one:
    noise uses its own rng). A noise defect (unknown_type, malformed_json, ...) can hit that one message, so
    Silver may legitimately lack the row for an order that did have a failed attempt."""
    msgs, _ = simulated(max_events, noise=False)
    ids = set()
    for _, raw, _ in msgs:
        e = json.loads(raw)
        if e.get("event_type") == "delivery_failed":
            ids.add(e["order_id"])
    return frozenset(ids)


def bronze_df(spark, msgs):
    ing = NOW.replace(tzinfo=None)
    rows = [(raw, topic, 0, i, kts, ing) for i, (topic, raw, kts) in enumerate(msgs)]
    return spark.createDataFrame(rows, BRONZE_SCHEMA).withColumn("ingest_date", F.to_date("ingest_time"))


def silver(spark, lake, table):
    return spark.read.format("delta").load(silver_path(str(lake), table))


def silver_counts(spark, lake):
    from delta.tables import DeltaTable

    return {t: (silver(spark, lake, t).count() if DeltaTable.isDeltaTable(spark, silver_path(str(lake), t)) else 0)
            for t in SILVER_TABLES}


def assert_unique_keys(spark, lake):
    for t in SILVER_TABLES:
        df = silver(spark, lake, t)
        assert df.count() == df.select(*SILVER_KEYS[t]).distinct().count(), t


def code_counts(spark, lake):
    return {r["error_code"]: r["n"] for r in
            silver(spark, lake, "silver_quarantine").groupBy("error_code").agg(F.count("*").alias("n")).collect()}


def within(actual, expected, tol=0.02):
    return abs(actual - expected) <= max(tol * expected, 0)


# 1. counts per table, no duplicate event_id, quarantine equals the manifest
def test_process_batch_counts_and_quarantine_match_manifest(spark, tmp_path):
    msgs, manifest = simulated(30_000)
    assert len(msgs) >= 20_000
    lake = tmp_path / "lake"
    counts = process_batch(spark, bronze_df(spark, msgs), str(lake), None, now=NOW)
    on_disk = silver_counts(spark, lake)
    assert counts == on_disk
    assert all(on_disk[t] > 0 for t in SILVER_TABLES), on_disk
    ev = silver(spark, lake, "silver_events")
    assert ev.count() == ev.select("event_id").distinct().count()
    codes = code_counts(spark, lake)
    inj = manifest["injected"]
    expected = sum(inj[k] for k in MANIFEST_KINDS)
    actual = sum(n for c, n in codes.items() if c != "CANCEL_AFTER_SHIPMENT")
    print("\nT1 events=%d tables=%s quarantine_non_cancel=%d manifest_sum=%d cancel_quarantined=%d "
          "manifest_cancel=%d codes=%s" % (len(msgs), on_disk, actual, expected,
                                           codes.get("CANCEL_AFTER_SHIPMENT", 0), inj["cancel_after_ship"], codes))
    assert within(actual, expected), (actual, expected, codes)
    assert within(codes.get("CANCEL_AFTER_SHIPMENT", 0), inj["cancel_after_ship"], 0.1) or \
        codes.get("CANCEL_AFTER_SHIPMENT", 0) <= inj["cancel_after_ship"]


# 2. idempotence
def test_process_batch_twice_changes_nothing(spark, tmp_path):
    msgs, _ = simulated(6_000)
    lake = str(tmp_path / "lake")
    df = bronze_df(spark, msgs)
    process_batch(spark, df, lake, None, now=NOW)
    first = silver_counts(spark, lake)
    process_batch(spark, df, lake, None, now=NOW)
    second = silver_counts(spark, lake)
    print("\nT2 events=%d first=%s second=%s" % (len(msgs), first, second))
    assert first == second


# 3. duplicates in the input collapse
def test_duplicate_input_collapses_to_one_row(spark, tmp_path):
    msgs, _ = simulated(6_000)
    single, doubled = str(tmp_path / "single"), str(tmp_path / "doubled")
    process_batch(spark, bronze_df(spark, msgs), single, None, now=NOW)
    process_batch(spark, bronze_df(spark, list(msgs) + list(msgs)), doubled, None, now=NOW)
    a, b = silver_counts(spark, single), silver_counts(spark, doubled)
    print("\nT3 events=%d single=%s doubled_input=%s" % (len(msgs), a, b))
    assert a == b
    assert_unique_keys(spark, doubled)


def _order_events(order_id, customer, shipped=True, seed=5):
    o = make_order(random.Random(seed), customer, CATALOG, None, datetime(2026, 9, 1, tzinfo=timezone.utc), order_id)
    events = [e for _, e in o]
    created = events[0]
    ship = next(e for e in events if e["event_type"] == "order_shipped")
    cancel = copy.deepcopy(ship)
    cancel["event_id"] = "evt_cancel_" + order_id
    cancel["event_type"] = "order_cancelled"
    cancel["payload"] = {"reason": "customer_request"}
    if shipped:
        cancel["event_time"] = (datetime.fromisoformat(ship["event_time"]) + timedelta(minutes=5)).isoformat()
        return [created, ship, cancel]
    cancel["event_time"] = (datetime.fromisoformat(created["event_time"]) + timedelta(minutes=5)).isoformat()
    return [created, cancel]


def _as_msgs(events):
    return [("orders", json.dumps(e), datetime.fromisoformat(e["event_time"]).replace(tzinfo=None))
            for e in events]


# 4. cancel after shipment
def test_cancel_after_shipment_is_quarantined_only_after_a_ship(spark, tmp_path):
    lake = str(tmp_path / "lake")
    a = _order_events("ord_a", CUSTOMERS[0], shipped=True)
    b = _order_events("ord_b", CUSTOMERS[1], shipped=False, seed=6)
    process_batch(spark, bronze_df(spark, _as_msgs(a + b)), lake, None, now=NOW)
    status = silver(spark, lake, "silver_order_status")
    cancels = {r["order_id"]: r["event_id"] for r in status.where("status = 'order_cancelled'").collect()}
    assert cancels == {"ord_b": "evt_cancel_ord_b"}
    q = silver(spark, lake, "silver_quarantine").where("error_code = 'CANCEL_AFTER_SHIPMENT'").collect()
    assert len(q) == 1 and json.loads(q[0]["raw_payload"])["event_id"] == "evt_cancel_ord_a"
    # the same cancel arriving in a later batch, the ship already in Silver, is quarantined too
    late = _order_events("ord_c", CUSTOMERS[2], shipped=True, seed=7)
    process_batch(spark, bronze_df(spark, _as_msgs(late[:2])), lake, None, now=NOW)
    process_batch(spark, bronze_df(spark, _as_msgs(late[2:])), lake, None, now=NOW)
    cancels = {r["order_id"] for r in silver(spark, lake, "silver_order_status")
               .where("status = 'order_cancelled'").collect()}
    assert cancels == {"ord_b"}
    assert silver(spark, lake, "silver_quarantine").where("error_code = 'CANCEL_AFTER_SHIPMENT'").count() == 2
    # replay changes nothing
    before = silver_counts(spark, lake)
    process_batch(spark, bronze_df(spark, _as_msgs(a + b + late)), lake, None, now=NOW)
    assert silver_counts(spark, lake) == before
    print("\nT4 counts=%s" % before)


def _write_bronze(spark, lake, msgs):
    df = bronze_df(spark, msgs)
    for topic, table in BRONZE_TABLES.items():
        part = df.where(F.col("kafka_topic") == topic)
        part.write.format("delta").partitionBy("ingest_date").mode("append").save(bronze_path(str(lake), topic))


# 5. streaming end to end, restart and full replay
def test_streaming_run_restart_and_replay_are_idempotent(spark, tmp_path):
    msgs, _ = simulated(8_000)
    lake, ck = tmp_path / "lake", tmp_path / "ck"
    ref = str(tmp_path / "ref")
    # run() validates against the wall clock, so the reference batch does too (now=None); the noise
    # future_timestamp events sit just after NOW and are only "future" for a validator clock at NOW.
    expected = process_batch(spark, bronze_df(spark, msgs), ref, None)
    # topics must all exist in Bronze, even when empty
    _write_bronze(spark, lake, msgs)
    q = run(str(lake), str(ck), once=True)
    q.awaitTermination(600)
    first = silver_counts(spark, lake)
    print("\nT5 events=%d expected(batch)=%s first_run=%s" % (len(msgs), expected, first))
    assert first == silver_counts(spark, ref)
    assert_unique_keys(spark, lake)
    q = run(str(lake), str(ck), once=True)
    q.awaitTermination(600)
    second = silver_counts(spark, lake)
    shutil.rmtree(ck / "silver_clean")
    q = run(str(lake), str(ck), once=True)
    q.awaitTermination(600)
    third = silver_counts(spark, lake)
    print("T5 after_rerun=%s after_checkpoint_deleted=%s" % (second, third))
    assert first == second == third


# 6. orders and items agree
def test_every_order_has_items_and_totals_match(spark, tmp_path):
    msgs, _ = simulated(10_000)
    lake = str(tmp_path / "lake")
    process_batch(spark, bronze_df(spark, msgs), lake, None, now=NOW)
    orders = silver(spark, lake, "silver_orders")
    items = silver(spark, lake, "silver_order_items")
    sums = items.groupBy("order_id").agg(F.sum("line_total").alias("s"), F.count("*").alias("n"))
    joined = orders.join(sums, "order_id", "left")
    n_orders = orders.count()
    assert n_orders > 0
    assert joined.where(F.col("n").isNull()).count() == 0
    bad = joined.where(F.abs(F.col("order_total") - F.col("s")) > Decimal("0.01")).count()
    print("\nT6 events=%d orders=%d order_without_items=0 total_mismatch=%d" % (len(msgs), n_orders, bad))
    assert bad == 0
    assert items.join(orders, "order_id", "left_anti").count() == 0


# 7. reference products
def test_known_products_loading_and_unknown_product_quarantine(spark, tmp_path):
    from scripts.seed_silver_reference import catalog_to_silver_rows
    from streaming.silver.writer import merge_insert

    lake = tmp_path / "lake"
    assert load_known_products(str(lake), spark) is None
    products, _ = catalog_to_silver_rows(CATALOG, CUSTOMERS, NOW)
    data = [tuple(coerce_row("silver_products", r).values()) for r in products]
    merge_insert(spark, silver_path(str(lake), "silver_products"),
                 spark.createDataFrame(data, SCHEMAS["silver_products"]), SILVER_KEYS["silver_products"])
    known = load_known_products(str(lake), spark)
    assert known == CATALOG_IDS
    msgs, manifest = simulated(30_000)
    process_batch(spark, bronze_df(spark, msgs), str(lake), known, now=NOW)
    codes = code_counts(spark, lake)
    expected = manifest["injected"]["unknown_product_id"]
    print("\nT7 events=%d known=%d UNKNOWN_PRODUCT=%d manifest_unknown_product_id=%d" % (
        len(msgs), len(known), codes.get("UNKNOWN_PRODUCT", 0), expected))
    assert expected > 0 and within(codes.get("UNKNOWN_PRODUCT", 0), expected)
    # without known products the same noise is not quarantined as UNKNOWN_PRODUCT
    lake2 = str(tmp_path / "lake2")
    process_batch(spark, bronze_df(spark, msgs), lake2, None, now=NOW)
    assert code_counts(spark, lake2).get("UNKNOWN_PRODUCT", 0) == 0


# ---- review fixes ----
def ref_counts(spark, lake):
    return {t: silver(spark, lake, t).count() for t in ("silver_products", "silver_customers")}


def test_seed_is_idempotent_and_adds_versions_only_for_changes(spark, tmp_path):
    import copy as _copy
    from scripts.seed_silver_reference import seed

    lake = str(tmp_path / "lake")
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    customers = make_customers(SEED, 300)
    assert seed(spark, lake, t0, CATALOG, customers) == (200, 300)
    seed(spark, lake, t0, CATALOG, customers)
    c = ref_counts(spark, lake)
    assert c["silver_products"] == 200 and c["silver_customers"] == 300
    t1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    seed(spark, lake, t1, CATALOG, customers)
    c = ref_counts(spark, lake)
    assert c["silver_products"] == 200 and c["silver_customers"] == 300  # nothing changed, no new version
    changed = _copy.deepcopy(CATALOG)
    changed[0]["price"] = changed[0]["price"] + 10
    seed(spark, lake, t1, changed, customers)
    seed(spark, lake, t1, changed, customers)
    c = ref_counts(spark, lake)
    print("\nSEED counts after changed price=%s" % c)
    assert c["silver_products"] == 201 and c["silver_customers"] == 300
    rows = silver(spark, lake, "silver_products").where(
        F.col("product_id") == changed[0]["product_id"]).collect()
    assert len(rows) == 2


def test_silver_tables_are_partitioned_and_older_date_replay_dedups(spark, tmp_path):
    from streaming.silver.writer import PARTITION_COLS

    lake = str(tmp_path / "lake")
    old, _ = simulated(4_000, seed=21, start=START)
    new, _ = simulated(4_000, seed=22, start=START + timedelta(days=10))
    process_batch(spark, bronze_df(spark, old), lake, None, now=NOW)
    process_batch(spark, bronze_df(spark, new), lake, None, now=NOW)
    first = silver_counts(spark, lake)
    for table in ("silver_events", "silver_orders", "silver_order_status"):
        detail = spark.sql("DESCRIBE DETAIL delta.`%s`" % silver_path(lake, table)).collect()[0]
        assert list(detail["partitionColumns"]) == ["event_date"] == [PARTITION_COLS[table]]
    for table in ("silver_order_items", "silver_quarantine"):
        detail = spark.sql("DESCRIBE DETAIL delta.`%s`" % silver_path(lake, table)).collect()[0]
        assert list(detail["partitionColumns"]) == []
    n_dates = silver(spark, lake, "silver_events").select("event_date").distinct().count()
    assert n_dates >= 2
    process_batch(spark, bronze_df(spark, old), lake, None, now=NOW)  # replay the older date
    print("\nPART dates=%d counts=%s replay_equal=%s" % (n_dates, first, silver_counts(spark, lake) == first))
    assert silver_counts(spark, lake) == first
    assert_unique_keys(spark, lake)


def test_cancel_before_ship_stays_and_full_replay_changes_nothing(spark, tmp_path):
    lake = str(tmp_path / "lake")
    evs = _order_events("ord_r", CUSTOMERS[3], shipped=True, seed=8)
    created, ship, cancel = evs
    process_batch(spark, bronze_df(spark, _as_msgs([created, cancel])), lake, None, now=NOW)
    process_batch(spark, bronze_df(spark, _as_msgs([ship])), lake, None, now=NOW)
    first = silver_counts(spark, lake)
    assert silver(spark, lake, "silver_order_status").where("status = 'order_cancelled'").count() == 1
    assert first["silver_quarantine"] == 0
    process_batch(spark, bronze_df(spark, _as_msgs(evs)), lake, None, now=NOW)
    assert silver_counts(spark, lake) == first
    print("\nCANCEL-BEFORE-SHIP counts=%s unchanged after replay" % first)


def test_ship_then_cancel_gives_one_quarantine_row_and_replay_keeps_one(spark, tmp_path):
    lake = str(tmp_path / "lake")
    evs = _order_events("ord_s", CUSTOMERS[4], shipped=True, seed=9)
    created, ship, cancel = evs
    process_batch(spark, bronze_df(spark, _as_msgs([created, ship])), lake, None, now=NOW)
    process_batch(spark, bronze_df(spark, _as_msgs([cancel])), lake, None, now=NOW)
    first = silver_counts(spark, lake)
    assert first["silver_quarantine"] == 1
    process_batch(spark, bronze_df(spark, _as_msgs(evs)), lake, None, now=NOW)
    assert silver_counts(spark, lake) == first
    assert silver(spark, lake, "silver_order_status").where("status = 'order_cancelled'").count() == 0


def test_driver_timezone_does_not_change_results(spark, tmp_path):
    import os
    import time

    if not hasattr(time, "tzset"):
        pytest.skip("tzset not available")
    base = None
    for hint, e in iter_batch(random.Random(3), CUSTOMERS, CATALOG, START, max_events=300):
        if e["event_type"] == "page_view":
            base = e
            break
    assert base is not None
    msgs = []
    for i, minutes_before in enumerate((5, 20)):  # kafka time is 2026-10-04T23:30Z
        e = copy.deepcopy(base)
        e["event_id"] = "evt_tz_%d" % i
        e["event_time"] = (datetime(2026, 10, 4, 23, 30, tzinfo=timezone.utc)
                           - timedelta(minutes=minutes_before)).isoformat()
        msgs.append(("app.events", json.dumps(e), datetime(2026, 10, 4, 23, 30)))
    df = bronze_df(spark, msgs)
    lake = str(tmp_path / "lake")
    old_tz = os.environ.get("TZ")
    os.environ["TZ"] = "Asia/Tokyo"
    time.tzset()
    try:
        process_batch(spark, df, lake, None, now=datetime(2026, 10, 6, tzinfo=timezone.utc))
    finally:
        if old_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = old_tz
        time.tzset()
    rows = {r["event_id"]: r for r in silver(spark, lake, "silver_events").collect()}
    print("\nTZ rows=%s" % {k: (str(v["event_date"]), v["is_late"]) for k, v in rows.items()})
    assert len(rows) == 2
    assert all(str(r["event_date"]) == "2026-10-05" for r in rows.values())  # 23:xx UTC is 02:xx Cairo next day
    assert rows["evt_tz_0"]["is_late"] is False and rows["evt_tz_1"]["is_late"] is True


# C2: order_returned, COD rule and platform on a simulated noisy dataset
def test_c2_returned_cod_and_platform_on_noisy_dataset(spark, tmp_path):
    msgs, _ = simulated(30_000)
    assert 20_000 <= len(msgs) <= 40_000
    lake = tmp_path / "lake"
    process_batch(spark, bronze_df(spark, msgs), str(lake), None, now=NOW)
    ev, orders, st = (silver(spark, lake, t) for t in ("silver_events", "silver_orders", "silver_order_status"))

    # order_returned flows as a normal status row
    returned = st.filter("status = 'order_returned'")
    n_returned = returned.count()
    assert n_returned > 0
    assert returned.filter("status_rank != 7 OR reason IS NULL").count() == 0
    ret_ids = returned.select("order_id").distinct()
    with_failed = ret_ids.join(st.filter("status = 'delivery_failed'").select("order_id").distinct(), "order_id")
    # a returned order lacks a delivery_failed row only when noise damaged that message (checked on the clean stream)
    missing = [r["order_id"] for r in ret_ids.join(with_failed, "order_id", "left_anti").collect()]
    assert set(missing) <= clean_failed_order_ids(), missing
    no_failed = 0
    bad_final = ret_ids.join(st.filter("status IN ('delivered', 'order_completed')"), "order_id").count()
    assert no_failed == 0 and bad_final == 0

    # COD orders have no payment_authorised row
    cod = orders.filter("payment_method = 'cod'").select("order_id")
    n_cod = cod.count()
    cod_authorised = cod.join(st.filter("status = 'payment_authorised'"), "order_id").count()
    prepaid_authorised = orders.filter("payment_method != 'cod'").select("order_id").join(
        st.filter("status = 'payment_authorised'"), "order_id").count()
    assert n_cod > 0 and prepaid_authorised > 0
    assert cod_authorised == 0

    # platform values and the app share among mobile sessions
    vals = {r["platform"] for r in ev.select("platform").distinct().collect()}
    assert vals <= {"app", "web", None}
    sess = ev.filter("session_id IS NOT NULL AND platform IS NOT NULL").groupBy("session_id").agg(
        F.countDistinct("platform").alias("n_platform"), F.first("platform").alias("platform"),
        F.max(F.when(F.col("device") == "mobile", 1).otherwise(0)).alias("is_mobile"))
    mobile = sess.filter("is_mobile = 1")
    n_mobile = mobile.count()
    n_app = mobile.filter("platform = 'app'").count()
    share = n_app / n_mobile
    assert 0.55 <= share <= 0.85, (n_app, n_mobile)
    n_null_events = ev.filter("platform IS NULL").count()

    # silver_orders.platform equals the platform of the order's session
    o = orders.filter("platform IS NOT NULL").select("order_id", "session_id", F.col("platform").alias("op"))
    j = o.join(sess.select("session_id", F.col("platform").alias("sp")), "session_id", "left")
    unmatched = j.filter("sp IS NULL").count()
    mismatch = j.filter("sp IS NOT NULL AND sp != op").count()
    n_orders = orders.count()
    print("\nC2 events=%d returned_rows=%d returned_orders=%d returned_without_failed=%d returned_with_delivered=%d "
          "cod_orders=%d cod_with_payment_authorised=%d prepaid_with_payment_authorised=%d platform_values=%s "
          "events_platform_null=%d mobile_sessions=%d app_mobile_sessions=%d app_share=%.4f orders=%d "
          "orders_platform_null=%d orders_session_not_found=%d orders_platform_mismatch=%d" % (
              len(msgs), n_returned, ret_ids.count(), no_failed, bad_final, n_cod, cod_authorised,
              prepaid_authorised, sorted(str(v) for v in vals), n_null_events, n_mobile, n_app, share,
              n_orders, orders.filter("platform IS NULL").count(), unmatched, mismatch))
    null_platform_orders = orders.filter("platform IS NULL").count()
    assert n_orders > 0 and unmatched == 0 and null_platform_orders == 0  # mismatch == 0 is not vacuous
    assert sess.filter("n_platform > 1").count() == 0
    assert mismatch == 0


# D2: district and page_url through Silver (spec section 18)
def test_d2_district_and_utm_on_noisy_dataset(spark, tmp_path):
    from contracts.geo import DISTRICTS, normalize_district
    from contracts.normalize import normalize_city

    msgs, manifest = simulated(30_000)
    assert manifest["injected"]["dirty_district"] > 0 and manifest["injected"]["dirty_utm"] > 0
    lake = tmp_path / "lake"
    stats = {}
    process_batch(spark, bronze_df(spark, msgs), str(lake), None, now=NOW, stats=stats)
    ev, orders = silver(spark, lake, "silver_events"), silver(spark, lake, "silver_orders")

    # expected UTM presence from the raw messages (one entry per event id)
    has_utm, raw_district = {}, {}
    for topic, raw, _ in msgs:
        try:
            e = json.loads(raw)
            url = (e.get("payload") or {}).get("page_url")
        except Exception:
            continue
        if isinstance(e, dict) and e.get("event_id") is not None and topic == "app.events":
            has_utm[e["event_id"]] = isinstance(url, str) and "utm_campaign=" in url
            raw_district[e["event_id"]] = (e.get("city"), e.get("district"))
    rows = ev.select("event_id", "city", "district", "utm_source", "utm_medium", "utm_campaign", "page_url").collect()
    assert rows
    n_known = n_known_unknown = n_utm_rows = n_mismatch = n_expected_district_wrong = 0
    for r in rows:
        assert r["district"] is not None
        if r["city"] in DISTRICTS:
            n_known += 1
            n_known_unknown += r["district"] == "unknown"
            assert r["district"] == "unknown" or r["district"] in DISTRICTS[r["city"]]  # no violation survives
            c, d = raw_district[r["event_id"]]
            n_expected_district_wrong += r["district"] != normalize_district(normalize_city(c), d)
        got = all(r[k] is not None for k in ("utm_source", "utm_medium", "utm_campaign"))
        none = all(r[k] is None for k in ("utm_source", "utm_medium", "utm_campaign"))
        want = has_utm[r["event_id"]]
        n_utm_rows += want
        n_mismatch += (want and not got) or (not want and not none)
        if want:
            assert r["utm_source"] == r["utm_source"].strip().lower() and r["page_url"]
    n_o = orders.count()
    o_null_district = orders.filter("district IS NULL").count()
    o_viol = [(r["city"], r["district"]) for r in orders.select("city", "district").distinct().collect()
              if r["city"] in DISTRICTS and r["district"] != "unknown" and r["district"] not in DISTRICTS[r["city"]]]
    o_campaign = orders.filter("utm_campaign IS NOT NULL")
    print("\nD2 events=%d silver_events=%d known_city=%d known_city_district_unknown=%d district_vs_raw_mismatch=%d "
          "utm_expected_rows=%d utm_mismatch=%d silver_orders=%d orders_null_district=%d orders_hierarchy_violations=%d "
          "orders_with_campaign=%d orders_with_medium=%d batch_district_violations=%s" % (
              len(msgs), len(rows), n_known, n_known_unknown, n_expected_district_wrong, n_utm_rows, n_mismatch, n_o,
              o_null_district, len(o_viol), o_campaign.count(), orders.filter("utm_medium IS NOT NULL").count(),
              stats.get("district_violations")))
    assert n_known > 0 and n_utm_rows > 0 and n_mismatch == 0
    assert n_expected_district_wrong == 0
    assert o_null_district == 0 and not o_viol and n_o > 0
    assert o_campaign.filter("utm_medium IS NULL").count() == 0
    assert stats["district_violations"] == 0  # the simulator never puts a district under the wrong city


def test_d2_batch_reports_district_violations_and_keeps_rows(spark, tmp_path):
    base = _order_events("ord_v", CUSTOMERS[5], shipped=False, seed=12)
    created = copy.deepcopy(base[0])
    created["city"], created["district"] = "Cairo", "Sidi Gaber"      # wrong city: violation
    good = copy.deepcopy(base[0])
    good["event_id"], good["order_id"], good["city"], good["district"] = "evt_g", "ord_g", "Cairo", "Maadi"
    for e in (created, good):
        for it in e["payload"]["items"]:
            it["product_id"] = CATALOG[0]["product_id"]
    msgs = [("orders", json.dumps(e), START.replace(tzinfo=None)) for e in (created, good)]
    lake = str(tmp_path / "lake")
    stats = {}
    counts = process_batch(spark, bronze_df(spark, msgs), lake, None, now=NOW, stats=stats)
    got = {r["order_id"]: r["district"] for r in silver(spark, lake, "silver_orders").collect()}
    assert counts["silver_quarantine"] == 0
    assert got[created["order_id"]] == "unknown" and got["ord_g"] == "Maadi"
    assert stats["district_violations"] == 1


def test_d2_seed_carries_district_and_stays_idempotent(spark, tmp_path):
    from contracts.geo import DISTRICTS
    from scripts.seed_silver_reference import seed

    lake = str(tmp_path / "lake")
    customers = make_customers(SEED, 300)
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    seed(spark, lake, t0, CATALOG, customers)
    seed(spark, lake, t0, CATALOG, customers)
    seed(spark, lake, t1, CATALOG, customers)   # unchanged attributes (district included): no new version
    cust = silver(spark, lake, "silver_customers")
    assert cust.columns.index("district") == cust.columns.index("city") + 1
    assert cust.count() == 300
    pairs = {(r["city"], r["district"]) for r in cust.select("city", "district").distinct().collect()}
    assert pairs and all(d in DISTRICTS[c] for c, d in pairs)
    # a changed district is a real change: exactly one new version for that customer
    moved = [dict(c) for c in customers]
    moved[0]["district"] = next(d for d in DISTRICTS[moved[0]["city"]] if d != moved[0]["district"]) \
        if len(DISTRICTS[moved[0]["city"]]) > 1 else moved[0]["district"]
    seed(spark, lake, t1, CATALOG, moved)
    seed(spark, lake, t1, CATALOG, moved)
    expected = 301 if moved[0]["district"] != customers[0]["district"] else 300
    assert silver(spark, lake, "silver_customers").count() == expected
