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
def simulated(max_events, noise=True, seed=SEED):
    """(messages, manifest): messages are (topic, raw_value, kafka_timestamp) tuples."""
    rng, noise_rng = random.Random(seed), random.Random(seed * 1_000_003 + 7)
    counter = NoiseCounter()
    cfg = NoiseConfig() if noise else NoiseConfig.off()
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
