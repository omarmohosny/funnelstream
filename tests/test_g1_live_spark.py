"""Spark tests for G1 (run in Docker: RUN_SPARK_TESTS=1): refresh_live_metrics / refresh_health totals against direct
counts, the minute sets process_batch reports, and the Silver stream with a LiveSink over a Bronze table."""
import json
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import pytest

pytestmark = pytest.mark.spark

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (IntegerType, LongType, StringType, StructField, StructType,  # noqa: E402
                               TimestampType)

import streaming.jobs.silver_clean as silver_clean  # noqa: E402
from scripts.run_simulator import iter_batch  # noqa: E402
from simulator.catalog import make_catalog, make_customers  # noqa: E402
from simulator.noise import NoiseConfig, NoiseCounter, apply_noise  # noqa: E402
from simulator.publish import route  # noqa: E402
from streaming.gold.bots import mark_bot_sessions  # noqa: E402
from streaming.gold.live import build_live_metrics_minute, minute_epochs  # noqa: E402
from streaming.jobs.pipeline import ensure_bronze_tables  # noqa: E402
from streaming.silver.spark_schemas import SCHEMAS, coerce_row  # noqa: E402
from streaming.silver.writer import PARTITION_COLS, SILVER_KEYS, merge_insert, silver_path  # noqa: E402
from warehouse import live_store  # noqa: E402
from warehouse.store import GoldStore  # noqa: E402

T0 = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 8, 13, 0, tzinfo=timezone.utc)
COUNTRIES = ["EG", "SA", "AE"]
BRONZE_SCHEMA = StructType([
    StructField("raw_value", StringType()), StructField("kafka_topic", StringType()),
    StructField("kafka_partition", IntegerType()), StructField("kafka_offset", LongType()),
    StructField("kafka_timestamp", TimestampType()), StructField("ingest_time", TimestampType()),
])


class MemStore(GoldStore):
    def __init__(self):
        self.tables = defaultdict(dict)

    def upsert(self, table, rows, key_cols):
        for r in rows:
            self.tables[table].setdefault(tuple(r[k] for k in key_cols), {}).update(r)
        return len(rows)

    def query(self, sql, params=None, retry=True):
        lo, hi = params
        return [(r["finished_at"],) for r in self.tables["pipeline_batch_log"].values() if lo <= r["finished_at"] < hi]

    def execute(self, sql, retry=True):
        pass

    def close(self):
        pass

    def snapshot(self, table):
        return {k: dict(v) for k, v in self.tables[table].items()}


def ev(i, minute, country, etype="product_view", session=None, late=False, arrival=None):
    et = T0 + timedelta(minutes=minute, seconds=i % 50)
    return {"event_id": "e%d" % i, "event_type": etype, "event_time": et,
            "kafka_timestamp": arrival or et + timedelta(seconds=1), "is_late": late, "is_bot": False,
            "session_id": session or "s%d" % (i % 17), "country": country, "event_date": et.date()}


def write_events(spark, lake, rows):
    df = spark.createDataFrame([tuple(coerce_row("silver_events", r).values()) for r in rows],
                               SCHEMAS["silver_events"])
    merge_insert(spark, silver_path(lake, "silver_events"), df, SILVER_KEYS["silver_events"],
                 PARTITION_COLS["silver_events"])


def write_quarantine(spark, lake, rows):
    df = spark.createDataFrame([tuple(coerce_row("silver_quarantine", r).values()) for r in rows],
                               SCHEMAS["silver_quarantine"])
    merge_insert(spark, silver_path(lake, "silver_quarantine"), df, SILVER_KEYS["silver_quarantine"])


def dataset():
    rnd, rows, i = random.Random(5), [], 0
    for minute in range(20):
        for _ in range(rnd.randint(8, 25)):
            i += 1
            rows.append(ev(i, minute, rnd.choice(COUNTRIES),
                           rnd.choice(["product_view", "add_to_cart", "purchase", "page_view"])))
    for _ in range(130):  # one bot session: more than 100 events inside 10 seconds, minute 5
        i += 1
        rows.append(dict(ev(i, 5, "EG", "page_view", session="bot1"), event_time=T0 + timedelta(minutes=5, seconds=i % 5)))
    for _ in range(6):  # late events never count in the live metrics
        i += 1
        rows.append(ev(i, 7, "EG", "product_view", late=True))
    return rows


def as_live(rows):
    return {(datetime.fromtimestamp(r["m"], tz=timezone.utc), r["country"]): tuple(r[c] for c in live_store.LIVE_METRIC_COLUMNS)
            for r in rows}


def store_live(store):
    return {k: tuple(v[c] for c in live_store.LIVE_METRIC_COLUMNS) for k, v in store.tables["live_metrics_minute"].items()}


def builder_totals(spark, lake):
    ev_df = spark.read.format("delta").load(silver_path(lake, "silver_events"))
    out = build_live_metrics_minute(mark_bot_sessions(ev_df)).select(
        F.unix_timestamp("minute_start").alias("m"), "country", *live_store.LIVE_METRIC_COLUMNS).collect()
    return as_live([r.asDict() for r in out])


def all_minutes(rows):
    return minute_epochs([r["event_time"] for r in rows])


# ---------------------------------------------------------------- refresh_live_metrics
def test_refresh_live_metrics_equals_the_builder_and_drops_bots_and_late(spark, tmp_path):
    lake, rows = str(tmp_path / "lake"), dataset()
    write_events(spark, lake, rows)
    store = MemStore()
    n = live_store.refresh_live_metrics(spark, lake, all_minutes(rows), store)
    expected = builder_totals(spark, lake)
    assert n == len(expected) > 20 and store_live(store) == expected
    assert sum(v[0] for k, v in expected.items() if k[0] == T0 + timedelta(minutes=5)) < 100  # the 130 bot rows are gone
    again = live_store.refresh_live_metrics(spark, lake, all_minutes(rows), store)
    assert again == n and store_live(store) == expected  # a repeat gives the same totals, nothing doubled
    print("\nG1 live minutes=%d rows=%d bot_minute_events=%d" % (
        len(all_minutes(rows)), n, sum(v[0] for k, v in expected.items() if k[0] == T0 + timedelta(minutes=5))))


def test_recompute_after_an_out_of_order_event_changes_only_that_minute(spark, tmp_path):
    lake, rows = str(tmp_path / "lake"), dataset()
    write_events(spark, lake, rows)
    store = MemStore()
    live_store.refresh_live_metrics(spark, lake, all_minutes(rows), store)
    before = store_live(store)
    write_events(spark, lake, [ev(9001, 3, "SA", "purchase", session="sNEW")])  # arrives later, minute 3
    live_store.refresh_live_metrics(spark, lake, [T0 + timedelta(minutes=3)], store)
    after = store_live(store)
    changed = {k for k in after if after[k] != before.get(k)}
    assert changed == {(T0 + timedelta(minutes=3), "SA")}
    old = before[(T0 + timedelta(minutes=3), "SA")]
    assert after[(T0 + timedelta(minutes=3), "SA")][0] == old[0] + 1  # events: recalculated total, +1 event
    assert after[(T0 + timedelta(minutes=3), "SA")][4] == old[4] + 1  # orders_placed
    assert after == builder_totals(spark, lake)


def test_refresh_live_metrics_with_a_range_filter_for_many_minutes(spark, tmp_path, monkeypatch):
    import streaming.gold.live as live

    monkeypatch.setattr(live, "MAX_MINUTE_LIST", 3)  # force the range branch
    lake, rows = str(tmp_path / "lake"), dataset()
    write_events(spark, lake, rows)
    store = MemStore()
    live_store.refresh_live_metrics(spark, lake, all_minutes(rows), store)
    assert store_live(store) == builder_totals(spark, lake)


def test_null_country_is_stored_as_unknown(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write_events(spark, lake, [ev(1, 0, None), ev(2, 0, "EG")])
    store = MemStore()
    live_store.refresh_live_metrics(spark, lake, [T0], store)
    assert {k[1] for k in store.tables["live_metrics_minute"]} == {"unknown", "EG"}


# ---------------------------------------------------------------- refresh_health
def test_refresh_health_equals_direct_counts(spark, tmp_path):
    lake = str(tmp_path / "lake")
    rows, i = [], 0
    for minute, (n, late) in enumerate([(5, 0), (9, 3), (0, 0), (4, 4), (7, 1)]):
        for k in range(n):
            i += 1  # arrival minute = `minute`; event_time is 20 minutes earlier for the late ones
            rows.append(ev(i, minute - (20 if k < late else 0), "EG", late=k < late,
                           arrival=T0 + timedelta(minutes=minute, seconds=k)))
    write_events(spark, lake, rows)
    write_quarantine(spark, lake, [
        {"quarantine_id": "q%d" % j, "raw_payload": "x", "source_topic": "orders", "error_code": "BAD_JSON",
         "error_detail": "d", "received_at": T0 + timedelta(minutes=m, seconds=j)} for j, m in enumerate([0, 0, 1, 3, 3, 3])])
    store = MemStore()
    for j, m in enumerate([0, 1, 1, 4]):  # silver batches that finished in minutes 0, 1, 1, 4
        live_store.log_batch(store, "r", j, T0 + timedelta(minutes=m, seconds=30))
    n = live_store.refresh_health(spark, lake, [T0 + timedelta(minutes=m) for m in range(5)], store)
    assert n == 5
    got = {k[0]: tuple(v[c] for c in live_store.HEALTH_COLUMNS) for k, v in store.tables["pipeline_health_minute"].items()}
    want = {T0 + timedelta(minutes=0): (5, 0, 2, 1), T0 + timedelta(minutes=1): (9, 3, 1, 2),
            T0 + timedelta(minutes=2): (0, 0, 0, 0), T0 + timedelta(minutes=3): (4, 4, 3, 0),
            T0 + timedelta(minutes=4): (7, 1, 0, 1)}
    assert got == want
    before = store.snapshot("pipeline_health_minute")
    live_store.refresh_health(spark, lake, [T0 + timedelta(minutes=m) for m in range(5)], store)
    assert store.snapshot("pipeline_health_minute") == before
    late_rate = sum(v[1] for v in got.values()) / sum(v[0] for v in got.values())
    print("\nG1 health minutes=5 events=%d late=%d late_rate=%.4f" % (
        sum(v[0] for v in got.values()), sum(v[1] for v in got.values()), late_rate))


# ---------------------------------------------------------------- process_batch minutes and the Silver stream
def bronze_rows(n=1500, seed=3):
    cat, cust = make_catalog(seed), make_customers(seed, 200)
    start = datetime(2026, 10, 5, 9, 0, tzinfo=timezone.utc)  # in the past for any container clock (FUTURE_TIMESTAMP)
    stream = iter_batch(random.Random(seed), cust, cat, start, max_events=n)
    now = datetime(2026, 10, 6, 14, 0, tzinfo=timezone.utc)
    rows = []
    for k, (hint, payload) in enumerate(apply_noise(stream, random.Random(seed + 1), NoiseConfig.off(),
                                                    {p["product_id"] for p in cat}, NoiseCounter(), now=now)):
        topic, _ = route(hint, payload)
        kts = datetime.fromisoformat(payload["event_time"]).astimezone(timezone.utc) + timedelta(seconds=1)
        rows.append((json.dumps(payload), topic, 0, k, kts.replace(tzinfo=None), now.replace(tzinfo=None)))
    return rows


def bronze_df(spark, rows):
    return spark.createDataFrame(rows, BRONZE_SCHEMA).withColumn("ingest_date", F.to_date("ingest_time"))


def distinct_minutes(df, col):
    sec = F.unix_timestamp(col)
    return sorted(r["m"] for r in df.select((sec - (sec % 60)).alias("m")).distinct().collect())


def test_process_batch_reports_the_minutes_it_touched(spark, tmp_path):
    lake, stats = str(tmp_path / "lake"), {"collect_minutes": True}
    silver_clean.process_batch(spark, bronze_df(spark, bronze_rows()), lake, None, stats=stats)
    ev_df = spark.read.format("delta").load(silver_path(lake, "silver_events"))
    assert stats["event_minutes"] == distinct_minutes(ev_df, "event_time")
    assert stats["arrival_minutes"] == distinct_minutes(ev_df, "kafka_timestamp")
    assert len(stats["event_minutes"]) > 5
    plain = {}
    silver_clean.process_batch(spark, bronze_df(spark, bronze_rows()), lake, None, stats=plain)
    assert "event_minutes" not in plain  # off unless asked: the existing flow is unchanged


def run_silver_once(spark, lake, ckpt, sink):
    q = silver_clean.run(lake, ckpt, once=True, sink=sink)
    q.awaitTermination(300)
    return q


def test_silver_stream_with_a_live_sink_fills_the_tables_and_a_second_run_changes_nothing(spark, tmp_path):
    lake, ckpt = str(tmp_path / "lake"), str(tmp_path / "ckpt")
    ensure_bronze_tables(spark, lake)
    bronze_df(spark, bronze_rows()).write.format("delta").mode("append").partitionBy("ingest_date").save(
        str(tmp_path / "lake" / "bronze" / "bronze_app_events"))  # topic routing is irrelevant to Bronze
    store = MemStore()
    sink = live_store.LiveSink(lake, store=store, job_run_id="run-1")
    run_silver_once(spark, lake, ckpt, sink)
    assert store.tables["live_metrics_minute"] and store.tables["pipeline_health_minute"]
    assert ("silver_job",) in store.tables["pipeline_heartbeat"] and store.tables["pipeline_batch_log"]
    assert store_live(store) == builder_totals(spark, lake)
    ev_df = spark.read.format("delta").load(silver_path(lake, "silver_events"))
    assert sum(v["events"] for v in store.tables["pipeline_health_minute"].values()) == ev_df.count()
    first = {t: store.snapshot(t) for t in ("live_metrics_minute", "pipeline_health_minute")}
    ckpt2 = str(tmp_path / "ckpt2")  # a replay: the whole Bronze table again, Silver MERGE inserts nothing new
    sink2 = live_store.LiveSink(lake, store=store, job_run_id="run-2")
    run_silver_once(spark, lake, ckpt2, sink2)
    assert store.snapshot("live_metrics_minute") == first["live_metrics_minute"]  # identical totals
    health = store.snapshot("pipeline_health_minute")
    for minute, row in first["pipeline_health_minute"].items():  # silver_batches may grow; the data counts may not
        for c in ("events", "late_events", "quarantined"):
            assert health[minute][c] == row[c], (minute, c)
    print("\nG1 stream live_rows=%d health_rows=%d silver_events=%d" % (
        len(first["live_metrics_minute"]), len(first["pipeline_health_minute"]), ev_df.count()))


def test_silver_stream_survives_a_database_outage(spark, tmp_path):
    class Down(MemStore):
        def upsert(self, *a, **k):
            raise ConnectionError("postgres is down")

    lake, ckpt = str(tmp_path / "lake"), str(tmp_path / "ckpt")
    ensure_bronze_tables(spark, lake)
    bronze_df(spark, bronze_rows(400)).write.format("delta").mode("append").partitionBy("ingest_date").save(
        str(tmp_path / "lake" / "bronze" / "bronze_app_events"))
    run_silver_once(spark, lake, ckpt, live_store.LiveSink(lake, store=Down()))
    assert spark.read.format("delta").load(silver_path(lake, "silver_events")).count() > 0  # Silver still written


def test_ensure_bronze_tables_creates_empty_tables_once_and_silver_can_start(spark, tmp_path):
    lake = str(tmp_path / "lake")
    assert sorted(ensure_bronze_tables(spark, lake)) == ["bronze_app_events", "bronze_delivery_events",
                                                         "bronze_orders"]
    assert ensure_bronze_tables(spark, lake) == []
    q = silver_clean.run(lake, str(tmp_path / "ckpt"), once=True, live_store=False)
    q.awaitTermination(300)
    assert q.exception() is None
    assert spark.read.format("delta").load(str(tmp_path / "lake" / "bronze" / "bronze_orders")).count() == 0


def test_idle_silver_stream_keeps_reporting_alive(spark, tmp_path, monkeypatch):
    """No data, so Spark runs no micro-batch; the timer thread still writes the silver_job heartbeat."""
    import time

    monkeypatch.setattr(live_store, "LIVENESS_INTERVAL", 1.0)
    lake = str(tmp_path / "lake")
    ensure_bronze_tables(spark, lake)
    store = MemStore()
    q = silver_clean.run(lake, str(tmp_path / "ckpt"), once=False, sink=live_store.LiveSink(lake, store=store))
    try:
        deadline = time.time() + 60
        while ("silver_job",) not in store.tables["pipeline_heartbeat"] and time.time() < deadline:
            time.sleep(0.5)
        assert store.tables["pipeline_heartbeat"][("silver_job",)]["details"] == "alive"
        assert not store.tables["pipeline_batch_log"]  # and there was no batch
    finally:
        q.stop()
