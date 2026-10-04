import datetime as dt
import os
from pathlib import Path

import pytest
from pyspark.sql import Row

from streaming.jobs.bronze_ingest import (
    BRONZE_TABLES,
    bronze_frame,
    bronze_path,
    checkpoint_path,
)

VALUES = ['{"event_id":"evt_1"}', '{"name":"Zoë \u2603 \\"quoted\\" it\'s"}']


def _raw(spark):
    ts = dt.datetime(2026, 1, 1, 12, 0, 0)
    rows = [
        Row(key=b"k", value=v.encode("utf-8"), topic="app.events",
            partition=2, offset=17 + i, timestamp=ts)
        for i, v in enumerate(VALUES)
    ]
    return spark.createDataFrame(rows)


@pytest.mark.spark
def test_bronze_keeps_raw_value_and_metadata(spark):
    out = bronze_frame(_raw(spark))
    assert out.columns == ["raw_value", "kafka_topic", "kafka_partition",
                           "kafka_offset", "kafka_timestamp", "ingest_time",
                           "ingest_date"]
    rows = sorted(out.collect(), key=lambda r: r["kafka_offset"])
    assert [r["raw_value"] for r in rows] == VALUES
    assert rows[0]["kafka_topic"] == "app.events"
    assert rows[0]["kafka_partition"] == 2
    assert rows[0]["kafka_offset"] == 17
    for r in rows:
        assert r["ingest_date"] == r["ingest_time"].date()


def test_tables_and_paths():
    assert BRONZE_TABLES == {
        "app.events": "bronze_app_events",
        "orders": "bronze_orders",
        "delivery.events": "bronze_delivery_events",
    }
    assert Path(bronze_path("lake", "orders")) == Path("lake/bronze/bronze_orders")
    assert Path(checkpoint_path("ck", "delivery.events")) == Path(
        "ck/bronze_delivery_events")


@pytest.mark.spark
def test_delta_round_trip_is_append_only(spark, tmp_path):
    target = str(tmp_path / "bronze_app_events")
    df = bronze_frame(_raw(spark))
    df.write.format("delta").partitionBy("ingest_date").mode("append").save(target)
    back = spark.read.format("delta").load(target)
    assert back.count() == 2
    assert sorted(r["raw_value"] for r in back.collect()) == sorted(VALUES)
    df.write.format("delta").partitionBy("ingest_date").mode("append").save(target)
    assert spark.read.format("delta").load(target).count() == 4


def test_no_object_store_references():
    text = Path("streaming/jobs/bronze_ingest.py").read_text(encoding="utf-8").lower()
    assert "s3a" not in text
    assert "minio" not in text


@pytest.mark.skipif(os.environ.get("RUN_KAFKA_TESTS") != "1",
                    reason="needs a running Kafka broker")
def test_run_against_kafka():
    from streaming.jobs.bronze_ingest import run

    queries = run("localhost:9092", topics=["orders"])
    try:
        assert len(queries) == 1
    finally:
        for q in queries:
            q.stop()
