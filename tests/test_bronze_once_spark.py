"""Bounded Bronze mode (--once): trigger options, waiting for all queries, and a broker-free end to end check.

Not tested here: the Kafka source itself (needs a broker). The Delta streaming source below stands in for it; it
feeds bronze_frame the same columns Kafka gives and runs the same trigger options the job uses.
"""
import datetime as dt
from pathlib import Path

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (BinaryType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

from streaming.jobs.bronze_ingest import await_all, bronze_frame, trigger_options


def test_trigger_options_once_and_continuous():
    assert trigger_options(True) == {"availableNow": True}
    assert trigger_options(False) == {"processingTime": "10 seconds"}


def test_main_has_once_flag_and_passes_it():
    text = Path("streaming/jobs/bronze_ingest.py").read_text(encoding="utf-8")
    assert '"--once"' in text
    assert "once=args.once" in text


class _Q:
    def __init__(self):
        self.waited, self.stopped = False, False

    def awaitTermination(self):
        self.waited = True

    def stop(self):
        self.stopped = True


class _Spark:
    class streams:
        @staticmethod
        def awaitAnyTermination():
            raise AssertionError("bounded mode must wait for every query, not any one")


def test_await_all_bounded_waits_for_every_query():
    qs = [_Q(), _Q(), _Q()]
    await_all(qs, _Spark(), all_done=True)
    assert all(q.waited for q in qs)


def _kafka_like(spark, path, n):
    ts = dt.datetime(2026, 1, 1, 12, 0, 0)
    rows = [Row(key=b"k", value=('{"i":%d}' % i).encode(), topic="orders", partition=0, offset=i, timestamp=ts)
            for i in range(n)]
    schema = StructType([
        StructField("key", BinaryType()), StructField("value", BinaryType()),
        StructField("topic", StringType()), StructField("partition", IntegerType()),
        StructField("offset", LongType()), StructField("timestamp", TimestampType()),
    ])
    spark.createDataFrame(rows, schema).write.format("delta").save(path)


@pytest.mark.spark
def test_bounded_run_ends_by_itself_and_writes_all_rows(spark, tmp_path):
    src, out, ck = str(tmp_path / "src"), str(tmp_path / "out"), str(tmp_path / "ck")
    _kafka_like(spark, src, 50)
    q = (bronze_frame(spark.readStream.format("delta").load(src)).writeStream.format("delta")
         .option("checkpointLocation", ck).partitionBy("ingest_date")
         .trigger(**trigger_options(True)).start(out))
    await_all([q], spark, all_done=True)
    assert not q.isActive
    assert spark.read.format("delta").load(out).count() == 50
