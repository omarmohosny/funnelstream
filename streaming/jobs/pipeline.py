"""Combined streaming job: the three Bronze queries and the Silver query in ONE process and ONE SparkSession.

python -m streaming.jobs.pipeline --lake data/lake --checkpoints data/checkpoints [--show N] [--live-store]

One JVM instead of two keeps the demo inside the Docker memory budget (spec 17.21: one Spark container, 3 GB).
The two existing entry points (bronze_ingest, silver_clean) keep working unchanged; this module only starts the same
queries side by side. --show prints the Bronze progress ([bronze] lines) and the Silver batch views ([silver] lines)
exactly like the two jobs do. --live-store (or PG_DSN) makes the Silver query write the live and health tables.

The Silver query reads the Bronze Delta tables as a stream, and a Delta stream cannot start on a path that is not a
table yet, so missing Bronze tables are created empty first (same schema and partitioning as the Bronze writer).
"""
import argparse
import logging
import os
import threading

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, IntegerType, LongType, StringType, StructField, StructType,
                               TimestampType)

from streaming.jobs import bronze_ingest, silver_clean

log = logging.getLogger("pipeline")

BRONZE_SCHEMA = StructType([
    StructField("raw_value", StringType()), StructField("kafka_topic", StringType()),
    StructField("kafka_partition", IntegerType()), StructField("kafka_offset", LongType()),
    StructField("kafka_timestamp", TimestampType()), StructField("ingest_time", TimestampType()),
    StructField("ingest_date", DateType()),
])


def ensure_bronze_tables(spark, lake_root):
    """Create the Bronze Delta tables that do not exist yet (empty), so the Silver stream can start at once."""
    created = []
    for topic in bronze_ingest.BRONZE_TABLES:
        path = bronze_ingest.bronze_path(lake_root, topic)
        if not DeltaTable.isDeltaTable(spark, path):
            spark.createDataFrame([], BRONZE_SCHEMA).write.format("delta").partitionBy("ingest_date").save(path)
            created.append(bronze_ingest.BRONZE_TABLES[topic])
    return created


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="Bronze ingest and Silver clean in one process (stop with Ctrl+C)")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092"))
    ap.add_argument("--lake", default="data/lake")
    ap.add_argument("--checkpoints", default="data/checkpoints")
    ap.add_argument("--show", type=int, nargs="?", const=3, default=0, metavar="N",
                    help="print Bronze progress lines and the Silver batch views with the newest N rows (default 3)")
    ap.add_argument("--live-store", action="store_true",
                    help="write live_metrics_minute and the health tables to Postgres (also on when PG_DSN is set)")
    return ap.parse_args(argv)


def run(bootstrap, lake_root="data/lake", checkpoint_root="data/checkpoints", show=0, live_store=False):
    """Start the four queries; returns (bronze queries, silver query)."""
    from streaming.common.spark import get_spark

    lake_root, checkpoint_root = os.path.abspath(lake_root), os.path.abspath(checkpoint_root)
    spark = get_spark("pipeline", with_kafka=True)
    made = ensure_bronze_tables(spark, lake_root)
    if made:
        log.info("created empty Bronze tables: %s", ", ".join(made))
    bronze = bronze_ingest.run(bootstrap, lake_root, checkpoint_root)
    silver = silver_clean.run(lake_root, checkpoint_root, show=show, live_store=live_store)
    return bronze, silver


if __name__ == "__main__":
    args = parse_args()
    logging.basicConfig(level=logging.INFO)
    bronze_qs, silver_q = run(args.bootstrap, args.lake, args.checkpoints, args.show, args.live_store)
    session = SparkSession.getActiveSession()
    if args.show:
        threading.Thread(target=bronze_ingest.show_loop, args=(session, bronze_qs, args.lake, args.show),
                         daemon=True).start()
    bronze_ingest.await_all(bronze_qs + [silver_q], session)
