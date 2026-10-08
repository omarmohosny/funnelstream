"""Bronze ingest: raw Kafka values into append-only Delta tables, one query per topic.

Notes: invalid UTF-8 bytes in a Kafka value become U+FFFD when cast to string
(the simulator emits UTF-8 only). A null value is kept as a row with a null
raw_value. Nothing is dropped or parsed at Bronze.
"""
import os
import threading
import time

from pyspark.sql import DataFrame, functions as F

BRONZE_TABLES = {
    "app.events": "bronze_app_events",
    "orders": "bronze_orders",
    "delivery.events": "bronze_delivery_events",
}


def bronze_path(lake_root: str, topic: str) -> str:
    return os.path.join(os.path.abspath(lake_root), "bronze", BRONZE_TABLES[topic])


def checkpoint_path(checkpoint_root: str, topic: str) -> str:
    return os.path.join(os.path.abspath(checkpoint_root), BRONZE_TABLES[topic])


def bronze_frame(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingest_time"),
    ).withColumn("ingest_date", F.to_date("ingest_time"))


def kafka_options(bootstrap: str, topic: str) -> dict:
    return {
        "kafka.bootstrap.servers": bootstrap,
        "subscribe": topic,
        "startingOffsets": "earliest",
        # After retention deletes offsets or a topic is recreated, the demo
        # must restart and carry on instead of failing the query.
        "failOnDataLoss": "false",
    }


def format_progress(topic, progress) -> str:
    """One status line from a StreamingQuery.lastProgress dict (None before the first batch)."""
    if not progress:
        return "[bronze] %-15s waiting for the first batch" % topic
    src = (progress.get("sources") or [{}])[0]
    end = src.get("endOffset") or {}
    offsets = ""
    if isinstance(end, dict) and end:
        offsets = " end offsets " + ",".join("%s:%s" % (p, o) for p, o in sorted(next(iter(end.values())).items()))
    return "[bronze] %-15s batch %-4s +%d rows (%d/s)%s" % (
        topic, progress.get("batchId"), progress.get("numInputRows", 0),
        round(progress.get("inputRowsPerSecond") or 0), offsets)


def _say(text):
    print(text, flush=True)  # flush: the line must appear at once even when stdout is a pipe or a file


def show_loop(spark, queries, lake_root, sample=3, every=5.0, stop=None, out=_say):
    """Print a progress line per query and the newest `sample` raw messages whenever a new batch landed."""
    stop = stop or threading.Event()
    last = {}
    by_table = {BRONZE_TABLES[t]: t for t in BRONZE_TABLES}
    while not stop.wait(every):
        for q in queries:
            topic = by_table.get(q.name, q.name)
            p = q.lastProgress
            if not p or p.get("batchId") == last.get(topic):
                continue
            first = topic not in last
            last[topic] = p.get("batchId")
            if not p.get("numInputRows") and not first:
                continue  # idle trigger: nothing arrived, stay quiet
            out(format_progress(topic, p))
            if sample and p.get("numInputRows"):
                try:
                    rows = (spark.read.format("delta").load(bronze_path(lake_root, topic))
                            .orderBy(F.col("kafka_offset").desc()).limit(sample).collect())
                    for r in rows:
                        out("          p%s@%s  %s" % (r["kafka_partition"], r["kafka_offset"], (r["raw_value"] or "null")[:150]))
                except Exception as e:  # the table may be mid-write; the next tick retries
                    out("          (sample unavailable: %s)" % type(e).__name__)


def await_all(queries, spark=None):
    """Block until any query ends; raise its error. Always stop all queries."""
    try:
        if spark is None:
            raise RuntimeError("no active SparkSession")
        spark.streams.awaitAnyTermination()
    finally:
        for q in queries:
            try:
                q.stop()
            except Exception:
                pass


def run(bootstrap: str, lake_root: str = "data/lake",
        checkpoint_root: str = "data/checkpoints", topics=None):
    from streaming.common.spark import get_spark

    lake_root, checkpoint_root = os.path.abspath(lake_root), os.path.abspath(checkpoint_root)
    spark = get_spark("bronze_ingest", with_kafka=True)
    queries = []
    for topic in topics or list(BRONZE_TABLES):
        raw = (spark.readStream.format("kafka")
               .options(**kafka_options(bootstrap, topic))
               .load())
        queries.append(
            bronze_frame(raw).writeStream.format("delta")
            .option("checkpointLocation", checkpoint_path(checkpoint_root, topic))
            .partitionBy("ingest_date")
            .trigger(processingTime="10 seconds")
            .queryName(BRONZE_TABLES[topic])
            .start(bronze_path(lake_root, topic)))
    return queries


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    import argparse

    ap = argparse.ArgumentParser(description="Continuous Bronze ingest from Kafka (stop with Ctrl+C or timeout)")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092"))
    ap.add_argument("--lake", default="data/lake")
    ap.add_argument("--checkpoints", default="data/checkpoints")
    ap.add_argument("--show", type=int, nargs="?", const=3, default=0, metavar="N",
                    help="print a progress line per batch and the newest N raw messages (default 3)")
    args = ap.parse_args()
    qs = run(args.bootstrap, args.lake, args.checkpoints)
    if args.show:
        threading.Thread(target=show_loop, args=(SparkSession.getActiveSession(), qs, args.lake, args.show),
                         daemon=True).start()
    await_all(qs, SparkSession.getActiveSession())
