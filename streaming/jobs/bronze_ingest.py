"""Bronze ingest: raw Kafka values into append-only Delta tables, one query per topic.

Notes: invalid UTF-8 bytes in a Kafka value become U+FFFD when cast to string
(the simulator emits UTF-8 only). A null value is kept as a row with a null
raw_value. Nothing is dropped or parsed at Bronze.
"""
import os

from pyspark.sql import DataFrame, functions as F

BRONZE_TABLES = {
    "app.events": "bronze_app_events",
    "orders": "bronze_orders",
    "delivery.events": "bronze_delivery_events",
}


def bronze_path(lake_root: str, topic: str) -> str:
    return os.path.join(lake_root, "bronze", BRONZE_TABLES[topic])


def checkpoint_path(checkpoint_root: str, topic: str) -> str:
    return os.path.join(checkpoint_root, BRONZE_TABLES[topic])


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


def await_all(queries, spark=None):
    """Block until any query ends; raise its error. Always stop all queries."""
    try:
        if spark is not None:
            spark.streams.awaitAnyTermination()
        else:
            for q in queries:
                q.awaitTermination()
    finally:
        for q in queries:
            try:
                q.stop()
            except Exception:
                pass


def run(bootstrap: str, lake_root: str = "data/lake",
        checkpoint_root: str = "data/checkpoints", topics=None):
    from streaming.common.spark import get_spark

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

    qs = run(os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092"))
    await_all(qs, SparkSession.getActiveSession())
