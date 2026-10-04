"""live_metrics_minute: LOCAL serving table (not Gold), recomputed from recent non-late, non-bot Silver events.

No surrogate keys are involved (Task 10c adds them to the Gold tables only).
"""
import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, functions as F

from streaming.gold.bots import mark_bot_sessions
from streaming.silver.writer import silver_path

DEFAULT_PATH = "data/lake/live/live_metrics_minute"


def live_path(lake_root: str = "data/lake") -> str:
    return os.path.join(lake_root, "live", "live_metrics_minute")


def build_live_metrics_minute(silver_events: DataFrame) -> DataFrame:
    """1-minute tumbling windows by country over event_time, excluding is_late and is_bot rows.

    Columns: minute_start, country, events, active_sessions (distinct session_id), product_views,
    add_to_cart, orders_placed (purchase events).
    """
    ev = silver_events.where(~F.coalesce(F.col("is_late"), F.lit(False))
                             & ~F.coalesce(F.col("is_bot"), F.lit(False)))
    et = F.col("event_type")
    return (ev.groupBy(F.window("event_time", "1 minute").alias("w"), "country")
            .agg(F.count(F.lit(1)).alias("events"),
                 F.countDistinct("session_id").alias("active_sessions"),
                 F.sum((et == "product_view").cast("int")).cast("long").alias("product_views"),
                 F.sum((et == "add_to_cart").cast("int")).cast("long").alias("add_to_cart"),
                 F.sum((et == "purchase").cast("int")).cast("long").alias("orders_placed"))
            .select(F.col("w.start").alias("minute_start"), "country", "events", "active_sessions",
                    "product_views", "add_to_cart", "orders_placed"))


def write_live_metrics(df: DataFrame, path: str = DEFAULT_PATH) -> str:
    """Full overwrite (no replaceWhere) of the Delta table at path with the recomputed window."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    return path


def refresh_live_metrics(spark, lake_root: str = "data/lake", minutes: int = 60, now=None) -> str:
    """Recompute the last `minutes` of Silver events (event_time >= now - minutes) and overwrite the table.

    Bots are marked over the selected rows with mark_bot_sessions first (Silver stores is_bot False).
    Returns the path written (<lake_root>/live/live_metrics_minute).
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is not None:
        now = now.astimezone(timezone.utc).replace(tzinfo=None)
    cutoff = now - timedelta(minutes=minutes)
    events = spark.read.format("delta").load(silver_path(lake_root, "silver_events"))
    recent = events.where(F.col("event_time") >= F.lit(cutoff).cast("timestamp"))
    return write_live_metrics(build_live_metrics_minute(mark_bot_sessions(recent)), live_path(lake_root))
