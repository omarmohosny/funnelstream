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
    return os.path.join(os.path.abspath(lake_root), "live", "live_metrics_minute")


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


def minute_epochs(minutes) -> list:
    """Sorted distinct minute starts as epoch seconds from datetimes (naive = UTC) or ints (any second of the minute)."""
    out = set()
    for m in minutes:
        if isinstance(m, datetime):
            m = int((m if m.tzinfo else m.replace(tzinfo=timezone.utc)).timestamp())
        out.add(int(m) - int(m) % 60)
    return sorted(out)


MAX_MINUTE_LIST = 2000  # above this many minutes the filter is a range, not an IN list


def live_metrics_for_minutes(events: DataFrame, minutes, bot_lookback_hours: float = 6,
                             country_default: str = "unknown") -> DataFrame:
    """build_live_metrics_minute restricted to the given minutes, with the same bot rule as refresh_live_metrics.

    Full totals for those minutes (never increments). Bot sessions are detected with mark_bot_sessions over the
    events from `bot_lookback_hours` before the first minute up to now. A null country becomes country_default
    (the Postgres key column is NOT NULL). LIMITATION as above: a burst older than the lookback is not detected.
    """
    epochs = minute_epochs(minutes)
    if not epochs:
        return build_live_metrics_minute(events.limit(0))
    lo, hi = epochs[0], epochs[-1] + 60
    sec = F.unix_timestamp("event_time")
    minute = sec - (sec % 60)
    small = len(epochs) <= MAX_MINUTE_LIST
    recent = events.where(minute.isin(epochs) if small
                          else (F.col("event_time") >= F.timestamp_seconds(F.lit(lo)))
                          & (F.col("event_time") < F.timestamp_seconds(F.lit(hi))))
    wide = events.where(F.col("event_time") >= F.timestamp_seconds(F.lit(lo - int(bot_lookback_hours * 3600))))
    bots = mark_bot_sessions(wide).where("is_bot").select("session_id").distinct()
    flagged = recent.join(F.broadcast(bots.withColumn("_bot", F.lit(True))), "session_id", "left")
    flagged = (flagged.withColumn("is_bot", F.coalesce(F.col("_bot"), F.lit(False)))
               .withColumn("country", F.coalesce(F.col("country"), F.lit(country_default)))
               .select(*recent.columns))
    out = build_live_metrics_minute(flagged)
    start = F.unix_timestamp("minute_start")
    return out.where(start.isin(epochs) if small else (start >= lo) & (start < hi))


def write_live_metrics(df: DataFrame, path: str = DEFAULT_PATH) -> str:
    """Full overwrite (no replaceWhere) of the Delta table at path with the recomputed window."""
    path = os.path.abspath(path)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    return path


def refresh_live_metrics(spark, lake_root: str = "data/lake", minutes: int = 60, now=None,
                         bot_lookback_hours: float = 6) -> str:
    """Recompute the last `minutes` of Silver events (event_time >= now - minutes) and overwrite the table.

    Bot sessions are detected with mark_bot_sessions over the wider last `bot_lookback_hours` of Silver
    (Silver stores is_bot False); every row of the window whose session is one of those ids is excluded.
    LIMITATION: a burst older than the lookback is not detected, so a session whose burst is older than
    bot_lookback_hours but which has events inside the window is counted as normal.
    Returns the path written (<lake_root>/live/live_metrics_minute).
    """
    lake_root = os.path.abspath(lake_root)
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is not None:
        now = now.astimezone(timezone.utc).replace(tzinfo=None)
    cutoff = now - timedelta(minutes=minutes)
    events = spark.read.format("delta").load(silver_path(lake_root, "silver_events"))
    recent = events.where(F.col("event_time") >= F.lit(cutoff).cast("timestamp"))
    lookback = now - timedelta(hours=bot_lookback_hours)
    wide = events.where(F.col("event_time") >= F.lit(min(lookback, cutoff)).cast("timestamp"))
    bots = mark_bot_sessions(wide).where("is_bot").select("session_id").distinct()
    flagged = recent.join(F.broadcast(bots.withColumn("_bot", F.lit(True))), "session_id", "left")
    flagged = flagged.withColumn("is_bot", F.coalesce(F.col("_bot"), F.lit(False))).select(*recent.columns)
    return write_live_metrics(build_live_metrics_minute(flagged), live_path(lake_root))


def main(argv=None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Recompute live_metrics_minute from Silver")
    ap.add_argument("--lake", default="data/lake")
    ap.add_argument("--minutes", type=int, default=60)
    ap.add_argument("--lookback-hours", type=float, default=6)
    args = ap.parse_args(argv)
    from streaming.common.spark import get_spark

    spark = get_spark("live_refresh")
    path = refresh_live_metrics(spark, args.lake, minutes=args.minutes, bot_lookback_hours=args.lookback_hours)
    rows = spark.read.format("delta").load(path).count()
    print("WROTE %s (%d minute rows)" % (path, rows))
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
