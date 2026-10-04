"""Bot marking over the WHOLE Silver events table (spec 16.5), not per micro-batch."""
from pyspark.sql import DataFrame, Window, functions as F


def mark_bot_sessions(silver_events: DataFrame, max_events: int = 100, window_seconds: float = 10) -> DataFrame:
    """silver_events with is_bot recomputed: True on every row of a session that has more than
    max_events events inside some window of window_seconds, False elsewhere.

    Same rule as streaming.common.bots.find_bot_sessions (window [t - window_seconds, t] over event_time,
    per session_id). Events with a null session_id or null event_time are never bots. The column order
    of the input is kept.
    """
    cols = silver_events.columns
    micros = F.unix_micros(F.col("event_time"))
    w = (Window.partitionBy("session_id").orderBy(micros)
         .rangeBetween(-int(window_seconds * 1_000_000), 0))
    valid = silver_events.where(F.col("session_id").isNotNull() & F.col("event_time").isNotNull())
    flagged = (valid.withColumn("_n", F.count(F.lit(1)).over(w)).where(F.col("_n") > max_events)
               .select("session_id").distinct().withColumn("_is_bot_session", F.lit(True)))
    out = silver_events.join(flagged, "session_id", "left")
    return out.withColumn("is_bot", F.coalesce(F.col("_is_bot_session"), F.lit(False))).select(*cols)
