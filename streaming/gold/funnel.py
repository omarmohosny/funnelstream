"""agg_funnel_daily as a pure Spark function over Silver DataFrames (spec 17.9).

Grain: event_date, country, channel, platform. payment_method is NOT part of the funnel (it is not known
before checkout); COD versus prepaid lives in agg_orders_daily.
"""
from pyspark.sql import DataFrame, functions as F


def _rate(denominator):
    raw = 1 - F.col("orders") / F.col(denominator)
    return F.when(F.col(denominator) > 0, F.greatest(F.lit(0.0), F.least(F.lit(1.0), raw))).cast("double")


def build_agg_funnel_daily(silver_events: DataFrame, silver_orders: DataFrame = None) -> DataFrame:
    """Day x country x channel x platform funnel counts (distinct sessions) and abandon rates.

    Bot rows (is_bot) and rows without session_id are excluded; late rows are INCLUDED (spec 16.1).
    A session is collapsed to one row per event_date whose country, channel and platform come from the
    session's EARLIEST event of that date (ties by event_id), so the distinct counts add up and the platform
    split sums to the total sessions. A NULL platform (events from before the field existed) becomes
    'unknown'. `orders` = distinct sessions with a purchase event (silver_orders is not needed and is
    accepted only for call compatibility). Abandon rates are 1 - orders / denominator, clamped to [0, 1],
    NULL when the denominator is 0. event_date is the Silver event_date.
    """
    ev = silver_events.where(F.col("session_id").isNotNull() & ~F.coalesce(F.col("is_bot"), F.lit(False)))
    first = F.struct("event_time", "event_id")
    sess = ev.groupBy("event_date", "session_id").agg(
        F.min_by("country", first).alias("country"),
        F.min_by("channel", first).alias("channel"),
        F.min_by("platform", first).alias("platform"),
        F.max((F.col("event_type") == "product_view").cast("int")).alias("pv"),
        F.max((F.col("event_type") == "add_to_cart").cast("int")).alias("cart"),
        F.max((F.col("event_type") == "checkout_started").cast("int")).alias("checkout"),
        F.max((F.col("event_type") == "purchase").cast("int")).alias("purchase"))
    for c in ("country", "channel", "platform"):
        sess = sess.withColumn(c, F.coalesce(F.col(c), F.lit("unknown")))
    g = sess.groupBy("event_date", "country", "channel", "platform").agg(
        F.count(F.lit(1)).alias("sessions"), F.sum("pv").alias("product_views"),
        F.sum("cart").alias("cart_sessions"), F.sum("checkout").alias("checkout_sessions"),
        F.sum("purchase").alias("orders"))
    for c in ("sessions", "product_views", "cart_sessions", "checkout_sessions", "orders"):
        g = g.withColumn(c, F.col(c).cast("long"))
    return (g.withColumn("cart_abandon_rate", _rate("cart_sessions"))
            .withColumn("checkout_abandon_rate", _rate("checkout_sessions")))
