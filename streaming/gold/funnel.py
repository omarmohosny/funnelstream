"""fact_funnel_daily as a pure Spark function over Silver DataFrames.

Natural ids are used. Task 10c adds the surrogate keys and the attribution campaign_id where the Gold
tables need them (this table has none).
"""
from pyspark.sql import DataFrame, Window, functions as F


def _rate(denominator):
    raw = 1 - F.col("orders") / F.col(denominator)
    return F.when(F.col(denominator) > 0, F.greatest(F.lit(0.0), F.least(F.lit(1.0), raw))).cast("double")


def build_fact_funnel_daily(silver_events: DataFrame, silver_orders: DataFrame) -> DataFrame:
    """Day x country x channel x payment_method funnel counts (distinct sessions) and abandon rates.

    Bot rows (is_bot) and rows without session_id are excluded; late rows are INCLUDED (spec 16.1).
    A session is collapsed to one row per event_date (country and channel of its earliest event) so the
    distinct counts add up. payment_method is the method of the session's silver_orders row (first by
    order_created_at), else "unknown". Abandon rates are 1 - orders / denominator, clamped to [0, 1],
    NULL when the denominator is 0. event_date is the Silver event_date. Surrogate keys are not used.
    """
    ev = silver_events.where(F.col("session_id").isNotNull() & ~F.coalesce(F.col("is_bot"), F.lit(False)))
    sess = ev.groupBy("event_date", "session_id").agg(
        F.min_by("country", "event_time").alias("country"),
        F.min_by("channel", "event_time").alias("channel"),
        F.max((F.col("event_type") == "product_view").cast("int")).alias("pv"),
        F.max((F.col("event_type") == "add_to_cart").cast("int")).alias("cart"),
        F.max((F.col("event_type") == "checkout_started").cast("int")).alias("checkout"),
        F.max((F.col("event_type") == "purchase").cast("int")).alias("purchase"))
    w = Window.partitionBy("session_id").orderBy("order_created_at", "order_id")
    pay = (silver_orders.where(F.col("session_id").isNotNull())
           .withColumn("rn", F.row_number().over(w)).where("rn = 1")
           .select("session_id", F.col("payment_method").alias("_pay")))
    sess = (sess.join(pay, "session_id", "left")
            .withColumn("payment_method", F.coalesce(F.col("_pay"), F.lit("unknown"))))
    g = sess.groupBy("event_date", "country", "channel", "payment_method").agg(
        F.count(F.lit(1)).alias("sessions"), F.sum("pv").alias("product_views"),
        F.sum("cart").alias("cart_sessions"), F.sum("checkout").alias("checkout_sessions"),
        F.sum("purchase").alias("orders"))
    for c in ("sessions", "product_views", "cart_sessions", "checkout_sessions", "orders"):
        g = g.withColumn(c, F.col(c).cast("long"))
    return (g.withColumn("cart_abandon_rate", _rate("cart_sessions"))
            .withColumn("checkout_abandon_rate", _rate("checkout_sessions")))
