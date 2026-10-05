"""agg_orders_daily as a pure Spark function over fact_orders (spec 17.13).

Grain: date_key (Cairo order date), channel, payment_method, platform. Everything is derived from the
boolean columns of fact_orders, so the summary is recomputed from the fact and never incremented.
Delivered revenue is counted on the ORDER date (cohort view, spec 17.12).
"""
from pyspark.sql import DataFrame, functions as F

KEYS = ["date_key", "channel", "payment_method", "platform"]
AGG_ORDERS_DAILY_COLUMNS = KEYS + [
    "orders_placed", "orders_delivered", "orders_cancelled", "orders_returned", "orders_with_failed_attempt",
    "revenue_placed", "revenue_delivered", "new_customer_orders"]


def _flag(col):
    return F.sum(F.when(F.coalesce(F.col(col), F.lit(False)), 1).otherwise(0)).cast("long")


def build_agg_orders_daily(fact_orders: DataFrame) -> DataFrame:
    """One row per date_key x channel x payment_method x platform.

    orders_placed counts every order of the group; delivered, cancelled, returned, with-failed-attempt and
    new-customer orders count the is_delivered, is_cancelled, is_returned, has_failed_attempt and
    is_first_order flags. revenue_placed sums order_total of all orders, revenue_delivered of the delivered
    ones (Decimal(12,2), 0.00 when there are none). A NULL channel or platform becomes 'unknown'.
    """
    f = fact_orders
    for c in ("channel", "payment_method", "platform"):
        f = f.withColumn(c, F.coalesce(F.col(c), F.lit("unknown")))
    money = "decimal(12,2)"
    g = f.groupBy(*KEYS).agg(
        F.count(F.lit(1)).cast("long").alias("orders_placed"),
        _flag("is_delivered").alias("orders_delivered"),
        _flag("is_cancelled").alias("orders_cancelled"),
        _flag("is_returned").alias("orders_returned"),
        _flag("has_failed_attempt").alias("orders_with_failed_attempt"),
        F.coalesce(F.sum("order_total"), F.lit(0)).cast(money).alias("revenue_placed"),
        F.coalesce(F.sum(F.when(F.coalesce(F.col("is_delivered"), F.lit(False)), F.col("order_total"))),
                   F.lit(0)).cast(money).alias("revenue_delivered"),
        _flag("is_first_order").alias("new_customer_orders"))
    return g.select(*AGG_ORDERS_DAILY_COLUMNS)
