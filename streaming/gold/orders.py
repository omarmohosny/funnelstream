"""fact_orders and fact_order_lines as pure Spark functions over Silver DataFrames.

Natural ids (customer_id, product_id) are used here. Task 10c adds the surrogate keys (customer_key,
product_key, campaign_key) and the attribution campaign_id (campaign_id is always NULL in this module).
Dates: date_key is the Africa/Cairo local date of the order time as an int yyyymmdd (spec 16.6).
"""
from pyspark.sql import DataFrame, Window, functions as F

from streaming.common.status import STATUS_RANK

FACT_ORDERS_COLUMNS = [
    "order_id", "customer_id", "session_id", "channel", "utm_source", "utm_campaign", "click_id", "campaign_id",
    "payment_method", "carrier", "city", "country", "order_total", "current_status", "status_rank",
    "order_created_at", "delivered_at", "cancelled_at", "delivery_lead_hours", "delivery_failed_count",
    "first_attempt_delivered", "is_cancelled", "is_delivered", "order_seq", "is_first_order", "date_key",
]


def cairo_date_key(col):
    """int yyyymmdd of the Cairo-local date of a UTC timestamp column."""
    return F.date_format(F.from_utc_timestamp(col, "Africa/Cairo"), "yyyyMMdd").cast("int")


def _rank_expr():
    pairs = []
    for k, v in STATUS_RANK.items():
        pairs += [F.lit(k), F.lit(v)]
    return F.create_map(*pairs)[F.col("status")]


def build_fact_orders(silver_orders: DataFrame, silver_order_status: DataFrame) -> DataFrame:
    """One row per order that has at least one status row (current state).

    current_status: highest status rank over the order's status rows, except that an order_cancelled row
    counts only if the order has no order_shipped row (same rule as resolve_current_status); a rank tie
    (out_for_delivery vs delivery_failed) goes to the latest status_time. Delivery fields come from the
    delivered / delivery_failed / out_for_delivery rows; delivery_lead_hours keeps negative values (they
    come from out_of_order noise, see report_negative_lead_time).

    Surrogate keys (customer_key) and the attribution campaign_id are added by Task 10c; campaign_id is
    NULL here and natural customer_id is used.
    """
    s = silver_order_status.select("order_id", "status", "status_time", "carrier").withColumn("rank", _rank_expr())
    shipped = F.max(F.when(F.col("status") == "order_shipped", 1).otherwise(0)).over(
        Window.partitionBy("order_id")) == 1
    s = s.withColumn("counted", ~((F.col("status") == "order_cancelled") & shipped))
    w = Window.partitionBy("order_id").orderBy(F.col("rank").desc(), F.col("status_time").desc(), F.col("status"))
    cur = (s.where("counted").withColumn("rn", F.row_number().over(w)).where("rn = 1")
           .select("order_id", F.col("status").alias("current_status"), F.col("rank").alias("status_rank")))
    agg = s.groupBy("order_id").agg(
        F.max(F.when(F.col("status") == "order_shipped", F.col("carrier"))).alias("carrier"),
        F.min(F.when(F.col("status") == "delivered", F.col("status_time"))).alias("delivered_at"),
        F.min(F.when((F.col("status") == "order_cancelled") & F.col("counted"), F.col("status_time")))
        .alias("cancelled_at"),
        F.sum(F.when(F.col("status") == "delivery_failed", 1).otherwise(0)).cast("int").alias("delivery_failed_count"),
        F.min(F.when(F.col("status") == "out_for_delivery", F.col("status_time"))).alias("first_ofd_at"))
    o = silver_orders.join(agg, "order_id", "inner").join(cur, "order_id", "inner")
    o = (o.withColumn("is_delivered", F.col("delivered_at").isNotNull())
         .withColumn("is_cancelled", F.col("cancelled_at").isNotNull() & ~F.col("is_delivered"))
         .withColumn("first_attempt_delivered",
                     F.coalesce(F.col("is_delivered") & (F.col("delivery_failed_count") == 0)
                                & (F.col("first_ofd_at") < F.col("delivered_at")), F.lit(False)))
         .withColumn("delivery_lead_hours",
                     F.round((F.col("delivered_at").cast("double") - F.col("order_created_at").cast("double"))
                             / 3600.0, 2))
         .withColumn("campaign_id", F.lit(None).cast("string")))
    seq = Window.partitionBy("customer_id").orderBy("order_created_at", "order_id")
    o = (o.withColumn("order_seq", F.row_number().over(seq).cast("int"))
         .withColumn("is_first_order", F.col("order_seq") == 1)
         .withColumn("date_key", cairo_date_key(F.col("order_created_at"))))
    return o.select(*FACT_ORDERS_COLUMNS)


def build_fact_order_lines(silver_orders: DataFrame, silver_order_items: DataFrame) -> DataFrame:
    """One row per order line; items whose order is missing from silver_orders are dropped.

    Natural ids (customer_id, product_id) are used; Task 10c adds the surrogate keys customer_key and
    product_key. date_key and city come from the order (Cairo-local date).
    """
    o = silver_orders.select("order_id", "customer_id", "city", "order_created_at")
    j = silver_order_items.join(o, "order_id", "inner")
    return j.select("order_id", "line_no", "customer_id", "product_id",
                    cairo_date_key(F.col("order_created_at")).alias("date_key"), "city", "quantity", "line_total")


def report_negative_lead_time(fact_orders: DataFrame) -> int:
    """Number of delivered orders whose delivery_lead_hours is negative (out_of_order status noise)."""
    return int(fact_orders.where(F.col("delivery_lead_hours") < 0).count())
