"""fact_orders and fact_order_lines as pure Spark functions over Silver DataFrames.

Natural ids (customer_id, product_id) stay beside the surrogate keys. The keys are looked up in optional
dimension DataFrames (the version valid at order time) and are -1 (the unknown member, spec 17.2) when there
is no dimension, no natural id or no matching version. campaign_id is always NULL in this module (the
attribution build adds it).
Dates: date_key is the Africa/Cairo local date of the order time as an int yyyymmdd (spec 16.6).
"""
from pyspark.sql import DataFrame, Window, functions as F

from streaming.common.status import STATUS_RANK

FACT_ORDERS_COLUMNS = [
    "order_id", "customer_key", "customer_id", "session_id", "channel", "platform", "utm_source", "utm_campaign",
    "click_id", "campaign_id", "payment_method", "carrier", "city", "country", "order_total", "current_status",
    "status_rank", "order_created_at", "delivered_at", "cancelled_at", "returned_at", "delivery_lead_hours",
    "delivery_failed_count", "has_failed_attempt", "first_attempt_delivered", "is_cancelled", "is_delivered",
    "is_returned", "order_seq", "is_first_order", "date_key",
]
FACT_ORDER_LINES_COLUMNS = [
    "order_id", "line_no", "customer_key", "customer_id", "product_key", "product_id", "date_key", "city",
    "quantity", "line_total",
]
UNKNOWN_KEY = -1


def cairo_date_key(col):
    """int yyyymmdd of the Cairo-local date of a UTC timestamp column."""
    return F.date_format(F.from_utc_timestamp(col, "Africa/Cairo"), "yyyyMMdd").cast("int")


def _rank_expr():
    pairs = []
    for k, v in STATUS_RANK.items():
        pairs += [F.lit(k), F.lit(v)]
    return F.create_map(*pairs)[F.col("status")]


def _version_key(df, dim, natural, key, at_col):
    """Add `key` to df: the surrogate key of the dimension version valid at df[at_col]
    (valid_from <= t < valid_to), UNKNOWN_KEY when there is no dim or no matching version."""
    if dim is None:
        return df.withColumn(key, F.lit(UNKNOWN_KEY).cast("long"))
    d = dim.select(F.col(natural).alias("_nat"), F.col(key).alias("_key"), "valid_from", "valid_to")
    cond = (df[natural] == d["_nat"]) & (df[at_col] >= d["valid_from"]) & (df[at_col] < d["valid_to"])
    # defensive de-duplication: overlapping versions or several current rows must never duplicate a row;
    # keep the version with the latest valid_from per input row
    df = df.withColumn("_rid", F.monotonically_increasing_id())
    w = Window.partitionBy("_rid").orderBy(F.col("valid_from").desc_nulls_last(), F.col("_key").desc_nulls_last())
    j = (df.join(d, cond, "left").withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
         .withColumn(key, F.coalesce(F.col("_key"), F.lit(UNKNOWN_KEY)).cast("long")))
    return j.drop("_nat", "_key", "valid_from", "valid_to", "_rid", "_rn")


def build_fact_orders(silver_orders: DataFrame, silver_order_status: DataFrame, dim_customer=None) -> DataFrame:
    """One row per order that has at least one status row (current state). Spec 17.5, 17.6, 17.8.

    current_status (same rule as streaming.common.status.resolve_current_status_timed): an order_cancelled
    row counts only if no order_shipped row has status_time EARLIER than the cancel's; a counted cancel is
    terminal and wins; otherwise the highest status rank wins, and a rank tie (out_for_delivery vs
    delivery_failed at 5, order_completed vs order_returned at 7) goes to the latest status_time. Arrival
    order never matters. is_cancelled = a counted cancel exists and the order was not delivered;
    is_returned = an order_returned row exists (returned_at is its earliest time); has_failed_attempt =
    delivery_failed_count > 0. platform comes from silver_orders. delivery_lead_hours keeps negative values
    (out_of_order noise, see report_negative_lead_time) and is Decimal(8,2): exact hours from microsecond
    times, rounded HALF UP to 2 decimals before the cast; NULL when not delivered.

    customer_key (surrogate key): the dim_customer version valid at order_created_at, else -1; the natural
    customer_id stays so a later build repairs a -1.
    """
    # an order without order_created_at has no date_key and no order_seq position: it is left out and counted
    # by report_orders_without_created_at
    silver_orders = silver_orders.where(F.col("order_created_at").isNotNull())
    s = silver_order_status.select("order_id", "status", "status_time", "carrier").withColumn("rank", _rank_expr())
    first_ship = F.min(F.when(F.col("status") == "order_shipped", F.col("status_time"))).over(
        Window.partitionBy("order_id"))
    s = s.withColumn("counted", (F.col("status") == "order_cancelled")
                     & ~F.coalesce(first_ship < F.col("status_time"), F.lit(False)))
    s = s.withColumn("usable", (F.col("status") != "order_cancelled") | F.col("counted"))
    # a counted cancel is terminal (above every other status); otherwise rank, then latest time
    prio = F.when(F.col("counted"), F.lit(99)).otherwise(F.col("rank"))
    w = Window.partitionBy("order_id").orderBy(prio.desc(), F.col("status_time").desc(), F.col("status"))
    cur = (s.where("usable").withColumn("rn", F.row_number().over(w)).where("rn = 1")
           .select("order_id", F.col("status").alias("current_status"), F.col("rank").alias("status_rank")))
    agg = s.groupBy("order_id").agg(
        F.max(F.when(F.col("status") == "order_shipped", F.col("carrier"))).alias("carrier"),
        F.min(F.when(F.col("status") == "delivered", F.col("status_time"))).alias("delivered_at"),
        F.min(F.when(F.col("counted"), F.col("status_time"))).alias("counted_cancel_at"),
        F.min(F.when(F.col("status") == "order_returned", F.col("status_time"))).alias("returned_at"),
        F.sum(F.when(F.col("status") == "delivery_failed", 1).otherwise(0)).cast("int").alias("delivery_failed_count"),
        F.min(F.when(F.col("status") == "out_for_delivery", F.col("status_time"))).alias("first_ofd_at"))
    o = silver_orders.join(agg, "order_id", "inner").join(cur, "order_id", "inner")
    o = (o.withColumn("is_delivered", F.col("delivered_at").isNotNull())
         .withColumn("is_cancelled", F.col("counted_cancel_at").isNotNull() & ~F.col("is_delivered"))
         .withColumn("cancelled_at", F.col("counted_cancel_at"))
         .withColumn("is_returned", F.col("returned_at").isNotNull())
         .withColumn("has_failed_attempt", F.col("delivery_failed_count") > 0)
         .withColumn("first_attempt_delivered",
                     F.coalesce(F.col("is_delivered") & (F.col("delivery_failed_count") == 0)
                                & (F.col("first_ofd_at") < F.col("delivered_at")), F.lit(False)))
         .withColumn("delivery_lead_hours",
                     F.round((F.unix_micros("delivered_at") - F.unix_micros("order_created_at"))
                             .cast("decimal(24,0)") / F.lit(3_600_000_000).cast("decimal(24,0)"), 2)
                     .cast("decimal(8,2)"))
         .withColumn("campaign_id", F.lit(None).cast("string")))
    o = _version_key(o, dim_customer, "customer_id", "customer_key", "order_created_at")
    seq = Window.partitionBy("customer_id").orderBy("order_created_at", "order_id")
    o = (o.withColumn("order_seq", F.row_number().over(seq).cast("int"))
         .withColumn("is_first_order", F.col("order_seq") == 1)
         .withColumn("date_key", cairo_date_key(F.col("order_created_at"))))
    return o.select(*FACT_ORDERS_COLUMNS)


def build_fact_order_lines(silver_orders: DataFrame, silver_order_items: DataFrame, dim_customer=None,
                           dim_product=None) -> DataFrame:
    """One row per order line; items whose order is missing from silver_orders are dropped.

    customer_key and product_key (surrogate keys) are the dimension versions valid at order_created_at, else -1 (unknown
    member); the natural customer_id and product_id stay beside them. date_key and city come from the order
    (Cairo-local date).
    """
    o = silver_orders.select("order_id", "customer_id", "city", "order_created_at")
    j = silver_order_items.join(o, "order_id", "inner")
    j = _version_key(j, dim_customer, "customer_id", "customer_key", "order_created_at")
    j = _version_key(j, dim_product, "product_id", "product_key", "order_created_at")
    return j.select("order_id", "line_no", "customer_key", "customer_id", "product_key", "product_id",
                    cairo_date_key(F.col("order_created_at")).alias("date_key"), "city", "quantity", "line_total")


def report_negative_lead_time(fact_orders: DataFrame) -> int:
    """Number of delivered orders whose delivery_lead_hours is negative (out_of_order status noise)."""
    return int(fact_orders.where(F.col("delivery_lead_hours") < 0).count())


class CancelCounts(int):
    """int (orders with a counted cancel and a later shipment) with the extra attribute
    delivered_after_cancel (orders with a counted cancel and a later delivered row)."""

    delivered_after_cancel: int = 0


def report_orders_without_created_at(silver_orders: DataFrame, silver_order_status: DataFrame) -> int:
    """Orders with status rows but a NULL order_created_at; build_fact_orders leaves them out."""
    ids = silver_order_status.select("order_id").distinct()
    return int(silver_orders.where(F.col("order_created_at").isNull()).select("order_id").distinct()
               .join(ids, "order_id", "inner").count())


def report_cancel_then_shipped(silver_order_status: DataFrame) -> int:
    """Orders with a counted cancel (no earlier shipment) AND a later order_shipped row (spec 17.6 check).

    Such an order resolves to order_cancelled (a counted cancel is terminal); the count is reported so the
    cancel-then-ship pattern is visible instead of silently absorbed. The result is an int (the
    cancel-then-shipped count); its attribute delivered_after_cancel counts the orders with a counted cancel
    and a later delivered row (current_status order_cancelled but is_delivered True).
    """
    s = silver_order_status.select("order_id", "status", "status_time")
    ships = s.where("status = 'order_shipped'").groupBy("order_id").agg(
        F.min("status_time").alias("first_ship"), F.max("status_time").alias("last_ship"))
    bad = (s.where("status = 'order_cancelled'").join(ships, "order_id", "inner")
           .where((F.col("status_time") <= F.col("first_ship")) & (F.col("last_ship") > F.col("status_time"))))
    out = CancelCounts(bad.select("order_id").distinct().count())
    first_ship = ships.select("order_id", "first_ship")
    counted = (s.where("status = 'order_cancelled'").join(first_ship, "order_id", "left")
               .where(F.col("first_ship").isNull() | (F.col("first_ship") >= F.col("status_time"))))
    deliv = s.where("status = 'delivered'").select("order_id", F.col("status_time").alias("dt"))
    out.delivered_after_cancel = int(counted.join(deliv, "order_id", "inner")
                                     .where(F.col("dt") > F.col("status_time")).select("order_id").distinct().count())
    return out
