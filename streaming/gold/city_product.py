"""agg_city_product_daily as a pure Spark function over Silver events and fact_order_lines.

Grain: city, product_id, date_key (Cairo day as yyyyMMdd int). City grain on purpose (no district). Same semantics as
warehouse/gold/11_agg_city_product_daily.sql:

* sessions_viewed = DISTINCT sessions with at least one product_view event for the product in the city on the
  Silver event_date; add_to_cart = DISTINCT sessions with at least one add_to_cart event for the product (distinct
  sessions, not event counts). A session that changed city during a day is counted in each city it has events in.
* Bot rows (is_bot) and rows without session_id are excluded, late rows are INCLUDED (spec 16.1), events without
  product_id are ignored.
* units = sum(quantity) and revenue = sum(line_total) of ALL placed order lines (any order status, cancelled orders
  included, the basis of revenue_placed), by the line's city, product_id and date_key. Lines without product_id are
  ignored.
* A NULL or empty city becomes 'unknown'. The row set is the union of the event keys and the order-line keys; the
  missing side is 0 (0.00 for revenue), never NULL.
"""
from pyspark.sql import DataFrame, functions as F

KEYS = ["city", "product_id", "date_key"]
AGG_CITY_PRODUCT_DAILY_COLUMNS = KEYS + ["sessions_viewed", "add_to_cart", "units", "revenue"]


def _city(col="city"):
    c = F.col(col)
    return F.when(c.isNull() | (F.trim(c) == ""), F.lit("unknown")).otherwise(c)


def build_agg_city_product_daily(silver_events: DataFrame, fact_order_lines: DataFrame) -> DataFrame:
    """One row per city x product x Cairo day that has product views, carts or order lines (see module docstring)."""
    ev = silver_events.where(F.col("session_id").isNotNull() & ~F.coalesce(F.col("is_bot"), F.lit(False))
                             & F.col("product_id").isNotNull()
                             & F.col("event_type").isin("product_view", "add_to_cart"))
    evg = (ev.withColumn("city", _city())
           .withColumn("date_key", F.date_format("event_date", "yyyyMMdd").cast("int"))
           .groupBy(*KEYS).agg(
               F.countDistinct(F.when(F.col("event_type") == "product_view", F.col("session_id")))
               .cast("long").alias("sessions_viewed"),
               F.countDistinct(F.when(F.col("event_type") == "add_to_cart", F.col("session_id")))
               .cast("long").alias("add_to_cart")))
    olg = (fact_order_lines.where(F.col("product_id").isNotNull() & F.col("date_key").isNotNull())
           .withColumn("city", _city())
           .groupBy(*KEYS).agg(
               F.coalesce(F.sum("quantity"), F.lit(0)).cast("long").alias("units"),
               F.coalesce(F.sum("line_total"), F.lit(0)).cast("decimal(12,2)").alias("revenue")))
    j = evg.join(olg, KEYS, "full_outer")
    for c, typ in (("sessions_viewed", "long"), ("add_to_cart", "long"), ("units", "long"),
                   ("revenue", "decimal(12,2)")):
        j = j.withColumn(c, F.coalesce(F.col(c), F.lit(0).cast(typ)))
    return j.select(*AGG_CITY_PRODUCT_DAILY_COLUMNS)
