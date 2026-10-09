"""agg_campaign_daily as a pure Spark function over fact_orders, fact_attribution and the ad snapshots.

Grain: campaign_id, report_date (Cairo date), attribution_model (always 'last_touch' here). Same semantics as
warehouse/gold/10_agg_campaign_daily.sql:

* Ad measures come from the snapshot row with the LATEST pulled_at per (campaign_id, report_date); earlier pulls
  are never summed. Ties on pulled_at are broken by the greatest impressions, clicks, platform_conversions, spend,
  ad_platform (in that order); a NULL pulled_at loses. Rows with a NULL campaign_id or report_date are ignored.
  A day without an ad row has NULL ad measures.
* Order measures come from the orders whose last_touch campaign is not NULL and not 'unknown', counted on the
  Cairo date the order was PLACED (date_key), so delivered revenue sits on the order date. A day with ad data and
  no orders has 0 counts, 0.00 revenue and NULL order rates.
* Rates are a double division cast to Decimal(8,4), money ratios Decimal(12,2); NULL when the denominator is NULL
  or 0 (cpa, cost_per_delivered_order and roas also when spend is NULL or 0).
* open_rate: orders that are not delivered, cancelled or returned and were placed more than 14 days before
  `as_of`, divided by orders_placed.
"""
from datetime import datetime

from pyspark.sql import DataFrame, Window, functions as F

KEYS = ["campaign_id", "report_date", "attribution_model"]
AGG_CAMPAIGN_DAILY_COLUMNS = KEYS + [
    "ad_platform", "impressions", "clicks", "platform_conversions", "spend", "orders_placed", "orders_delivered",
    "orders_cancelled", "orders_returned", "orders_with_failed_attempt", "revenue_placed", "revenue_delivered",
    "cancel_rate", "failed_attempt_rate", "return_rate", "open_rate", "new_customer_orders", "aov", "ctr",
    "conversion_rate", "cpa", "cost_per_delivered_order", "roas_placed", "roas_delivered"]
OPEN_AFTER_DAYS = 14
RATE, MONEY = "decimal(8,4)", "decimal(12,2)"


def _flag(col):
    return F.coalesce(F.col(col), F.lit(False))


def _count_if(cond):
    return F.sum(F.when(cond, 1).otherwise(0)).cast("long")


def _ratio(num, den, typ, extra=None):
    """num / den as double cast to typ; NULL unless den > 0 (and extra, when given)."""
    cond = F.col(den) > 0
    if extra is not None:
        cond = cond & extra
    return F.when(cond, F.col(num).cast("double") / F.col(den).cast("double")).cast(typ)


def build_agg_campaign_daily(fact_orders: DataFrame, fact_attribution: DataFrame, campaign_snapshots: DataFrame,
                             as_of: datetime) -> DataFrame:
    """One row per campaign_id x report_date that has ad data or attributed orders (see module docstring)."""
    ranked = Window.partitionBy("campaign_id", "report_date").orderBy(
        F.col("pulled_at").desc_nulls_last(), F.col("impressions").desc_nulls_last(),
        F.col("clicks").desc_nulls_last(), F.col("platform_conversions").desc_nulls_last(),
        F.col("spend").desc_nulls_last(), F.col("ad_platform").desc_nulls_last())
    ad = (campaign_snapshots.where(F.col("campaign_id").isNotNull() & F.col("report_date").isNotNull())
          .withColumn("rn", F.row_number().over(ranked)).where("rn = 1")
          .select("campaign_id", "report_date", "ad_platform", "impressions", "clicks", "platform_conversions",
                  "spend"))

    attributed = fact_attribution.where(
        (F.col("model") == "last_touch") & F.col("campaign_id").isNotNull() & (F.col("campaign_id") != "unknown")
    ).select("order_id", "campaign_id")
    as_of_col = F.lit(as_of).cast("timestamp")
    is_open = (~_flag("is_delivered") & ~_flag("is_cancelled") & ~_flag("is_returned")
               & (F.col("order_created_at") < as_of_col - F.expr("INTERVAL %d DAYS" % OPEN_AFTER_DAYS)))
    od = (fact_orders.drop("campaign_id").where(F.col("date_key").isNotNull()).join(attributed, "order_id")
          .withColumn("report_date", F.to_date(F.col("date_key").cast("string"), "yyyyMMdd"))
          .groupBy("campaign_id", "report_date").agg(
              F.count(F.lit(1)).cast("long").alias("orders_placed"),
              _count_if(_flag("is_delivered")).alias("orders_delivered"),
              _count_if(_flag("is_cancelled")).alias("orders_cancelled"),
              _count_if(_flag("is_returned")).alias("orders_returned"),
              _count_if(_flag("has_failed_attempt")).alias("orders_with_failed_attempt"),
              F.coalesce(F.sum("order_total"), F.lit(0)).cast(MONEY).alias("revenue_placed"),
              F.coalesce(F.sum(F.when(_flag("is_delivered"), F.col("order_total"))), F.lit(0)).cast(MONEY)
              .alias("revenue_delivered"),
              _count_if(_flag("is_first_order")).alias("new_customer_orders"),
              _count_if(is_open).alias("orders_open")))

    j = ad.alias("ad").join(od.alias("od"), ["campaign_id", "report_date"], "full_outer")
    for c, typ in (("orders_placed", "long"), ("orders_delivered", "long"), ("orders_cancelled", "long"),
                   ("orders_returned", "long"), ("orders_with_failed_attempt", "long"),
                   ("new_customer_orders", "long"), ("orders_open", "long"),
                   ("revenue_placed", MONEY), ("revenue_delivered", MONEY)):
        j = j.withColumn(c, F.coalesce(F.col(c), F.lit(0).cast(typ)))
    spend_ok = F.col("spend") > 0
    out = (j.withColumn("attribution_model", F.lit("last_touch"))
           .withColumn("cancel_rate", _ratio("orders_cancelled", "orders_placed", RATE))
           .withColumn("failed_attempt_rate", _ratio("orders_with_failed_attempt", "orders_placed", RATE))
           .withColumn("return_rate", _ratio("orders_returned", "orders_placed", RATE))
           .withColumn("open_rate", _ratio("orders_open", "orders_placed", RATE))
           .withColumn("aov", _ratio("revenue_placed", "orders_placed", MONEY))
           .withColumn("ctr", _ratio("clicks", "impressions", RATE))
           .withColumn("conversion_rate", _ratio("orders_placed", "clicks", RATE))
           .withColumn("cpa", _ratio("spend", "orders_placed", MONEY, spend_ok))
           .withColumn("cost_per_delivered_order", _ratio("spend", "orders_delivered", MONEY, spend_ok))
           .withColumn("roas_placed", F.when(spend_ok, F.col("revenue_placed").cast("double")
                                             / F.col("spend").cast("double")).cast(MONEY))
           .withColumn("roas_delivered", F.when(spend_ok, F.col("revenue_delivered").cast("double")
                                                / F.col("spend").cast("double")).cast(MONEY)))
    return out.select(*AGG_CAMPAIGN_DAILY_COLUMNS)
