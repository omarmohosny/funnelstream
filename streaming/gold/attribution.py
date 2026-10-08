"""fact_attribution as pure Spark functions over Silver DataFrames (spec 17 rules 10 and 11, docs/metrics.md).

This is the REFERENCE implementation. The production logic is warehouse/gold/07_fact_attribution.sql; the
Spark parity test compares the two column by column.

Click (spec 4): in a session, the FIRST page_view or product_view event (by event_time, ties by event_id) that
carries a utm_campaign. Sessions that are bots (recomputed over the whole events table, same rule as the funnel)
have no click. The click is attributed to silver_events.campaign_id when it is populated, else to the
dim_campaign row with the same utm_campaign; when neither resolves the click has a NULL campaign_id and does
not count as a click for attribution. A click carries the customer_id of the click event itself (linking an
anonymous pre-login session to a customer after login is out of scope, spec 17.11).

Models (two rows per order, key order_id + model):
  1. the order's own session: the click whose click_id equals the order's click_id (no time condition);
  2. else, for an identified order (customer_id not NULL): the customer's clicks from any session in
     [order_created_at - window_days, order_created_at): last_touch takes the latest, first_touch the earliest;
  3. else direct: campaign_id NULL and click_id NULL.
Ties on click time: the smallest click_id wins (NULL click_id last), then the smallest session_id.
"""
from pyspark.sql import DataFrame, Window, functions as F

from streaming.gold.bots import mark_bot_sessions

FACT_ATTRIBUTION_COLUMNS = ["order_id", "model", "campaign_id", "click_id", "order_total"]
CLICK_COLUMNS = ["click_id", "session_id", "customer_id", "campaign_id", "click_time"]
MODELS = ("last_touch", "first_touch")
CLICK_EVENT_TYPES = ("page_view", "product_view")
UNKNOWN_CAMPAIGN_ID = "unknown"  # the seeded unknown member of dim_campaign never resolves a click


def build_clicks(silver_events: DataFrame, dim_campaign: DataFrame = None) -> DataFrame:
    """One row per non-bot session that has a click event: click_id, session_id, customer_id, campaign_id,
    click_time. campaign_id is NULL when the click's campaign could not be resolved (such a click is not used
    by build_fact_attribution; report_unresolved_clicks counts them). is_bot is recomputed here over the whole
    events table, the stored column is not trusted."""
    ev = mark_bot_sessions(silver_events)
    cand = ev.where(~F.col("is_bot") & F.col("session_id").isNotNull() & F.col("event_time").isNotNull()
                    & F.col("event_type").isin(*CLICK_EVENT_TYPES) & F.col("utm_campaign").isNotNull())
    w = Window.partitionBy("session_id").orderBy(F.col("event_time"), F.col("event_id"))
    first = cand.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
    if dim_campaign is not None:
        camp = (dim_campaign.where((F.col("campaign_id") != UNKNOWN_CAMPAIGN_ID) & F.col("utm_campaign").isNotNull())
                .groupBy("utm_campaign").agg(F.min("campaign_id").alias("_dim_campaign_id")))
        first = first.join(camp, "utm_campaign", "left")
    else:
        first = first.withColumn("_dim_campaign_id", F.lit(None).cast("string"))
    return first.select(
        "click_id", "session_id", "customer_id",
        F.coalesce(F.col("campaign_id"), F.col("_dim_campaign_id")).cast("string").alias("campaign_id"),
        F.col("event_time").alias("click_time"))


def report_unresolved_clicks(clicks: DataFrame) -> int:
    """Clicks (first UTM event of a non-bot session) whose campaign could not be resolved."""
    return int(clicks.where(F.col("campaign_id").isNull()).count())


def build_fact_attribution(silver_orders: DataFrame, clicks: DataFrame, window_days: int = 7) -> DataFrame:
    """Two rows per order (last_touch and first_touch). Orders without order_created_at are left out like
    fact_orders does. order_total is copied from the order."""
    o = (silver_orders.where(F.col("order_created_at").isNotNull())
         .select(F.col("order_id"), F.col("customer_id").alias("o_customer"), F.col("click_id").alias("o_click"),
                 F.col("order_created_at").alias("o_at"), F.col("order_total")))
    k = clicks.where(F.col("campaign_id").isNotNull())

    # step 1: the order's own session click; click_id is unique among clicks (smallest click_time, session_id)
    uw = Window.partitionBy("click_id").orderBy(F.col("click_time"), F.col("session_id"))
    uniq = (k.where(F.col("click_id").isNotNull()).withColumn("_rn", F.row_number().over(uw)).where("_rn = 1")
            .select(F.col("click_id").alias("u_click"), F.col("campaign_id").alias("u_campaign")))
    own = o.join(uniq, o["o_click"] == uniq["u_click"], "inner").select(
        "order_id", F.col("u_campaign").alias("own_campaign"), F.col("u_click").alias("own_click"))

    # step 2: the customer's clicks in [order_created_at - window_days, order_created_at), any session
    lo = F.expr("o_at - INTERVAL %d DAYS" % int(window_days))
    cond = ((F.col("o_customer") == k["customer_id"]) & (k["click_time"] >= lo) & (k["click_time"] < F.col("o_at")))
    tie = [F.col("click_id").asc_nulls_last(), F.col("session_id").asc()]
    j = o.where(F.col("o_customer").isNotNull()).join(k, cond, "inner")
    last_w = Window.partitionBy("order_id").orderBy(F.col("click_time").desc(), *tie)
    first_w = Window.partitionBy("order_id").orderBy(F.col("click_time").asc(), *tie)
    j = (j.withColumn("_last", F.row_number().over(last_w)).withColumn("_first", F.row_number().over(first_w))
         .select("order_id", F.col("campaign_id").alias("fb_campaign"), F.col("click_id").alias("fb_click"),
                 "_last", "_first"))
    base = o.join(own, "order_id", "left")
    out = None
    for model, flag in (("last_touch", "_last"), ("first_touch", "_first")):
        fb = j.where(F.col(flag) == 1).drop("_last", "_first")
        m = base.join(fb, "order_id", "left")
        has_own = F.col("own_click").isNotNull()
        m = m.select("order_id", F.lit(model).alias("model"),
                     F.when(has_own, F.col("own_campaign")).otherwise(F.col("fb_campaign")).alias("campaign_id"),
                     F.when(has_own, F.col("own_click")).otherwise(F.col("fb_click")).alias("click_id"),
                     "order_total")
        out = m if out is None else out.unionByName(m)
    return out.select(*FACT_ATTRIBUTION_COLUMNS)
