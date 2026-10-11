"""Daily inventory mart (inventory design, section 9, and the Power BI and Grafana inventory pages).

inventory_daily: one row per store, product and Cairo day, computed from the Silver stock tables.
oos_demand_daily: live_oos_demand_minute summed per Cairo day (plain SQL in Postgres): all served demand, and the part
flagged out of stock when the minute was written.
express_daily: express orders per store and Cairo day against their promise, computed from the Silver express tables.
"""
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from pyspark.sql import Window, functions as F

from warehouse.live_common import _read
from warehouse.live_express import CLOSED_STATUSES
from warehouse.live_stock import _stock_levels

CAIRO, UTC = ZoneInfo("Africa/Cairo"), ZoneInfo("UTC")
GRACE_SECONDS = 60  # the same one minute of grace as the live express panels


def cairo_day_bounds(report_date):
    """(start, end) of the Cairo calendar day as aware UTC datetimes; end is exclusive."""
    start = datetime.combine(report_date, time(0), tzinfo=CAIRO).astimezone(UTC)
    end = datetime.combine(report_date + timedelta(days=1), time(0), tzinfo=CAIRO).astimezone(UTC)
    return start, end


def compute_inventory_daily(spark, silver_root, report_date):
    """inventory_daily rows for the Cairo day: end-of-day stock plus the day's flows, for every store and product pair
    that has a snapshot before the end of the day. [] when the Silver stock tables do not exist.

    The day is cut on event_time with the Cairo bounds, not on the UTC event_date column (a Cairo day spans two UTC
    dates)."""
    start, end = cairo_day_bounds(report_date)
    levels = _stock_levels(spark, silver_root, as_of=end)
    if levels is None:
        return []
    keys = ["store_id", "product_id"]
    moves = _read(spark, silver_root, "silver_stock_movements")
    flows = None
    if moves is not None:
        day = moves.where((F.col("event_time") >= F.lit(start)) & (F.col("event_time") < F.lit(end)))

        def total(reason, value):
            return F.coalesce(F.sum(F.when(F.col("reason") == reason, value)), F.lit(0))

        flows = day.groupBy(*keys).agg(
            total("sold", -F.col("qty_delta")).alias("units_sold"),
            total("received", F.col("qty_delta")).alias("units_received"),
            total("shrinkage", -F.col("qty_delta")).alias("units_shrunk"),
            total("adjustment", F.col("qty_delta")).alias("units_adjusted"),
            total("sold", F.col("requested_qty") + F.col("qty_delta")).alias("oversold_units"))
    joined = levels.join(flows, keys, "left") if flows is not None else levels
    rows = []
    for r in joined.collect():
        on_hand, reserved = int(r["on_hand"]), int(r["reserved_qty"] or 0)

        def flow(name):
            return int(r[name] or 0) if flows is not None else 0

        rows.append(dict(report_date=report_date, store_id=r["store_id"], product_id=r["product_id"],
                         closing_on_hand=max(on_hand, 0), units_sold=flow("units_sold"),
                         units_received=flow("units_received"), units_shrunk=flow("units_shrunk"),
                         units_adjusted=flow("units_adjusted"), oversold_units=flow("oversold_units"),
                         ended_out_of_stock=(on_hand - reserved) <= 0))
    return sorted(rows, key=lambda x: (x["store_id"], x["product_id"]))


def _minutes(value):
    return None if value is None else Decimal(str(round(float(value), 2)))


def compute_express_daily(spark, silver_root, report_date, now=None):
    """express_daily rows for the Cairo day of `silver_express_orders.created_at`, one per store ([] for a quiet day).

    orders: express orders created that day. delivered: those with a `delivered` status (the earliest counts, so a
    replayed status row changes nothing). on_time: delivered with delivered_at - created_at <= promised_minutes.
    breached: delivered late, plus undelivered orders whose latest status is not cancelled or returned and whose age at
    `now` (the build time, default the current UTC time) is more than promised_minutes plus one minute. An undelivered
    cancelled or returned order is only in `orders`. avg/max_delivery_minutes: over the delivered orders, None when
    there are none. An order still open at midnight is not breached yet when the day is built just after midnight; the
    build script rebuilds the last two days so the next run corrects it."""
    now = now or datetime.now(timezone.utc)
    start, end = cairo_day_bounds(report_date)
    orders = _read(spark, silver_root, "silver_express_orders")
    if orders is None:
        return []
    orders = (orders.where((F.col("created_at") >= F.lit(start)) & (F.col("created_at") < F.lit(end)))
              .select("order_id", "store_id", "promised_minutes", "created_at"))
    status = _read(spark, silver_root, "silver_order_status")
    if status is None:
        df = (orders.withColumn("delivered_at", F.lit(None).cast("timestamp"))
              .withColumn("latest_status", F.lit(None).cast("string")))
    else:
        status = status.join(orders.select("order_id"), "order_id", "left_semi")
        delivered = (status.where(F.col("status") == "delivered").groupBy("order_id")
                     .agg(F.min("status_time").alias("delivered_at")))
        window = Window.partitionBy("order_id").orderBy(F.col("status_time").desc(), F.col("status_rank").desc())
        latest = (status.withColumn("_rn", F.row_number().over(window)).where("_rn = 1")
                  .select("order_id", F.col("status").alias("latest_status")))
        df = orders.join(delivered, "order_id", "left").join(latest, "order_id", "left")
    is_delivered = F.col("delivered_at").isNotNull()
    promise = F.col("promised_minutes") * 60
    took = F.col("delivered_at").cast("long") - F.col("created_at").cast("long")  # seconds
    age = F.lit(now).cast("long") - F.col("created_at").cast("long")
    closed = F.coalesce(F.col("latest_status").isin(*CLOSED_STATUSES), F.lit(False))
    on_time = is_delivered & (took <= promise)
    late = is_delivered & (took > promise)
    overdue = ~is_delivered & ~closed & (age > promise + GRACE_SECONDS)
    out = (df.groupBy("store_id")
           .agg(F.count(F.lit(1)).alias("orders"), F.sum(is_delivered.cast("int")).alias("delivered"),
                F.sum(on_time.cast("int")).alias("on_time"), F.sum((late | overdue).cast("int")).alias("breached"),
                F.avg(F.when(is_delivered, took / 60.0)).alias("avg_min"),
                F.max(F.when(is_delivered, took / 60.0)).alias("max_min"))
           .collect())
    return sorted((dict(report_date=report_date, store_id=r["store_id"], orders=int(r["orders"]),
                        delivered=int(r["delivered"] or 0), on_time=int(r["on_time"] or 0),
                        breached=int(r["breached"] or 0), avg_delivery_minutes=_minutes(r["avg_min"]),
                        max_delivery_minutes=_minutes(r["max_min"])) for r in out), key=lambda x: x["store_id"])


def rollup_oos_demand_daily(pg_store, report_date):
    """Replace the oos_demand_daily rows of one Cairo day with the sums of live_oos_demand_minute (idempotent)."""
    day = report_date.isoformat()  # a date object: nothing else can reach the SQL text
    # DELETE first, same transaction: a key that is no longer produced (a campaign re-mapped to another id after a
    # campaign-list change) must not keep its old row and double-count the day.
    delete = "DELETE FROM oos_demand_daily WHERE report_date = DATE'%s'" % day
    insert = (
        "INSERT INTO oos_demand_daily (report_date, store_id, product_id, campaign_id, utm_campaign, served_views, "
        "served_add_to_cart, served_clicks, oos_views, oos_add_to_cart, oos_clicks) "
        "SELECT (minute_start AT TIME ZONE 'Africa/Cairo')::date, store_id, product_id, campaign_id, min(utm_campaign), "
        "sum(views), sum(add_to_cart), sum(landing_clicks), "
        "coalesce(sum(views) FILTER (WHERE oos_at_write), 0), "
        "coalesce(sum(add_to_cart) FILTER (WHERE oos_at_write), 0), "
        "coalesce(sum(landing_clicks) FILTER (WHERE oos_at_write), 0) FROM live_oos_demand_minute "
        "WHERE (minute_start AT TIME ZONE 'Africa/Cairo')::date = DATE'%s' GROUP BY 1, 2, 3, 4 "
        "ON CONFLICT (report_date, store_id, product_id, campaign_id) DO UPDATE SET "
        "utm_campaign = EXCLUDED.utm_campaign, served_views = EXCLUDED.served_views, "
        "served_add_to_cart = EXCLUDED.served_add_to_cart, served_clicks = EXCLUDED.served_clicks, "
        "oos_views = EXCLUDED.oos_views, oos_add_to_cart = EXCLUDED.oos_add_to_cart, "
        "oos_clicks = EXCLUDED.oos_clicks" % day)
    pg_store.execute_atomic([delete, insert])
