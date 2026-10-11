"""Live stock and out-of-stock demand (inventory design, sections 7 and 8).

Three Spark computes (no database: stock now, served demand, current products) and one Postgres writer. The stock and
demand computes run in the live sink's thread pool beside the live-metrics and health computes; the writer runs on the
sink's single connection.
"""
from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import Window, functions as F

from contracts.stores import district_rows
from simulator.campaigns import CAMPAIGNS
from streaming.gold.live import MAX_MINUTE_LIST, minute_epochs
from warehouse.live_common import _read, _utc

DEMAND_KEYS = ["minute_start", "store_id", "product_id", "campaign_id"]


def _stock_levels(spark, silver_root, pairs=None, as_of=None):
    """DataFrame (store_id, product_id, on_hand, reserved_qty, reorder_point): the latest snapshot plus the movements
    strictly after it, both taken before `as_of` (exclusive; None means no limit).

    pairs None means every store and product pair. Returns None when no snapshot table exists. A pair without any
    snapshot has no row. Used by the live sink (no as_of) and by the daily inventory mart (as_of = end of the day)."""
    snaps = _read(spark, silver_root, "silver_stock_snapshots")
    if snaps is None:
        return None
    moves = _read(spark, silver_root, "silver_stock_movements")
    cols = ["store_id", "product_id"]
    keys = (F.broadcast(spark.createDataFrame(sorted(set(pairs)), "store_id string, product_id string"))
            if pairs is not None else None)

    def narrow(df):
        if keys is not None:
            df = df.join(keys, cols, "left_semi")
        if as_of is not None:
            df = df.where(F.col("event_time") < F.lit(as_of))
        return df

    window = Window.partitionBy(*cols).orderBy(F.col("event_time").desc(), F.col("event_id").desc())
    latest = (narrow(snaps).withColumn("_rn", F.row_number().over(window)).where("_rn = 1")
              .select(*cols, "on_hand", "reserved_qty", "reorder_point", F.col("event_time").alias("snap_time")))
    if moves is None:
        return latest.select(*cols, "on_hand", "reserved_qty", "reorder_point")
    m = narrow(moves).select(*cols, "qty_delta", "event_time")
    after = F.col("event_time") > F.col("snap_time")
    agg = (latest.join(m, cols, "left")
           .groupBy(*cols, "on_hand", "reserved_qty", "reorder_point")
           .agg(F.coalesce(F.sum(F.when(after, F.col("qty_delta"))), F.lit(0)).alias("delta")))
    return agg.select(*cols, (F.col("on_hand") + F.col("delta")).alias("on_hand"), "reserved_qty", "reorder_point")


def compute_stock_rows(spark, silver_root, pairs, now):
    """live_stock_now rows for the (store_id, product_id) pairs: the latest snapshot plus the movements strictly after it.

    A pair without any snapshot is skipped (it is repaired when its first snapshot arrives). available is never
    negative; on_hand keeps the raw value so a stock error stays visible."""
    pairs = sorted(set(pairs))
    if not pairs:
        return []
    levels = _stock_levels(spark, silver_root, pairs=pairs)
    if levels is None:
        return []
    out = []
    for r in levels.collect():
        on_hand, reserved = int(r["on_hand"]), int(r["reserved_qty"] or 0)
        out.append(dict(store_id=r["store_id"], product_id=r["product_id"], on_hand=on_hand, reserved_qty=reserved,
                        available=max(on_hand - reserved, 0), reorder_point=int(r["reorder_point"] or 0),
                        updated_at=now))
    return out


def compute_demand_rows(spark, silver_root, minutes):
    """Product views and add-to-carts per (minute, store, product, campaign) for the event-time `minutes`.

    ALL demand of served districts is returned, whatever the stock was: there is no out-of-stock filter here, and no
    flag (write_oos_demand sets `oos_at_write`). The store comes from the customer's city and district through the one
    store map. `campaign_id` comes from the utm_campaign through the static campaign list ('direct' when the event has
    none, 'unknown' when the utm_campaign is not listed); `utm_campaign` stays as an attribute. A landing click is a
    product view that carries a utm_campaign. Events of a place without a store are left out. Bots are not removed (v1)."""
    epochs = minute_epochs(minutes)
    if not epochs:
        return []
    events = _read(spark, silver_root, "silver_events")
    if events is None:
        return []
    mapping = spark.createDataFrame([(r["city"], r["district"], r["store_id"]) for r in district_rows()],
                                    "city string, district string, store_id string")
    campaigns = spark.createDataFrame([(c["utm_campaign"], c["campaign_id"]) for c in CAMPAIGNS],
                                      "utm_campaign string, _cid string")
    sec = F.unix_timestamp("event_time")
    minute = sec - (sec % 60)
    if len(epochs) <= MAX_MINUTE_LIST:
        in_window = minute.isin(epochs)
    else:
        in_window = (sec >= epochs[0]) & (sec < epochs[-1] + 60)
    is_view, is_cart = F.col("event_type") == "product_view", F.col("event_type") == "add_to_cart"
    has_utm = F.col("utm_campaign").isNotNull() & (F.col("utm_campaign") != "")
    campaign_id = F.when(~has_utm, F.lit("direct")).otherwise(F.coalesce(F.col("_cid"), F.lit("unknown")))
    df = (events.where((is_view | is_cart) & F.col("product_id").isNotNull() & in_window)
          .join(F.broadcast(mapping), ["city", "district"], "inner")
          .join(F.broadcast(campaigns), ["utm_campaign"], "left")
          .groupBy(minute.alias("m"), "store_id", "product_id", campaign_id.alias("cid"))
          .agg(F.sum(is_view.cast("int")).alias("views"), F.sum(is_cart.cast("int")).alias("carts"),
               F.sum((is_view & has_utm).cast("int")).alias("clicks"),
               F.min(F.coalesce(F.col("utm_campaign"), F.lit(""))).alias("utm")))
    return [dict(minute_start=_utc(r["m"]), store_id=r["store_id"], product_id=r["product_id"], campaign_id=r["cid"],
                 utm_campaign=r["utm"], views=int(r["views"] or 0), add_to_cart=int(r["carts"] or 0),
                 landing_clicks=int(r["clicks"] or 0))
            for r in df.collect()]


def write_oos_demand(store, rows):
    """Upsert the demand rows of the touched minutes, every one of them, each flagged `oos_at_write`.

    The flag is true when the pair had no available stock in live_stock_now at the moment the sink writes: it is the
    stock when the minute was written, not the stock at event time. Nothing is filtered out. Known limitation: the flag
    is not replay-safe; a replay, a late event or a rewrite of the minute after a restock can flag it differently, and a
    stock-out that starts in the middle of a minute credits the whole minute."""
    if not rows:
        return 0
    empty = {(a, b) for a, b in store.query("SELECT store_id, product_id FROM live_stock_now WHERE available <= 0")}
    flagged = [dict(r, oos_at_write=(r["store_id"], r["product_id"]) in empty) for r in rows]
    return store.upsert("live_oos_demand_minute", flagged, DEMAND_KEYS)


def compute_product_rows(spark, silver_root):
    """dim_product_current rows: the latest silver_products version of every product ([] without the table).

    Written by scripts/apply_live_ddl.py next to dim_store so Grafana tables can show names instead of ids."""
    products = _read(spark, silver_root, "silver_products")
    if products is None:
        return []
    latest = Window.partitionBy("product_id").orderBy(F.col("updated_at").desc())
    rows = (products.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1")
            .select("product_id", "name", "category", "price").collect())
    return [dict(product_id=r["product_id"], name=r["name"], category=r["category"],
                 price=None if r["price"] is None else Decimal(str(r["price"]))) for r in rows]
