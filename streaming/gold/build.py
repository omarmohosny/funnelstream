"""Gold reference build: Silver Delta tables to fact_orders, fact_order_lines, fact_attribution, agg_orders_daily,
agg_funnel_daily, agg_campaign_daily, agg_city_product_daily.

python -m streaming.gold.build [--lake data/lake] [--out data/lake/gold_ref]
Surrogate keys are -1 (unknown member) here: the reference build reads no dimensions. Campaigns are resolved
from simulator.campaigns (the same rows as dim_campaign); fact_orders.campaign_id is the last_touch campaign.
"""
import argparse
import os
from datetime import datetime, timezone

from simulator.campaigns import CAMPAIGNS
from streaming.gold.attribution import build_clicks, build_fact_attribution
from streaming.gold.bots import mark_bot_sessions
from streaming.gold.agg_campaign import build_agg_campaign_daily
from streaming.gold.agg_orders import build_agg_orders_daily
from streaming.gold.city_product import build_agg_city_product_daily
from streaming.gold.funnel import build_agg_funnel_daily
from streaming.gold.orders import build_fact_order_lines, build_fact_orders, with_last_touch_campaign
from streaming.silver.writer import silver_path


SNAPSHOT_SCHEMA = ("campaign_id string, report_date date, pulled_at timestamp, utm_campaign string, "
                   "ad_platform string, impressions long, clicks long, platform_conversions long, "
                   "spend decimal(12,4), currency string")


def build_all(spark, lake_root: str = "data/lake", out_root=None, as_of=None) -> dict:
    """Read Silver, recompute is_bot over the whole events table, build the facts and summaries.

    silver_campaign_snapshots is not part of the local lake (ads land in Postgres/Databricks): when the lake has
    no such table agg_campaign_daily is built from an empty snapshot set (order measures only). as_of is the
    timestamp of the open rate (default: now).

    Returns {table_name: DataFrame}; with out_root also writes each as Delta under <out_root>/<table>
    (overwrite) and returns DataFrames reading the written tables back.
    """
    lake_root = os.path.abspath(lake_root)
    out_root = os.path.abspath(out_root) if out_root else None

    def read(table):
        return spark.read.format("delta").load(silver_path(lake_root, table))

    events = mark_bot_sessions(read("silver_events"))
    orders, status, items = read("silver_orders"), read("silver_order_status"), read("silver_order_items")
    base_orders = build_fact_orders(orders, status)
    camps = spark.createDataFrame([(c["campaign_id"], c["utm_campaign"]) for c in CAMPAIGNS],
                                  "campaign_id string, utm_campaign string")
    # attribution covers the orders fact_orders has (orders with status rows), like the Gold SQL does
    attributed = orders.join(base_orders.select("order_id"), "order_id", "left_semi")
    attribution = build_fact_attribution(attributed, build_clicks(events, camps))
    fact_orders = with_last_touch_campaign(base_orders, attribution)
    snap_path = silver_path(lake_root, "silver_campaign_snapshots")
    snapshots = (read("silver_campaign_snapshots") if os.path.isdir(snap_path)
                 else spark.createDataFrame([], SNAPSHOT_SCHEMA))
    order_lines = build_fact_order_lines(orders, items)
    result = {
        "fact_orders": fact_orders,
        "fact_order_lines": order_lines,
        "fact_attribution": attribution,
        "agg_orders_daily": build_agg_orders_daily(fact_orders),
        "agg_funnel_daily": build_agg_funnel_daily(events, orders),
        "agg_campaign_daily": build_agg_campaign_daily(fact_orders, attribution, snapshots,
                                                       as_of or datetime.now(timezone.utc)),
        "agg_city_product_daily": build_agg_city_product_daily(events, order_lines),
    }
    if out_root:
        for name, df in list(result.items()):
            path = os.path.join(out_root, name)
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
            result[name] = spark.read.format("delta").load(path)
    return result


if __name__ == "__main__":
    from streaming.common.spark import get_spark

    ap = argparse.ArgumentParser()
    ap.add_argument("--lake", default="data/lake")
    ap.add_argument("--out", default="data/lake/gold_ref")
    args = ap.parse_args()
    sp = get_spark("gold_build")
    for table, frame in build_all(sp, args.lake, args.out).items():
        print("%s: %d rows" % (table, frame.count()))
