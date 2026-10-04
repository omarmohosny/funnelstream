"""Gold reference build: Silver Delta tables to fact_orders, fact_order_lines, fact_funnel_daily.

python -m streaming.gold.build [--lake data/lake] [--out data/lake/gold_ref]
Task 10c adds the surrogate keys and the attribution campaign_id; this build uses natural ids.
"""
import argparse
import os

from streaming.gold.bots import mark_bot_sessions
from streaming.gold.funnel import build_fact_funnel_daily
from streaming.gold.orders import build_fact_order_lines, build_fact_orders
from streaming.silver.writer import silver_path


def build_all(spark, lake_root: str = "data/lake", out_root=None) -> dict:
    """Read Silver, recompute is_bot over the whole events table, build the three facts.

    Returns {table_name: DataFrame}; with out_root also writes each as Delta under <out_root>/<table>
    (overwrite) and returns DataFrames reading the written tables back.
    """
    def read(table):
        return spark.read.format("delta").load(silver_path(lake_root, table))

    events = mark_bot_sessions(read("silver_events"))
    orders, status, items = read("silver_orders"), read("silver_order_status"), read("silver_order_items")
    result = {
        "fact_orders": build_fact_orders(orders, status),
        "fact_order_lines": build_fact_order_lines(orders, items),
        "fact_funnel_daily": build_fact_funnel_daily(events, orders),
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
