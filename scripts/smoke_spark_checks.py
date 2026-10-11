"""Container-side reads for scripts/smoke_test.py (read-only except express-backfill; run it with `docker compose run --rm spark python ...`).

  python scripts/smoke_spark_checks.py counts     --lake data/smoke/lake
  python scripts/smoke_spark_checks.py invariants --lake data/smoke/lake
  python scripts/smoke_spark_checks.py express-backfill --lake data/smoke/lake

Both print one last line, `SMOKE_JSON {...}`, which the smoke test parses.
  counts      rows of every Bronze and Silver table (null when the table does not exist) and the Cairo day of the
              newest silver_events row (`data_day`).
  invariants  the quarantine row count and the stock checks: for every store and product pair the stock is replayed
              in Python from all snapshots and movements (a snapshot sets the level, a movement adds to it, a movement
              at the same time as a snapshot counts first) and compared with the live rule of the serving layer
              (latest snapshot plus the later movements, warehouse.live_stock._stock_levels).
  express-backfill  the one mode that writes: live_express_orders in the smoke database, computed from all of Silver
              (the live sink writes only the last 3 hours, a history run is older; see express_backfill).
Nothing else is written.
"""
import argparse
import datetime
import json
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

BRONZE_TABLES = ["bronze_app_events", "bronze_orders", "bronze_delivery_events", "bronze_inventory_events"]
SILVER_TABLES = ["silver_events", "silver_orders", "silver_order_status", "silver_order_items", "silver_quarantine",
                 "silver_products", "silver_customers", "silver_stock_movements", "silver_stock_snapshots",
                 "silver_express_orders"]
SAMPLE = 5


def replay_stock(snapshots, movements):
    """Replay the stock of every pair from its events. Returns {(store_id, product_id): (final_level, lowest_level)}.

    snapshots: dicts with store_id, product_id, event_time, event_id, on_hand. movements: the same with qty_delta.
    A pair without a snapshot has no level (the live rule skips it too). Events are ordered by time; at equal times a
    movement comes before the snapshot, because the snapshot already contains it (the live rule applies only the
    movements strictly after the latest snapshot)."""
    events = [(e["event_time"], 1, e["event_id"], e["store_id"], e["product_id"], "snap", e["on_hand"]) for e in snapshots]
    events += [(e["event_time"], 0, e["event_id"], e["store_id"], e["product_id"], "move", e["qty_delta"]) for e in movements]
    events.sort(key=lambda t: (t[0], t[1], t[2]))
    level, lowest = {}, {}
    for _, _, _, store, product, kind, value in events:
        key = (store, product)
        if kind == "snap":
            level[key] = int(value)
        elif key in level:
            level[key] += int(value or 0)
        else:
            continue
        lowest[key] = min(lowest.get(key, level[key]), level[key])
    return {k: (level[k], lowest[k]) for k in level}


def compare_stock(replayed, live):
    """(negative pairs, mismatching pairs). live: {(store_id, product_id): on_hand} from the live rule."""
    negative = sorted(k for k, (_, low) in replayed.items() if low < 0)
    mismatch = sorted(k for k in set(replayed) | set(live) if (replayed.get(k) or (None,))[0] != live.get(k))
    return negative, mismatch


def _delta(spark, root, table):
    from delta.tables import DeltaTable
    path = os.path.join(os.path.abspath(root), table.split("_", 1)[0], table)
    return spark.read.format("delta").load(path) if os.path.isdir(path) and DeltaTable.isDeltaTable(spark, path) else None


def counts(spark, lake):
    from pyspark.sql import functions as F
    out = {"bronze": {}, "silver": {}, "data_day": None}
    for group, tables in (("bronze", BRONZE_TABLES), ("silver", SILVER_TABLES)):
        for t in tables:
            df = _delta(spark, lake, t)
            out[group][t] = None if df is None else df.count()
    events = _delta(spark, lake, "silver_events")
    if events is not None and out["silver"]["silver_events"]:
        day = events.agg(F.date_format(F.from_utc_timestamp(F.max("event_time"), "Africa/Cairo"), "yyyy-MM-dd")).first()[0]
        out["data_day"] = day
    return out


def invariants(spark, lake):
    from warehouse.live_stock import _stock_levels
    quarantine = _delta(spark, lake, "silver_quarantine")
    snaps = _delta(spark, lake, "silver_stock_snapshots")
    moves = _delta(spark, lake, "silver_stock_movements")
    out = {"quarantine": None if quarantine is None else quarantine.count(), "pairs": 0, "negative": [], "mismatch": []}
    if snaps is None:
        return out
    snap_rows = [r.asDict() for r in snaps.select("store_id", "product_id", "event_time", "event_id", "on_hand").collect()]
    move_rows = ([] if moves is None else
                 [r.asDict() for r in moves.select("store_id", "product_id", "event_time", "event_id", "qty_delta").collect()])
    replayed = replay_stock(snap_rows, move_rows)
    levels = _stock_levels(spark, os.path.abspath(lake))
    live = {(r["store_id"], r["product_id"]): int(r["on_hand"]) for r in levels.collect()} if levels is not None else {}
    negative, mismatch = compare_stock(replayed, live)
    out.update(pairs=len(replayed), negative_count=len(negative), mismatch_count=len(mismatch),
               negative=["%s/%s" % k for k in negative[:SAMPLE]],
               mismatch=["%s/%s replay=%s live=%s" % (k + ((replayed.get(k) or (None,))[0], live.get(k)))
                         for k in mismatch[:SAMPLE]])
    return out


EXPRESS_SINCE = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)  # "from the beginning" for the backfill


def express_backfill(spark, lake):
    """Fill live_express_orders (in the database PG_DSN names, the smoke database) from ALL of Silver.

    The live sink only writes the express orders created in the last 3 hours of the wall clock, and a history run is
    days old, so after it the table is empty. The same compute with a `since` far in the past fills it, and the rules
    and checks that read the table then see the simulated orders. Writes by order_id, so it can run again."""
    from warehouse.live_express import compute_express_rows
    from warehouse.store import get_store
    rows = compute_express_rows(spark, os.path.abspath(lake), EXPRESS_SINCE)
    store = get_store("postgres")
    try:
        written = store.upsert("live_express_orders", rows, ["order_id"]) if rows else 0
    finally:
        store.close()
    return {"express_rows": len(rows), "written": written}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("what", choices=["counts", "invariants", "express-backfill"])
    ap.add_argument("--lake", default="data/smoke/lake")
    args = ap.parse_args(argv)
    from streaming.common.spark import get_spark
    spark = get_spark("smoke_checks")
    spark.sparkContext.setLogLevel("ERROR")
    if args.what == "express-backfill":
        result = express_backfill(spark, args.lake)
    else:
        result = counts(spark, args.lake) if args.what == "counts" else invariants(spark, args.lake)
    print("SMOKE_JSON " + json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
