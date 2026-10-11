"""Duplicate-key check on the local Silver Delta tables (read-only).

docker compose run --rm sync python scripts/check_duplicates.py [--lake data/lake]

Prints one line per table (rows and duplicate keys) and a final NO DUPLICATES or DUPLICATES FOUND line.
Exit codes: 0 no duplicates, 1 duplicates found, 2 the lake path or a table is missing (message, no traceback).
Nothing is written. A Delta read sees the last committed version of a table, so a batch that the streaming job is
writing right now is not part of the result and the read never blocks the job.
"""
import argparse
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# The Silver keys are defined in streaming/silver/writer.py (SILVER_KEYS); that module needs delta, which the host
# does not have, so the tables to check are named here and their keys are read from SILVER_KEYS at run time.
TABLES = ["silver_events", "silver_orders", "silver_order_status", "silver_order_items",
          "silver_stock_movements", "silver_stock_snapshots", "silver_express_orders"]
# Tables that exist only in a lake that ever saw inventory events: absent is normal for a run without --inventory.
OPTIONAL = {"silver_stock_movements", "silver_stock_snapshots", "silver_express_orders"}


def check_lake(spark, lake):
    """Return (results, missing). results = [(table, key_cols, rows, duplicate_keys)], missing = [table, ...]."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from streaming.silver.writer import SILVER_KEYS, silver_path

    results, missing = [], []
    for table in TABLES:
        path = silver_path(lake, table)
        if not (os.path.isdir(path) and DeltaTable.isDeltaTable(spark, path)):
            missing.append(table)
            continue
        keys = SILVER_KEYS[table]
        df = spark.read.format("delta").load(path)
        rows = df.count()
        dups = df.groupBy(*keys).count().where(F.col("count") > 1).count()
        results.append((table, keys, rows, dups))
    return results, missing


def format_line(table, keys, rows, dups):
    return "%-20s key=%-20s rows=%d duplicate_keys=%d" % (table, ",".join(keys), rows, dups)


def run(lake, spark=None, out=print):
    """Check the lake; returns the exit code (0, 1 or 2)."""
    if not os.path.isdir(os.path.join(lake, "silver")):
        out("Lake path not found: %s (expected %s)" % (lake, os.path.join(lake, "silver")))
        return 2
    if spark is None:
        from streaming.common.spark import get_spark
        spark = get_spark("check_duplicates")
        spark.sparkContext.setLogLevel("ERROR")
    results, missing = check_lake(spark, lake)
    for r in results:
        out(format_line(*r))
    absent_optional = [t for t in missing if t in OPTIONAL]
    missing = [t for t in missing if t not in OPTIONAL]
    if absent_optional:
        out("Not present (no inventory data in this lake): %s" % ", ".join(absent_optional))
    if missing:
        out("Missing Silver table(s): %s" % ", ".join(missing))
        return 2
    if any(r[3] for r in results):
        out("DUPLICATES FOUND")
        return 1
    out("NO DUPLICATES")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description="Report duplicate keys in the local Silver tables (read-only)")
    ap.add_argument("--lake", default="data/lake", help="lake root (default data/lake)")
    args = ap.parse_args(argv)
    return run(args.lake)


if __name__ == "__main__":
    sys.exit(main())
