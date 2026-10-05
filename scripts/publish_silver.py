"""Publish the local Silver Delta tables to the Databricks Silver mirror (allow-listed columns only).

Run in the Spark container so DATABRICKS_* from .env reach it:
  docker compose run --rm spark python scripts/publish_silver.py --dry-run
"""
import argparse
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from warehouse.publish import PUBLISH_ALLOWLIST, make_uploader, publish
from warehouse.store import StoreConfigError, env_catalog, env_volume, get_store


def _get_spark():
    from streaming.common.spark import get_spark
    return get_spark("publish_silver")


def main(argv=None):
    ap = argparse.ArgumentParser(description="Export Silver (allow-listed columns), upload to a Databricks "
                                             "volume, MERGE into the Silver mirror.")
    ap.add_argument("--lake", default="data/lake")
    ap.add_argument("--export-dir", default="data/export")
    ap.add_argument("--tables", help="comma separated subset of: " + ", ".join(PUBLISH_ALLOWLIST))
    ap.add_argument("--dry-run", action="store_true", help="export locally, print the statements, upload nothing")
    ap.add_argument("--export-only", action="store_true", help="stop after the local Parquet export")
    ap.add_argument("--upload-only", action="store_true", help="skip the export, upload what is in --export-dir")
    args = ap.parse_args(argv)
    if args.export_only and args.upload_only:
        print("error: --export-only and --upload-only cannot be combined", file=sys.stderr)
        return 2
    tables = [t.strip() for t in args.tables.split(",") if t.strip()] if args.tables else None
    if tables and any(t not in PUBLISH_ALLOWLIST for t in tables):
        print("error: unknown table(s): %s" % ", ".join(t for t in tables if t not in PUBLISH_ALLOWLIST),
              file=sys.stderr)
        return 2
    store = uploader = spark = None
    try:
        catalog, volume = env_catalog(), env_volume()
        if not (args.dry_run or args.export_only):  # these two never talk to Databricks
            store = get_store("databricks")
            uploader = make_uploader(os.environ["DATABRICKS_HOST"], os.environ["DATABRICKS_TOKEN"])
            store.execute("CREATE VOLUME IF NOT EXISTS %s.silver.%s" % (catalog, volume))
        if not args.upload_only:
            spark = _get_spark()
        result = publish(spark, args.lake, args.export_dir, store, uploader, catalog, volume, tables=tables,
                         dry_run=args.dry_run, export_only=args.export_only, upload_only=args.upload_only)
    except (StoreConfigError, ValueError) as e:
        print("error: %s" % e, file=sys.stderr)
        return 2
    except Exception as e:
        token = os.environ.get("DATABRICKS_TOKEN") or ""
        text = " ".join(str(e).split())[:300]
        print("error: publish failed (%s): %s" % (type(e).__name__, text.replace(token, "***") if token else text),
              file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()
        if store is not None:
            store.close()
    for table, r in result.items():
        print("%-28s rows=%-8d files=%d merged=%s" % (table, r["rows"], r["files"], r["merged"]))
        for stmt in r.get("statements", []):
            print("  would run: " + stmt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
