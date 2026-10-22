"""Add and fill the Cairo-time (*_cairo) columns of an existing local Silver lake.

New Silver rows get these columns from the Silver transformation. A lake written before that change has no such columns
(or NULLs in them), and Gold reads them for every day, so run this once on an old lake. It is idempotent.

  docker compose run --rm -e PG_DSN= spark python scripts/backfill_cairo_columns.py --lake data/lake          (plan only)
  docker compose run --rm -e PG_DSN= spark python scripts/backfill_cairo_columns.py --lake data/lake --yes    (executes)

The Databricks Silver mirror is not touched here: recreate its tables with apply_ddl.py --recreate and publish again
(docs/RUNNING.md), which is what the benchmark does.
"""
import argparse
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from streaming.silver.schemas import CAIRO_TWINS  # noqa: E402
from streaming.silver.spark_schemas import ALL_COLUMNS  # noqa: E402
from streaming.silver.writer import silver_path  # noqa: E402


def plan(lake):
    """[(table, twin, source)] for every Silver table that exists under `lake` and should carry a twin."""
    out = []
    for table, cols in ALL_COLUMNS.items():
        if not os.path.isdir(silver_path(lake, table)):
            continue
        for twin, source in CAIRO_TWINS.items():
            if twin in cols and source in cols:
                out.append((table, twin, source))
    return out


def run(lake, execute):
    steps = plan(lake)
    for table, twin, source in steps:
        print("%s: %s = Cairo time of %s" % (table, twin, source))
    if not execute:
        print("plan only; add --yes to execute (%d columns)" % len(steps))
        return 0
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    from streaming.common.spark import get_spark

    spark = get_spark("backfill_cairo_columns")
    for table, twin, source in steps:
        path = os.path.abspath(silver_path(lake, table))
        if twin not in spark.read.format("delta").load(path).columns:
            spark.sql("ALTER TABLE delta.`%s` ADD COLUMNS (`%s` TIMESTAMP)" % (path, twin))
        DeltaTable.forPath(spark, path).update(
            condition="`%s` IS NULL AND `%s` IS NOT NULL" % (twin, source),
            set={twin: F.expr("from_utc_timestamp(`%s`, 'Africa/Cairo')" % source)})
        print("filled %s.%s" % (table, twin))
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--lake", default="data/lake")
    ap.add_argument("--yes", action="store_true", help="really run (default: print the plan)")
    args = ap.parse_args(argv)
    return run(args.lake, args.yes)


if __name__ == "__main__":
    sys.exit(main())
