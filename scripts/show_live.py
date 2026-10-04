"""Print the latest N rows of live_metrics_minute, newest first (run in Docker).

docker compose run --rm spark python scripts/show_live.py [--n 10] [--lake data/lake]
"""
import argparse
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from streaming.gold.live import live_path  # noqa: E402

MISSING = "No live metrics yet. Run streaming.gold.live.refresh_live_metrics first."


def show_live(path, n=10, spark=None):
    """Print the latest n rows of the table at path; returns the exit code (1 if the table is missing)."""
    from delta.tables import DeltaTable
    from streaming.common.spark import get_spark

    path = os.path.abspath(path)
    spark = spark or get_spark("show_live")
    if not DeltaTable.isDeltaTable(spark, path):
        print(MISSING)
        return 1
    df = (spark.read.format("delta").load(path)
          .orderBy(["minute_start", "country"], ascending=[False, True]).limit(n))
    try:
        print(df.toPandas().to_string(index=False))
    except ImportError:  # pandas is not installed in the Spark image
        df.show(n, truncate=False)
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=10)
    ap.add_argument("--lake", default="data/lake")
    args = ap.parse_args()
    sys.exit(show_live(live_path(args.lake), args.n))


if __name__ == "__main__":
    main()
