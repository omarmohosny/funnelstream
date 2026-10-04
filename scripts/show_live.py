"""Print the latest N rows of live_metrics_minute, newest first (run in Docker).

docker compose run --rm spark python scripts/show_live.py [--n 10] [--lake data/lake]
"""
import argparse

from streaming.common.spark import get_spark
from streaming.gold.live import live_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=10)
    ap.add_argument("--lake", default="data/lake")
    args = ap.parse_args()
    spark = get_spark("show_live")
    df = (spark.read.format("delta").load(live_path(args.lake))
          .orderBy(["minute_start", "country"], ascending=[False, True]).limit(args.n))
    try:
        print(df.toPandas().to_string(index=False))
    except ImportError:  # pandas is not installed in the Spark image
        df.show(args.n, truncate=False)


if __name__ == "__main__":
    main()
