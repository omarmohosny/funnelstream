"""Build inventory_daily, oos_demand_daily and express_daily in Postgres for one or more Cairo days.

Needs Spark (the Silver tables) and PG_DSN, not Databricks: run it in the spark container:
  docker compose run --rm spark python scripts/build_inventory_daily.py [--date today] [--days 2]
By default it rebuilds the last 2 Cairo days (yesterday and the day before), so late events and express orders that
were still open at midnight are corrected by the next run. This script is the one writer of the three daily tables.
"""
import argparse
import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from streaming.common.spark import get_spark
from warehouse.inventory_daily import compute_express_daily, compute_inventory_daily, rollup_oos_demand_daily
from warehouse.store import get_store


def parse_day(text, today):
    if text == "today":
        return today
    if text == "yesterday":
        return today - timedelta(days=1)
    return date.fromisoformat(text)


def days_to_build(last, count):
    """The `count` Cairo days ending at `last`, oldest first."""
    return [last - timedelta(days=back) for back in range(count - 1, -1, -1)]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--date", default="yesterday", help="last Cairo day to build: today, yesterday or YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=2, help="how many days to build, ending at --date (default 2)")
    ap.add_argument("--lake", default="data/lake")
    args = ap.parse_args(argv)
    if args.days < 1:
        print("error: --days must be at least 1", file=sys.stderr)
        return 2
    today = datetime.now(ZoneInfo("Africa/Cairo")).date()
    last = parse_day(args.date, today)
    lake = os.path.abspath(args.lake)
    pg = get_store("postgres")
    try:
        spark = get_spark("inventory_daily")
        for day in days_to_build(last, args.days):
            rows = compute_inventory_daily(spark, lake, day)
            if rows:
                pg.upsert("inventory_daily", rows, ["report_date", "store_id", "product_id"])
            express = compute_express_daily(spark, lake, day)
            if express:
                pg.upsert("express_daily", express, ["report_date", "store_id"])
            rollup_oos_demand_daily(pg, day)
            print("%s: %d inventory rows, %d express rows" % (day, len(rows), len(express)))
    finally:
        pg.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
