"""Build the marketing D-1 mart in Postgres for one Cairo day (default: yesterday in Cairo).

Day rule: the demand is the Cairo day, the ad row is the row whose report_date (the ad API's UTC day) has the same date
value; the mismatch of at most 3 hours is accepted. Needs Databricks settings (the spend) and PG_DSN, not Spark:
  docker compose run --rm spark python scripts/build_d1_marketing.py [--date 2026-10-09]
"""
import argparse
import os
import sys
from datetime import date, datetime, timezone

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from simulator.campaigns import CAMPAIGNS
from warehouse import d1_marketing as d1
from warehouse.store import get_store


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--date", help="Cairo day to report, YYYY-MM-DD (default yesterday in Cairo)")
    args = ap.parse_args(argv)
    now = datetime.now(timezone.utc)
    today = now.astimezone(d1.CAIRO).date()
    report_date = date.fromisoformat(args.date) if args.date else d1.cairo_yesterday(now)
    dbx, pg = get_store("databricks"), get_store("postgres")
    try:
        spend = d1.read_spend_rows(dbx, report_date)
        served, oos, pairs = d1.read_oos_demand(pg, report_date)
        campaign_rows = d1.assemble_campaign_rows(report_date, today, CAMPAIGNS, spend, served, oos)
        product_rows = d1.assemble_product_rows(report_date, pairs, d1.read_prices(pg))
        # replace, not merge: a row the rebuild no longer produces must go (each call is one transaction); an
        # empty result leaves the earlier rows alone, so a missing ad row cannot wipe a good day
        if campaign_rows:
            pg.replace_for_date("marketing_d1_campaign", "report_date", report_date, campaign_rows,
                                ["report_date", "campaign_id"])
        if product_rows:
            pg.replace_for_date("marketing_d1_product", "report_date", report_date, product_rows,
                                ["report_date", "store_id", "product_id"])
    finally:
        dbx.close()
        pg.close()
    print("D-1 %s: %d campaign rows, %d product rows" % (report_date, len(campaign_rows), len(product_rows)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
