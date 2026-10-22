"""Marketing D-1 mart (inventory design, section 9).

The pure functions turn yesterday's numbers into the rows of two Postgres tables. The readers (Databricks spend,
Postgres demand and prices) are thin and live below.

Day rule: every measure is the Cairo day, the ad report included (the ad account reports in Cairo time);
agg_campaign_daily.report_date is that day. The job takes yesterday in Cairo for the demand and the ad row.

The metric is an ESTIMATE: exposed_spend = spend x oos_clicks / served_clicks, both counts from live_oos_demand_minute
(served districts only, the same landing-click definition), assuming spend is spread evenly over served landing clicks.
The out-of-stock flag is the stock when the minute was written (not replay-safe). It is titled "estimated spend exposed
to out-of-stock" everywhere.
"""
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

PROVISIONAL_DAYS = 3  # ROAS delivered matures one to three days after the order day
CAIRO = ZoneInfo("Africa/Cairo")
_CENT, _SHARE = Decimal("0.01"), Decimal("0.0001")


def cairo_yesterday(now):
    """The default D-1 date: yesterday on the Cairo calendar (`now` is an aware datetime)."""
    return now.astimezone(CAIRO).date() - timedelta(days=1)


def exposed_spend(spend, oos_clicks, served_clicks):
    """spend * oos_clicks / served_clicks to two decimals; None when there are no served clicks or no spend."""
    if spend is None or not served_clicks:
        return None
    return (Decimal(str(spend)) * oos_clicks / served_clicks).quantize(_CENT, rounding=ROUND_HALF_UP)


def assemble_campaign_rows(report_date, today, campaigns, spend_rows, served_by_campaign, oos_by_campaign):
    """One row per campaign that has a spend row for the day (campaigns: dicts with campaign_id and utm_campaign)."""
    utm_of = {c["campaign_id"]: c["utm_campaign"] for c in campaigns}
    provisional = (today - report_date) < timedelta(days=PROVISIONAL_DAYS)
    out = []
    for s in spend_rows:
        cid = s["campaign_id"]
        served, oos = int(served_by_campaign.get(cid, 0)), int(oos_by_campaign.get(cid, 0))
        share = (Decimal(oos) / Decimal(served)).quantize(_SHARE, rounding=ROUND_HALF_UP) if served else None
        out.append(dict(report_date=report_date, campaign_id=cid, utm_campaign=utm_of.get(cid, ""),
                        spend=None if s["spend"] is None else Decimal(str(s["spend"])).quantize(_CENT),
                        ad_clicks=s["clicks"], orders_placed=s["orders_placed"], revenue_placed=s["revenue_placed"],
                        revenue_delivered=s["revenue_delivered"], cpa=s["cpa"], roas_placed=s["roas_placed"],
                        roas_delivered=s["roas_delivered"], served_clicks=served, oos_clicks=oos,
                        oos_click_share=share, exposed_spend=exposed_spend(s["spend"], oos, served),
                        provisional=provisional))
    return out


def assemble_product_rows(report_date, oos_demand_by_pair, price_by_product):
    """One row per (store, product) with demand flagged out of stock that day; every pair is kept, the visual does Top N.

    oos_demand_by_pair: {(store_id, product_id): (oos_views, oos_add_to_carts)}. revenue_at_risk is add-to-carts times
    price, NULL when the product has no price. Sorted by revenue at risk, then store and product."""
    rows = []
    for (store_id, product_id), (views, carts) in oos_demand_by_pair.items():
        price = price_by_product.get(product_id)
        rows.append(dict(report_date=report_date, store_id=store_id, product_id=product_id, oos_views=int(views),
                         oos_add_to_carts=int(carts),
                         revenue_at_risk=None if price is None else (Decimal(str(price)) * int(carts)).quantize(_CENT)))
    rows.sort(key=lambda r: (r["revenue_at_risk"] is None, -(r["revenue_at_risk"] or 0), r["store_id"], r["product_id"]))
    return rows


_CAIRO_DAY = "(minute_start AT TIME ZONE 'Africa/Cairo')::date = %s"


def read_spend_rows(dbx_store, report_date):
    """D-1 rows of agg_campaign_daily (last touch) from Databricks: the ad row whose report_date has the same date value."""
    sql = ("SELECT campaign_id, spend, clicks, orders_placed, revenue_placed, revenue_delivered, cpa, roas_placed, "
           "roas_delivered FROM %s WHERE report_date = DATE'%s' AND attribution_model = 'last_touch'"
           % (dbx_store.qualified("agg_campaign_daily"), report_date.isoformat()))
    cols = ["campaign_id", "spend", "clicks", "orders_placed", "revenue_placed", "revenue_delivered", "cpa", "roas_placed",
            "roas_delivered"]
    return [dict(zip(cols, r)) for r in dbx_store.query(sql)]


def read_oos_demand(pg_store, report_date):
    """(served clicks by campaign_id, out-of-stock clicks by campaign_id, {(store, product): (oos views, oos add-to-carts)}).

    All three come from live_oos_demand_minute for the Cairo day, so numerator and denominator of the share have the same
    population: landing clicks in served districts. 'Out of stock' is the oos_at_write flag."""
    served, oos = {}, {}
    for cid, n_served, n_oos in pg_store.query(
            "SELECT campaign_id, sum(landing_clicks), coalesce(sum(landing_clicks) FILTER (WHERE oos_at_write), 0) "
            "FROM live_oos_demand_minute WHERE " + _CAIRO_DAY + " GROUP BY campaign_id", (report_date,)):
        served[cid], oos[cid] = int(n_served), int(n_oos)
    pairs = {(store_id, product_id): (int(views), int(carts)) for store_id, product_id, views, carts in pg_store.query(
        "SELECT store_id, product_id, sum(views), sum(add_to_cart) FROM live_oos_demand_minute WHERE " + _CAIRO_DAY
        + " AND oos_at_write GROUP BY store_id, product_id", (report_date,))}
    return served, oos, pairs


def read_prices(pg_store):
    """{product_id: price} from dim_product_current (filled by scripts/apply_live_ddl.py from silver_products)."""
    return {pid: Decimal(str(price)) for pid, price in
            pg_store.query("SELECT product_id, price FROM dim_product_current WHERE price IS NOT NULL")}
