from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from warehouse.d1_marketing import (assemble_campaign_rows, assemble_product_rows, utc_yesterday, exposed_spend,
                                    read_oos_demand, read_prices)

D = date(2026, 10, 9)
CAMPAIGNS = [{"campaign_id": "camp_201", "utm_campaign": "eid_sale_1"}, {"campaign_id": "camp_202", "utm_campaign": "ramadan_offer_1"}]
SPEND = [dict(campaign_id="camp_201", spend=Decimal("1000.0000"), clicks=500, orders_placed=20,
              revenue_placed=Decimal("5000.00"), revenue_delivered=Decimal("4000.00"), cpa=Decimal("50.00"),
              roas_placed=Decimal("5.00"), roas_delivered=Decimal("4.00"))]
SERVED, OOS = {"camp_201": 120}, {"camp_201": 30}


class FakePg:
    """Answers the three queries of the readers and records their text."""

    def __init__(self):
        self.sql = []

    def query(self, sql, params=None):
        self.sql.append(sql)
        if "dim_product_current" in sql:
            return [("p1", Decimal("100.00"))]
        if "GROUP BY campaign_id" in sql:
            return [("camp_201", 120, 30), ("direct", 0, 0)]
        return [("store_a", "p1", 7, 4)]


def test_exposed_spend_is_spend_times_the_out_of_stock_click_share():
    assert exposed_spend(Decimal("1000"), 30, 120) == Decimal("250.00")
    assert exposed_spend(Decimal("1000.0000"), 1, 3) == Decimal("333.33")
    assert exposed_spend(Decimal("1000"), 0, 120) == Decimal("0.00")


def test_exposed_spend_is_none_without_served_clicks_or_spend():
    assert exposed_spend(Decimal("1000"), 5, 0) is None
    assert exposed_spend(None, 5, 10) is None


def test_the_campaign_row_joins_spend_with_the_click_counts_through_the_campaign_id():
    rows = assemble_campaign_rows(D, date(2026, 10, 10), CAMPAIGNS, SPEND, SERVED, OOS)
    [row] = rows  # camp_202 has no spend row: it is not reported
    assert row["campaign_id"] == "camp_201" and row["utm_campaign"] == "eid_sale_1"
    assert (row["served_clicks"], row["oos_clicks"]) == (120, 30)
    assert row["oos_click_share"] == Decimal("0.2500")
    assert row["exposed_spend"] == Decimal("250.00")
    assert row["spend"] == Decimal("1000.00") and row["roas_delivered"] == Decimal("4.00")
    assert row["report_date"] == D


def test_a_recent_day_is_provisional_and_an_old_one_is_not():
    recent = assemble_campaign_rows(D, date(2026, 10, 10), CAMPAIGNS, SPEND, {"camp_201": 10}, {})[0]
    old = assemble_campaign_rows(D, date(2026, 10, 14), CAMPAIGNS, SPEND, {"camp_201": 10}, {})[0]
    assert recent["provisional"] is True and old["provisional"] is False


def test_a_campaign_without_served_clicks_has_no_share_and_no_exposed_spend():
    [row] = assemble_campaign_rows(D, date(2026, 10, 10), CAMPAIGNS, SPEND, {}, {})
    assert row["served_clicks"] == 0 and row["oos_click_share"] is None and row["exposed_spend"] is None


def test_every_out_of_stock_product_of_the_day_is_kept_per_store():
    demand = {("s1", "p1"): (4, 10), ("s2", "p1"): (1, 2), ("s1", "p2"): (6, 3), ("s1", "p6"): (2, 4), ("s1", "p7"): (5, 0)}
    prices = {"p1": Decimal("100.00"), "p2": Decimal("900.00"), "p7": Decimal("5.00")}  # p6 has no price
    rows = assemble_product_rows(D, demand, prices)
    assert len(rows) == 5  # no Top N here: the visual does it
    assert [(r["store_id"], r["product_id"]) for r in rows] == [("s1", "p2"), ("s1", "p1"), ("s2", "p1"), ("s1", "p7"), ("s1", "p6")]
    assert rows[0]["oos_add_to_carts"] == 3 and rows[0]["revenue_at_risk"] == Decimal("2700.00")
    assert rows[3]["revenue_at_risk"] == Decimal("0.00") and rows[4]["revenue_at_risk"] is None  # kept, but unpriced


def test_the_default_day_is_yesterday_on_the_utc_calendar():
    assert utc_yesterday(datetime(2026, 10, 9, 22, 30, tzinfo=timezone.utc)) == date(2026, 10, 8)
    assert utc_yesterday(datetime(2026, 10, 10, 0, 30, tzinfo=timezone.utc)) == date(2026, 10, 9)
    cairo = timezone(timedelta(hours=3))  # 01:30 on the 10th in Cairo is still the 9th in UTC
    assert utc_yesterday(datetime(2026, 10, 10, 1, 30, tzinfo=cairo)) == date(2026, 10, 8)


def test_both_sides_of_the_share_and_the_prices_come_from_postgres_not_from_silver():
    pg = FakePg()
    served, oos, pairs = read_oos_demand(pg, D)
    assert served == {"camp_201": 120, "direct": 0} and oos == {"camp_201": 30, "direct": 0}
    assert pairs == {("store_a", "p1"): (7, 4)}
    assert read_prices(pg) == {"p1": Decimal("100.00")}
    assert all("live_oos_demand_minute" in s or "dim_product_current" in s for s in pg.sql)
    assert not any("silver" in s.lower() for s in pg.sql)


def test_build_script_replaces_the_day_rows_of_both_marts_in_one_transaction(monkeypatch):
    """M4: a campaign that is no longer produced must not keep its old row."""
    from datetime import date
    from scripts import build_d1_marketing as script

    class Pg:
        def __init__(self):
            self.replaced, self.upserted, self.closed = [], [], False

        def replace_for_date(self, table, date_col, day, rows, keys):
            self.replaced.append((table, date_col, day, list(rows), keys))
            return len(rows)

        def upsert(self, *a):
            self.upserted.append(a)

        def close(self):
            self.closed = True

    pg, dbx = Pg(), Pg()
    monkeypatch.setattr(script, "get_store", lambda kind: dbx if kind == "databricks" else pg)
    monkeypatch.setattr(script.d1, "read_spend_rows", lambda s, d: [])
    monkeypatch.setattr(script.d1, "read_oos_demand", lambda s, d: ({}, {}, {}))
    monkeypatch.setattr(script.d1, "read_prices", lambda s: {})
    monkeypatch.setattr(script.d1, "assemble_campaign_rows", lambda *a: [{"report_date": a[0], "campaign_id": "c1"}])
    monkeypatch.setattr(script.d1, "assemble_product_rows",
                        lambda *a: [{"report_date": a[0], "store_id": "s", "product_id": "p"}])
    assert script.main(["--date", "2026-10-04"]) == 0
    assert [(t, c, d, k) for t, c, d, _, k in pg.replaced] == [
        ("marketing_d1_campaign", "report_date", date(2026, 10, 4), ["report_date", "campaign_id"]),
        ("marketing_d1_product", "report_date", date(2026, 10, 4), ["report_date", "store_id", "product_id"])]
    assert pg.upserted == [] and pg.closed and dbx.closed
