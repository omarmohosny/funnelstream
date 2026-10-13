"""Express SLA and Live stock monitor dashboards, their alert rules and the stock history writer."""
import json
import re
from pathlib import Path

import yaml

from monitoring.live_dashboards import SLA_TARGET, build_express_sla, build_live_stock
from warehouse.live_stock import write_stock_history

ROOT = Path(__file__).parent.parent
DASH = ROOT / "monitoring" / "grafana" / "dashboards"
ALERTS = ROOT / "monitoring" / "grafana" / "provisioning" / "alerting" / "alerting.yaml"
ALLOWED = {"live_stock_now", "live_stock_store_minute", "live_oos_demand_minute", "dim_store", "dim_product_current",
           "live_express_orders"}


def data_panels(d):
    return [p for p in d["panels"] if p["type"] not in ("text", "row")]


def test_files_equal_their_builders():
    assert json.loads((DASH / "express-sla.json").read_text(encoding="utf-8")) == build_express_sla()
    assert json.loads((DASH / "live-stock.json").read_text(encoding="utf-8")) == build_live_stock()


def test_ids_are_unique_the_store_variable_exists_and_every_query_filters_by_it():
    for d, uid in ((build_express_sla(), "fs-express-sla"), (build_live_stock(), "fs-live-stock")):
        ids = [p["id"] for p in d["panels"]]
        assert len(ids) == len(set(ids)) and d["uid"] == uid
        [var] = d["templating"]["list"]
        assert var["name"] == "store" and var["multi"] and var["includeAll"]
        for p in data_panels(d):
            assert p["datasource"]["uid"] == "fs-postgres"
            for t in p["targets"]:
                tables = {m.lower() for m in re.findall(r"(?:from|join)\s+([a-z_]+)", t["rawSql"], re.I)}
                assert tables and tables <= ALLOWED, (p["title"], tables)
                if p["title"] != "Seconds since the last stock write":
                    assert "$store" in t["rawSql"], p["title"]


def test_the_express_dashboard_shows_the_target_and_the_sla_panels():
    d = build_express_sla()
    titles = [p["title"] for p in d["panels"]]
    for needed in ("On-time rate, last hour", "Breaches, last hour", "SLA by store, last hour",
                   "Delivery time p50 and p90 (minutes)", "Open orders closest to their promise"):
        assert needed in titles, needed
    rate = next(p for p in d["panels"] if p["title"].startswith("On-time rate by order creation time"))
    steps = rate["fieldConfig"]["defaults"]["thresholds"]["steps"]
    assert steps[-1]["value"] == SLA_TARGET == 95 and rate["fieldConfig"]["defaults"]["custom"]["thresholdsStyle"]
    assert d["time"]["from"] == "now-3h"  # live_express_orders keeps three hours


def test_the_stock_dashboard_has_history_and_a_watchlist():
    d = build_live_stock()
    titles = [p["title"] for p in d["panels"]]
    for needed in ("Out-of-stock products per store", "Units on hand per store", "Units added and removed per minute",
                   "Stock health by store", "Restock watchlist", "In-stock rate"):
        assert needed in titles, needed
    history = [t["rawSql"] for p in d["panels"] for t in p.get("targets", []) if "live_stock_store_minute" in t["rawSql"]]
    assert len(history) == 3


def alerting():
    return {r["uid"]: r for g in yaml.safe_load(ALERTS.read_text())["groups"] for r in g["rules"]}


def test_the_two_new_alert_rules():
    rules = alerting()
    ontime, outs = rules["fs-express-ontime"], rules["fs-stock-outs"]

    def sql(r):
        return next(x for x in r["data"] if x["refId"] == "A")["model"]["rawSql"]

    def threshold(r):
        c = next(x for x in r["data"] if x["refId"] == "C")["model"]["conditions"][0]["evaluator"]
        return c["type"], c["params"][0]

    assert threshold(ontime) == ("lt", SLA_TARGET) and "HAVING count(*) >= 5" in sql(ontime)
    assert "interval '30 minutes'" in sql(ontime) and ontime["noDataState"] == "OK"
    assert "table 'SLA by store'" in ontime["annotations"]["runbook"]
    assert threshold(outs) == ("gt", 20) and "live_stock_now" in sql(outs) and outs["for"] == "1m"
    assert "table 'Restock watchlist'" in outs["annotations"]["runbook"]


class RecordingStore:
    def __init__(self):
        self.statements = []

    def execute(self, sql, retry=True):
        self.statements.append(sql)


def test_stock_history_is_one_idempotent_snapshot_with_a_24_hour_retention():
    store = RecordingStore()
    write_stock_history(store)
    insert, prune = store.statements
    assert insert.startswith("INSERT INTO live_stock_store_minute") and "FROM live_stock_now GROUP BY store_id" in insert
    assert "date_trunc('minute', now())" in insert and "ON CONFLICT (minute_start, store_id) DO UPDATE" in insert
    assert prune == "DELETE FROM live_stock_store_minute WHERE minute_start < now() - interval '24 hours'"


ALL_BUILDERS = None


def all_dashboards():
    from monitoring import dashboards
    return [dashboards.build_inventory_ops(), dashboards.build_inventory_daily(), dashboards.build_marketing_d1(),
            build_express_sla(), build_live_stock()]


def test_a_stat_without_data_shows_a_dash_not_a_coloured_no_data():
    for d in all_dashboards():
        for p in d["panels"]:
            if p["type"] != "stat":
                continue
            defaults = p["fieldConfig"]["defaults"]
            assert defaults["noValue"] == "–", p["title"]
            assert any(m["type"] == "special" and m["options"]["match"] == "null+nan" for m in defaults["mappings"]), p["title"]


def test_rates_and_stock_levels_leave_a_gap_for_a_bucket_without_data_instead_of_zero():
    sla = {p["title"]: p for p in build_express_sla()["panels"]}
    for title in ("On-time rate by order creation time (5-minute buckets)", "Delivery time p50 and p90 (minutes)"):
        assert "'5m', NULL)" in sla[title]["targets"][0]["rawSql"], title
    stock = {p["title"]: p for p in build_live_stock()["panels"]}
    for title in ("Out-of-stock products per store", "Units on hand per store"):
        assert "'1m', NULL)" in stock[title]["targets"][0]["rawSql"], title
        assert stock[title]["fieldConfig"]["defaults"]["custom"]["spanNulls"] is True, title  # a level, joined across minutes
    # counts per bucket stay zero-filled: no events really is zero events
    assert "'5m', 0)" in sla["On-time and breached orders per 5 minutes"]["targets"][0]["rawSql"]


def test_open_express_orders_ignore_rows_older_than_the_three_hours_the_table_keeps():
    sla = {p["title"]: p for p in build_express_sla()["panels"]}
    for title in ("Open express orders", "Open orders closest to their promise"):
        assert "created_at >= now() - interval '3 hours'" in sla[title]["targets"][0]["rawSql"], title


def test_every_on_time_rate_uses_the_same_95_percent_colours():
    from monitoring import dashboards
    for d in all_dashboards():
        for p in d["panels"]:
            if p["type"] == "stat" and "on-time rate" in p["title"].lower():
                steps = p["fieldConfig"]["defaults"]["thresholds"]["steps"]
                assert [s["value"] for s in steps] == [None, SLA_TARGET - 5, SLA_TARGET], p["title"]
                assert [s["color"] for s in steps] == ["red", "orange", "green"], p["title"]


def test_the_marketing_tables_have_readable_headers_and_formats():
    from monitoring import dashboards
    d = {p["title"]: p for p in dashboards.build_marketing_d1()["panels"]}
    over = {o["matcher"]["options"]: {x["id"]: x["value"] for x in o["properties"]}
            for o in d["Campaigns, sorted by exposed spend"]["fieldConfig"]["overrides"]}
    assert over["oos_click_share"]["unit"] == "percentunit" and over["oos_click_share"]["displayName"] == "Out-of-stock share"
    assert over["spend"]["displayName"] == "Spend (EGP)" and over["utm_campaign"]["displayName"] == "UTM campaign"
    top = {o["matcher"]["options"]: {x["id"]: x["value"] for x in o["properties"]}
           for o in d["Top 5 products by revenue at risk"]["fieldConfig"]["overrides"]}
    assert top["revenue_at_risk"]["displayName"] == "Revenue at risk (EGP)"


def test_money_and_unit_counts_are_not_abbreviated_to_k():
    from monitoring import dashboards
    wanted = {"Spend (EGP)", "Units on hand", "Closing stock, latest day (units)"}
    seen = {p["title"]: p["fieldConfig"]["defaults"]["unit"] for d in all_dashboards() for p in d["panels"] if p["title"] in wanted}
    assert seen == {t: "locale" for t in wanted}


def test_no_table_alias_is_doubled_and_the_units_chart_scales_to_its_data():
    for d in all_dashboards():
        for p in d["panels"]:
            for t in p.get("targets", []):
                assert "o.o." not in t["rawSql"], p["title"]
    stock = {p["title"]: p for p in build_live_stock()["panels"]}
    assert "min" not in stock["Units on hand per store"]["fieldConfig"]["defaults"]  # auto: sales stay visible
    assert stock["Out-of-stock products per store"]["fieldConfig"]["defaults"]["min"] == 0


def test_titles_and_headers_fit_their_boxes():
    from monitoring import dashboards
    for p in dashboards.build_marketing_d1()["panels"]:
        if p["type"] == "stat":  # a w4 stat holds about 30 characters of title
            assert len(p["title"]) <= 32, p["title"]
    campaigns = next(p for p in dashboards.build_marketing_d1()["panels"] if p["title"].startswith("Campaigns"))
    headers = [x["value"] for o in campaigns["fieldConfig"]["overrides"] for x in o["properties"] if x["id"] == "displayName"]
    assert max(len(h) for h in headers) <= 20, headers
