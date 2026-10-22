import json
import re
from pathlib import Path

from monitoring.dashboards import build_inventory_daily, build_inventory_ops, build_marketing_d1

ROOT = Path(__file__).parent.parent
DASH = ROOT / "monitoring" / "grafana" / "dashboards" / "inventory-ops.json"
ALLOWED = {"live_stock_now", "live_oos_demand_minute", "live_metrics_minute", "dim_store", "dim_store_district",
           "dim_product_current", "live_express_orders"}


def sqls(d):
    return [t["rawSql"] for p in d["panels"] for t in p.get("targets", [])]


def test_the_file_is_what_the_builder_produces():
    assert json.loads(DASH.read_text(encoding="utf-8")) == build_inventory_ops()


def test_eleven_data_panels_a_row_two_notes_and_unique_ids():
    d = build_inventory_ops()
    ids = [p["id"] for p in d["panels"]]
    assert len(ids) == len(set(ids)) and d["uid"] == "fs-inventory-ops"
    assert sorted(p["type"] for p in d["panels"] if p["type"] not in ("text", "row")) ==         ["stat"] * 7 + ["table"] * 3 + ["timeseries"] * 2
    notes = [p for p in d["panels"] if p["type"] == "text"]
    assert len(notes) == 2 and "simulated" in notes[0]["options"]["content"].lower()
    [row] = [p for p in d["panels"] if p["type"] == "row"]
    assert row["title"] == "Express delivery, 20-minute promise"


def test_every_panel_uses_the_postgres_datasource_and_only_the_live_tables():
    d = build_inventory_ops()
    for p in d["panels"]:
        if p["type"] in ("text", "row"):
            continue
        assert p["datasource"]["uid"] == "fs-postgres"
    for sql in sqls(d):
        tables = {m.lower() for m in re.findall(r"(?:from|join)\s+([a-z_]+)", sql, re.I)}
        assert tables and tables <= ALLOWED, (sql, tables)


def test_the_stat_thresholds_follow_the_design():
    d = build_inventory_ops()
    steps = {p["title"]: [s["value"] for s in p["fieldConfig"]["defaults"]["thresholds"]["steps"]]
             for p in d["panels"] if p["type"] == "stat"}
    assert steps["Out-of-stock products now"] == [None, 1, 5]
    assert steps["Seconds since the last stock change"] == [None, 30, 60]
    assert steps["Express orders at risk now"] == [None, 1, 3]
    assert steps["Express breaches, last hour"] == [None, 1]
    assert steps["On-time rate, last hour"] == [None, 90, 95]
    assert steps["Slowest express delivery, last hour (minutes)"] == [None, 15, 20]
    colors = {p["title"]: [s["color"] for s in p["fieldConfig"]["defaults"]["thresholds"]["steps"]]
              for p in d["panels"] if p["type"] == "stat"}
    assert colors["Express orders at risk now"] == ["green", "orange", "red"]
    assert colors["Express breaches, last hour"] == ["green", "red"]  # any breach is red
    assert colors["On-time rate, last hour"] == ["red", "orange", "green"]  # the 95% target, as on the SLA board
    assert next(p for p in d["panels"] if p["title"] == "On-time rate, last hour")["fieldConfig"]["defaults"]["unit"]         == "percent"


def test_time_series_queries_use_the_grafana_time_macros():
    for p in build_inventory_ops()["panels"]:
        if p["type"] == "timeseries":
            assert "$__timeFilter(" in p["targets"][0]["rawSql"] and "$__timeGroupAlias(" in p["targets"][0]["rawSql"]


def test_out_of_stock_demand_panels_filter_on_the_flag():
    # the table holds all served demand; only oos_at_write rows are out-of-stock demand
    d = build_inventory_ops()
    for p in d["panels"]:
        for t in p.get("targets", []):
            if "live_oos_demand_minute" in t["rawSql"]:
                assert "oos_at_write" in t["rawSql"], p["title"]


def express_sql(title):
    return next(p for p in build_inventory_ops()["panels"] if p["title"] == title)["targets"][0]["rawSql"]


def test_the_express_panels_read_only_the_express_table_and_the_store_dimension():
    titles = ["Express orders at risk now", "Express breaches, last hour", "On-time rate, last hour",
              "Slowest express delivery, last hour (minutes)", "Slowest express orders, last hour"]
    for t in titles:
        sql = express_sql(t)
        tables = {m.lower() for m in re.findall(r"(?:from|join)\s+([a-z_]+)", sql, re.I)}
        assert tables <= {"live_express_orders", "dim_store"} and "live_express_orders" in tables, t
        if not t.startswith("Slowest express delivery"):  # elapsed time alone needs no promise
            assert "promised_minutes" in sql, t  # always the order's own promise, never a constant 20
        assert "interval '20" not in sql and "* 20" not in sql, t


def test_the_express_definitions_are_the_agreed_ones():
    risk = express_sql("Express orders at risk now")
    assert risk == ("SELECT count(*) AS \"orders\" FROM live_express_orders WHERE delivered_at IS NULL AND NOT "
                    "closed_without_delivery AND now() - created_at >= promised_minutes * interval '45 seconds' AND "
                    "now() - created_at <= promised_minutes * interval '1 minute' + interval '1 minute'")
    for t in ["Express breaches, last hour", "On-time rate, last hour"]:
        sql = express_sql(t)
        assert "promised_minutes * interval '1 minute' + interval '1 minute'" in sql  # one minute of grace
        assert "NOT closed_without_delivery" in sql  # a closed order is never "late"
    assert "interval '1 hour'" in express_sql("Express breaches, last hour")
    rate = express_sql("On-time rate, last hour")
    assert "nullif" in rate and "FILTER" in rate and "delivered_at - created_at <= promised_minutes" in rate
    table = express_sql("Slowest express orders, last hour")
    assert "LIMIT 10" in table and "DESC" in table and "dim_store" in table
    # an order of a store missing from dim_store still shows, under its id
    assert "LEFT JOIN dim_store s ON s.store_id = o.store_id" in table
    assert "coalesce(s.store_name, o.store_id) AS \"store\"" in table
    for state in ["on time", "at risk", "breached", "closed without delivery"]:
        assert "'%s'" % state in table
    text = [p for p in build_inventory_ops()["panels"] if p["type"] == "text"][1]["options"]["content"]
    assert "on time" in text.lower() and "breached" in text.lower() and "grace" in text.lower()


# ---- the daily inventory page -------------------------------------------------------------------------------------

DAILY = ROOT / "monitoring" / "grafana" / "dashboards" / "inventory-daily.json"
DAILY_ALLOWED = {"inventory_daily", "oos_demand_daily", "express_daily", "dim_store", "dim_product_current"}


def daily_panel(title):
    return next(p for p in build_inventory_daily()["panels"] if p["title"] == title)


def test_the_daily_file_is_what_the_builder_produces():
    assert json.loads(DAILY.read_text(encoding="utf-8")) == build_inventory_daily()


def test_the_daily_page_has_unique_ids_the_right_uid_and_a_range_of_days():
    d = build_inventory_daily()
    ids = [p["id"] for p in d["panels"]]
    assert len(ids) == len(set(ids)) and d["uid"] == "fs-inventory-daily"
    assert d["title"] == "FunnelStream: Inventory insights, daily"
    assert d["time"]["from"] != "now-30m"  # a daily page needs days, not minutes
    assert sorted(p["type"] for p in d["panels"]) == ["row"] + ["stat"] * 5 + ["table", "text"] + ["timeseries"] * 4
    note = next(p for p in d["panels"] if p["type"] == "text")["options"]["content"].lower()
    assert "simulated" in note and "lower bound" in note and "reserve-on-order" in note


def test_every_daily_panel_uses_the_postgres_datasource_and_only_the_daily_tables():
    d = build_inventory_daily()
    for p in d["panels"]:
        if p["type"] not in ("text", "row"):
            assert p["datasource"]["uid"] == "fs-postgres", p["title"]
    for sql in sqls(d):
        tables = {m.lower() for m in re.findall(r"(?:from|join)\s+([a-z_]+)", sql, re.I)}
        assert tables and tables <= DAILY_ALLOWED, (sql, tables)


def test_the_daily_stats_and_their_thresholds():
    d = build_inventory_daily()
    stats = {p["title"]: p for p in d["panels"] if p["type"] == "stat"}
    assert set(stats) == {"Closing stock, latest day (units)", "Products out of stock at day end",
                          "Oversold units, latest day", "Shrinkage share of units out, latest day",
                          "Express on-time rate, latest day"}

    def steps(title):
        return [s["value"] for s in stats[title]["fieldConfig"]["defaults"]["thresholds"]["steps"]]
    assert steps("Products out of stock at day end") == [None, 1, 5]
    assert steps("Oversold units, latest day") == [None, 1, 20]
    assert steps("Shrinkage share of units out, latest day") == [None, 3, 6]
    assert stats["Shrinkage share of units out, latest day"]["fieldConfig"]["defaults"]["unit"] == "percent"
    rate = stats["Express on-time rate, latest day"]["fieldConfig"]["defaults"]
    assert rate["unit"] == "percent" and [s["value"] for s in rate["thresholds"]["steps"]] == [None, 90, 95]
    assert [s["color"] for s in rate["thresholds"]["steps"]] == ["red", "orange", "green"]  # the 95% target
    for p in stats.values():
        assert "(SELECT max(report_date) FROM" in p["targets"][0]["rawSql"]  # always the latest day


def test_the_daily_time_series_use_the_time_filter():
    series = [p for p in build_inventory_daily()["panels"] if p["type"] == "timeseries"]
    assert [p["title"] for p in series] == [
        "Closing stock by store (units)", "Units sold, received and shrunk per day", "Demand on empty shelves per day",
        "Slowest express delivery per day (minutes)"]
    for p in series:
        assert "$__timeFilter(" in p["targets"][0]["rawSql"], p["title"]


def test_the_daily_express_panels_read_the_express_table():
    on_time = daily_panel("Express on-time rate, latest day")["targets"][0]["rawSql"]
    assert on_time == ("SELECT coalesce(round(100.0 * sum(on_time) / nullif(sum(on_time) + sum(breached), 0), 1), 100) "
                       "AS \"percent\" FROM express_daily WHERE report_date = (SELECT max(report_date) FROM express_daily)")
    slow = daily_panel("Slowest express delivery per day (minutes)")["targets"][0]["rawSql"]
    assert "FROM express_daily" in slow and "max(max_delivery_minutes) AS \"slowest\"" in slow
    assert "20 AS \"promise\"" in slow


def test_the_demand_on_empty_shelves_panel_reads_the_flagged_columns():
    sql = daily_panel("Demand on empty shelves per day")["targets"][0]["rawSql"]
    assert "sum(oos_views)" in sql and "sum(oos_add_to_cart)" in sql and "served_" not in sql


def test_the_days_of_cover_table_shows_names_and_the_lowest_cover_first():
    p = daily_panel("Lowest days of cover, latest day (20 products)")
    sql = p["targets"][0]["rawSql"]
    assert p["type"] == "table" and "LIMIT 20" in sql and "NULLS LAST" in sql
    assert "LEFT JOIN dim_product_current" in sql and "coalesce(p.name, i.product_id)" in sql


# ---- the marketing D-1 page ---------------------------------------------------------------------------------------

MARKETING = ROOT / "monitoring" / "grafana" / "dashboards" / "marketing-d1.json"
MARKETING_ALLOWED = {"marketing_d1_campaign", "marketing_d1_product", "dim_store", "dim_product_current"}


def marketing_panel(title):
    return next(p for p in build_marketing_d1()["panels"] if p["title"] == title)


def test_the_marketing_file_is_what_the_builder_produces():
    assert json.loads(MARKETING.read_text(encoding="utf-8")) == build_marketing_d1()


def test_the_marketing_page_has_six_stats_two_tables_a_note_and_unique_ids():
    d = build_marketing_d1()
    ids = [p["id"] for p in d["panels"]]
    assert len(ids) == len(set(ids)) and d["uid"] == "fs-marketing-d1"
    assert d["title"] == "FunnelStream: Marketing, D-1"
    assert sorted(p["type"] for p in d["panels"]) == ["stat"] * 6 + ["table"] * 2 + ["text"]


def test_every_marketing_panel_uses_the_postgres_datasource_and_only_the_mart_tables():
    d = build_marketing_d1()
    for p in d["panels"]:
        if p["type"] != "text":
            assert p["datasource"]["uid"] == "fs-postgres", p["title"]
    for sql in sqls(d):
        tables = {m.lower() for m in re.findall(r"(?:from|join)\s+([a-z_0-9]+)", sql, re.I)}
        assert tables and tables <= MARKETING_ALLOWED, (sql, tables)


def test_the_marketing_cards_read_the_latest_day_and_the_exposed_spend_definition():
    for p in build_marketing_d1()["panels"]:
        if p["type"] != "text":
            assert "(SELECT max(report_date) FROM marketing_d1_" in p["targets"][0]["rawSql"], p["title"]
    exposed = marketing_panel("Exposed spend (EGP)")["targets"][0]["rawSql"]
    assert "sum(exposed_spend)" in exposed
    share = marketing_panel("Out-of-stock click share")
    assert share["fieldConfig"]["defaults"]["unit"] == "percentunit"
    assert "sum(oos_clicks)::numeric / nullif(sum(served_clicks), 0)" in share["targets"][0]["rawSql"]
    assert "sum(revenue_delivered)" in marketing_panel("ROAS delivered (provisional)")["targets"][0]["rawSql"]


def test_the_marketing_tables_sort_and_limit_as_agreed():
    camp = marketing_panel("Campaigns, sorted by exposed spend")["targets"][0]["rawSql"]
    assert "ORDER BY exposed_spend DESC NULLS LAST" in camp and "LIMIT" not in camp
    top = marketing_panel("Top 5 products by revenue at risk")["targets"][0]["rawSql"]
    assert "ORDER BY m.revenue_at_risk DESC NULLS LAST LIMIT 5" in top and "JOIN dim_store s" in top


def test_the_marketing_note_states_the_day_rule_and_never_says_upper_bound():
    d = build_marketing_d1()
    note = next(p for p in d["panels"] if p["type"] == "text")["options"]["content"].lower()
    assert "simulated" in note and "estimate" in note and "cairo day" in note and "utc" not in note
    assert "provisional" in note
    for p in d["panels"]:
        assert "upper bound" not in (p.get("description", "") + p.get("title", "")
                                     + p.get("options", {}).get("content", "")).lower()
