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
