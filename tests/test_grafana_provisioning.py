"""G2: Grafana provisioning files, dashboard JSON, alert rules, compose service (spec 17.18 to 17.19)."""
import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
PROV = ROOT / "monitoring" / "grafana" / "provisioning"
DASH = ROOT / "monitoring" / "grafana" / "dashboards" / "pipeline-live.json"
CONTRACT_TABLES = {"live_metrics_minute", "pipeline_health_minute", "pipeline_batch_log", "pipeline_heartbeat",
                   "live_stock_now", "live_oos_demand_minute", "dim_store", "dim_store_district", "dim_product_current",
                   "live_express_orders", "live_stock_store_minute"}
REQUIRED_PANELS = [
    "Events per minute (all countries)", "Events, last full minute", "Active sessions per minute",
    "Orders placed per minute", "Product views and add-to-cart per minute", "Late-event rate % (5 min window)",
    "Quarantine rows per hour (rolling 60 min)", "Quarantine share % (5 min window)",
    "Seconds since Silver job last reported", "Seconds since last Gold update", "Seconds since last publish",
    "Alert rules and what to do",
]


def yaml_files():
    return sorted(PROV.rglob("*.yaml"))


def dashboard():
    return json.loads(DASH.read_text(encoding="utf-8"))


def sql_of(panel):
    return [t["rawSql"] for t in panel.get("targets", []) if "rawSql" in t]


def tables_in(sql):
    return set(re.findall(r"\b(?:FROM|JOIN)\s+([a-z_][a-z0-9_.]*)(?![a-z0-9_.]*\s*\()", sql, flags=re.I))


def test_provisioning_yaml_files_exist_and_parse():
    names = {p.name for p in yaml_files()}
    assert {"postgres.yaml", "dashboards.yaml", "alerting.yaml"} <= names
    for p in yaml_files():
        assert yaml.safe_load(p.read_text(encoding="utf-8"))["apiVersion"] == 1


def test_datasource():
    ds = yaml.safe_load((PROV / "datasources" / "postgres.yaml").read_text())["datasources"][0]
    assert ds["type"] == "postgres" and ds["uid"] == "fs-postgres"
    assert ds["url"] == "postgres:5432" and ds["user"] == "grafana_ro"
    assert ds["jsonData"]["database"] == "${POSTGRES_DB}"
    assert ds["jsonData"]["sslmode"] == "disable" and ds["jsonData"]["timescaledb"] is False
    assert ds["secureJsonData"]["password"] == "${GRAFANA_DB_PASSWORD}"


def test_dashboard_provider_points_at_mounted_folder():
    prov = yaml.safe_load((PROV / "dashboards" / "dashboards.yaml").read_text())["providers"][0]
    assert prov["options"]["path"] == "/var/lib/grafana/dashboards"


def test_dashboard_header():
    d = dashboard()
    assert d["uid"] == "fs-live" and d["title"] == "FunnelStream: live pipeline"
    assert d["refresh"] == "5s"
    assert d["time"] == {"from": "now-30m", "to": "now"}
    assert d["timezone"] == "Africa/Cairo"


def test_dashboard_has_required_panels_and_unique_ids():
    panels = dashboard()["panels"]
    titles = [p["title"] for p in panels]
    for t in REQUIRED_PANELS:
        assert t in titles, t
    assert len({p["id"] for p in panels}) == len(panels)
    assert {"stat", "timeseries", "gauge", "text"} <= {p["type"] for p in panels}


def test_every_panel_uses_the_postgres_datasource():
    for p in dashboard()["panels"]:
        if p["type"] == "text":
            continue
        assert p["datasource"]["uid"] == "fs-postgres", p["title"]
        for t in p["targets"]:
            assert t["datasource"]["uid"] == "fs-postgres", p["title"]
        assert p["fieldConfig"]["defaults"].get("unit"), p["title"]


def test_every_query_reads_only_contract_tables():
    seen = set()
    for p in dashboard()["panels"]:
        for sql in sql_of(p):
            tabs = tables_in(sql)
            assert tabs, (p["title"], sql)
            assert tabs <= CONTRACT_TABLES, (p["title"], tabs)
            seen |= tabs
    assert {"live_metrics_minute", "pipeline_health_minute", "pipeline_heartbeat"} <= seen


def test_timeseries_queries_use_grafana_time_macros():
    for p in dashboard()["panels"]:
        if p["type"] == "timeseries":
            assert all("$__timeFilter" in s for s in sql_of(p)), p["title"]


def test_threshold_lines_and_freshness_thresholds():
    by = {p["title"]: p for p in dashboard()["panels"]}

    def steps(title):
        return [s["value"] for s in by[title]["fieldConfig"]["defaults"]["thresholds"]["steps"]]

    assert steps("Late-event rate % (5 min window)") == [None, 5]
    assert steps("Quarantine share % (5 min window)") == [None, 2]
    assert steps("Seconds since Silver job last reported") == [None, 30, 120]
    assert steps("Seconds since last Gold update") == [None, 86400, 172800]
    assert steps("Seconds since last publish") == [None, 86400, 172800]
    assert "component = 'silver_job'" in sql_of(by["Seconds since Silver job last reported"])[0]
    assert "component = 'gold_build'" in sql_of(by["Seconds since last Gold update"])[0]
    assert "component = 'publish'" in sql_of(by["Seconds since last publish"])[0]


def alerting():
    return yaml.safe_load((PROV / "alerting" / "alerting.yaml").read_text())


def test_alert_rules():
    group = alerting()["groups"][0]
    assert group["interval"] == "1m"
    rules = {r["uid"]: r for r in group["rules"]}
    assert set(rules) == {"fs-late-rate", "fs-quarantine-share", "fs-no-events", "fs-silver-silent",
                          "fs-oos-demand", "fs-express-sla", "fs-express-ontime", "fs-stock-outs"}
    assert len(group["rules"]) == 8

    def threshold(r):
        c = next(d for d in r["data"] if d["refId"] == "C")["model"]["conditions"][0]["evaluator"]
        return c["type"], c["params"][0]

    def sql(r):
        return next(d for d in r["data"] if d["refId"] == "A")["model"]["rawSql"]

    late, quar, none = rules["fs-late-rate"], rules["fs-quarantine-share"], rules["fs-no-events"]
    assert threshold(late) == ("gt", 5) and "interval '5 minutes'" in sql(late) and "sum(events) >= 20" in sql(late)
    assert threshold(quar) == ("gt", 2) and "interval '5 minutes'" in sql(quar) and ">= 20" in sql(quar)
    assert threshold(none) == ("lt", 1) and "interval '5 minutes'" in sql(none)
    silent = rules["fs-silver-silent"]
    assert threshold(silent) == ("lt", 1) and "interval '2 minutes'" in sql(silent)
    assert "component = 'silver_job'" in sql(silent) and "pipeline_heartbeat" in sql(silent)
    assert late["noDataState"] == "OK" and quar["noDataState"] == "OK" and none["noDataState"] == "Alerting"
    assert silent["noDataState"] == "Alerting"
    oos = rules["fs-oos-demand"]
    assert threshold(oos) == ("gt", 0) and oos["noDataState"] == "OK"
    assert "interval '3 minutes'" in sql(oos) and "count(*) >= 2" in sql(oos)
    assert "oos_at_write" in sql(oos) and "sum(add_to_cart) AS carts" in sql(oos) and "carts >= 5" in sql(oos)  # per minute, flagged
    exp = rules["fs-express-sla"]
    assert threshold(exp) == ("gt", 0) and exp["noDataState"] == "OK" and exp["for"] == "0s"
    assert exp["labels"]["severity"] == "critical"
    for needle in ["promised_minutes", "live_express_orders", "closed_without_delivery", "interval '1 minute'",
                   "interval '60 minutes'", "delivered_at - created_at > promised_minutes"]:
        assert needle in sql(exp), needle
    assert tables_in(sql(exp)) == {"live_express_orders"}
    assert exp["annotations"]["summary"] == "An express order missed its delivery promise"
    assert "table 'Slowest express orders'" in exp["annotations"]["runbook"]
    for r in rules.values():
        assert r["labels"]["severity"] and r["annotations"]["runbook"] and r["annotations"]["summary"]
        assert r["condition"] == "C"
        assert tables_in(sql(r)) <= CONTRACT_TABLES


def test_contact_point_and_policy():
    a = alerting()
    cp = a["contactPoints"][0]
    assert cp["name"] == "fs-webhook"
    assert cp["receivers"][0]["type"] == "webhook"
    assert cp["receivers"][0]["settings"]["url"] == "http://host.docker.internal:9099/alert"
    assert a["policies"][0]["receiver"] == "fs-webhook"


def test_alert_file_notes_history_mode_limit():
    assert "history-mode" in (PROV / "alerting" / "alerting.yaml").read_text().lower()


def test_compose_grafana_service():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    g = compose["services"]["grafana"]
    assert re.fullmatch(r"grafana/grafana-oss:\d+\.\d+\.\d+", g["image"])
    assert g["mem_limit"] == "512m"
    assert "127.0.0.1:3000:3000" in g["ports"]  # loopback only
    assert "profiles" not in g
    assert "postgres" in (g["depends_on"] if isinstance(g["depends_on"], list) else g["depends_on"].keys())
    env = g["environment"]
    assert env["GF_SECURITY_ADMIN_USER"] == "admin"
    assert env["GF_SECURITY_ADMIN_PASSWORD"] == "${GRAFANA_ADMIN_PASSWORD}"
    assert env["GF_USERS_ALLOW_SIGN_UP"] == "false"
    assert env["GF_DASHBOARDS_MIN_REFRESH_INTERVAL"] == "5s"
    assert env["GF_AUTH_ANONYMOUS_ENABLED"] == "true" and env["GF_AUTH_ANONYMOUS_ORG_ROLE"] == "Viewer"
    assert env["GRAFANA_DB_PASSWORD"] == "${GRAFANA_DB_PASSWORD}"
    assert "grafanadata:/var/lib/grafana" in g["volumes"]
    assert "./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro" in g["volumes"]
    assert "./monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro" in g["volumes"]
    assert "host.docker.internal:host-gateway" in g["extra_hosts"]
    assert g["restart"] == "unless-stopped"
    assert "grafanadata" in compose["volumes"]


def test_no_password_literal_in_committed_grafana_files():
    files = list((ROOT / "monitoring").rglob("*")) + [ROOT / "docker-compose.yml", ROOT / "scripts" / "alert_sink.py"]
    for f in files:
        if not f.is_file() or "__pycache__" in f.parts:  # monitoring/ is now a Python package
            continue
        text = f.read_text(encoding="utf-8")
        for m in re.finditer(r"(?i)\bpassword\b[\"']?\s*[:=]\s*(\S+)", text):
            value = m.group(1).strip("\"'")
            assert value.startswith("$"), (f.name, m.group(0))


def test_gitignore_covers_logs():
    assert "logs/" in (ROOT / ".gitignore").read_text().split()
