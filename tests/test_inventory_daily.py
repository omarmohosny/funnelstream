import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from warehouse.inventory_daily import cairo_day_bounds, rollup_oos_demand_daily
from warehouse.live_tables import LIVE_TABLE_NAMES, LIVE_TABLES

DDL = (Path(__file__).parent.parent / "warehouse" / "live_ddl.sql").read_text(encoding="utf-8")


def test_the_cairo_day_in_utc_follows_the_clock_of_egypt():
    start, end = cairo_day_bounds(date(2026, 10, 10))  # Egypt is on summer time (UTC+3) until the end of October
    assert start == datetime(2026, 10, 9, 21, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 10, 10, 21, 0, tzinfo=timezone.utc)
    start, end = cairo_day_bounds(date(2026, 12, 1))  # winter time, UTC+2
    assert start == datetime(2026, 11, 30, 22, 0, tzinfo=timezone.utc)
    assert (end - start).total_seconds() == 24 * 3600


def test_the_new_tables_are_specified_created_and_granted():
    spec = {t.name: t for t in LIVE_TABLES}
    assert {"inventory_daily", "oos_demand_daily", "express_daily"} <= set(LIVE_TABLE_NAMES)
    assert len(LIVE_TABLES) == len(LIVE_TABLE_NAMES) == 16
    assert spec["inventory_daily"].keys == ("report_date", "store_id", "product_id")
    assert spec["oos_demand_daily"].keys == ("report_date", "store_id", "product_id", "campaign_id")
    assert spec["express_daily"].keys == ("report_date", "store_id")
    cols = {c.name for c in spec["oos_demand_daily"].columns}
    assert {"campaign_id", "utm_campaign", "served_clicks", "oos_clicks", "oos_views", "oos_add_to_cart"} <= cols
    assert spec["express_daily"].column_names() == [
        "report_date", "store_id", "orders", "delivered", "on_time", "breached", "avg_delivery_minutes",
        "max_delivery_minutes"]
    for name in ("inventory_daily", "oos_demand_daily", "express_daily"):
        assert re.search(r"CREATE TABLE IF NOT EXISTS %s \(" % name, DDL), name
        assert name in DDL.split("GRANT SELECT ON")[1].split("TO grafana_ro")[0], name
    body = re.search(r"CREATE TABLE IF NOT EXISTS express_daily \((.*?)\n\);", DDL, re.S).group(1)
    assert re.search(r"avg_delivery_minutes\s+numeric\(8,2\)(?!\s+NOT NULL)", body)  # NULL when nothing was delivered
    assert re.search(r"max_delivery_minutes\s+numeric\(8,2\)(?!\s+NOT NULL)", body)


class RecordingStore:
    def __init__(self):
        self.executed = []
        self.atomic_calls = 0

    def execute_atomic(self, statements):
        self.atomic_calls += 1
        self.executed.append(list(statements))


def test_the_rollup_sums_the_minutes_of_one_cairo_day_and_is_idempotent():
    store = RecordingStore()
    rollup_oos_demand_daily(store, date(2026, 10, 10))
    [(delete, sql)] = store.executed  # M4: the old day rows go first, in the same transaction
    assert delete == "DELETE FROM oos_demand_daily WHERE report_date = DATE'2026-10-10'"
    assert store.atomic_calls == 1
    assert "FROM live_oos_demand_minute" in sql and "ON CONFLICT (report_date, store_id, product_id, campaign_id)" in sql
    assert "AT TIME ZONE 'Africa/Cairo'" in sql and "DATE'2026-10-10'" in sql
    assert "FILTER (WHERE oos_at_write)" in sql  # the oos_* columns are the flagged minutes, the served_* columns all of them
    assert "DO UPDATE SET utm_campaign = EXCLUDED.utm_campaign" in sql
    assert "served_clicks = EXCLUDED.served_clicks" in sql and "oos_clicks = EXCLUDED.oos_clicks" in sql  # a total is replaced, never incremented
    rollup_oos_demand_daily(store, date(2026, 10, 10))
    assert store.executed[0] == store.executed[1]


def test_the_build_script_defaults_to_the_last_two_days_ending_yesterday():
    from scripts import build_inventory_daily as script
    today = date(2026, 10, 11)
    assert script.parse_day("today", today) == today
    assert script.parse_day("yesterday", today) == date(2026, 10, 10)
    assert script.parse_day("2026-09-01", today) == date(2026, 9, 1)
    with pytest.raises(SystemExit) as stop:
        script.main(["--help"])
    assert stop.value.code == 0
    assert script.days_to_build(date(2026, 10, 10), 2) == [date(2026, 10, 9), date(2026, 10, 10)]
    assert script.days_to_build(date(2026, 10, 10), 1) == [date(2026, 10, 10)]
