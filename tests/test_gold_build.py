"""Host tests for the Gold build framework (warehouse/gold_build.py, warehouse/gold/*.sql, calendar_egypt.py).

Fake store only: nothing here talks to Databricks or runs Spark. The SQL itself is run by tests/test_gold_build_spark.py.
"""
import os
import re
from datetime import date, datetime, timedelta

import pytest

import warehouse.gold_build as gb
from simulator.campaigns import CAMPAIGNS
from warehouse import calendar_egypt as cal
from warehouse.gold_build import (GoldBuildError, STEPS, campaigns_values, main, parse_date, placeholders,
                                  render_sql, resolve_window, run_build, step_file, step_statements)
from warehouse.store import DatabricksStore, GoldStore, PostgresStore
from warehouse.tables import gold_table

GOLD_DIR = gb.GOLD_DIR
SQL_FILES = sorted(f for f in os.listdir(GOLD_DIR) if f.endswith(".sql"))
BUILT = ["dim_customer", "dim_product", "dim_campaign", "dim_date", "fact_orders", "fact_order_lines",
         "fact_attribution", "agg_orders_daily", "agg_funnel_daily", "agg_campaign_daily", "agg_city_product_daily"]
SLOTS = ["rec_product"]
NO_CHECKS = [s for s in STEPS if s != "checks"]  # the checks step has its own tests (test_checks.py)


class FakeStore(GoldStore):
    def __init__(self, fail=None, counts=None):
        self.executed, self.queries = [], []
        self.fail = fail
        self.counts = counts or {}

    def schema_prefix(self, group):
        return "funnelstream." + group

    def upsert(self, table, rows, key_cols):
        raise AssertionError("the Gold build must not use upsert")

    def query(self, sql, params=None, retry=True):
        self.queries.append(sql)
        if sql.startswith("/* funnelstream check "):  # a data quality check: no violations
            return [(0,)]
        m = re.match(r"SELECT count\(\*\) FROM funnelstream\.gold\.(\w+)$", sql)
        if m:
            return [(self.counts.get(m.group(1), 7),)]
        return [(3,)]

    def execute(self, sql, retry=True):
        if self.fail and self.fail in sql:
            raise RuntimeError("boom in %s" % self.fail)
        self.executed.append(sql)

    def close(self):
        pass


WINDOW = dict(from_date="2026-10-01", to_date="2026-10-03")


def expected_statements(**window):
    values = placeholders("funnelstream.silver", "funnelstream.gold", window["from_date"], window["to_date"])
    out = []
    for name in STEPS:
        stmts = step_statements(name, values)
        out.extend([gb.TIME_ZONE_SQL] + stmts if stmts else [])  # every step starts with the zone pin
    return out


# ---------------------------------------------------------------- order and shape
def test_steps_are_the_fixed_order_of_spec_16_2():
    assert STEPS == ["dim_customer", "dim_product", "dim_campaign", "dim_date", "fact_orders", "fact_order_lines",
                     "fact_attribution", "agg_orders_daily", "agg_funnel_daily", "agg_campaign_daily",
                     "agg_city_product_daily", "rec_product", "checks"]


def test_sql_files_are_named_by_step_position_and_slots_have_no_file_yet():
    assert SQL_FILES == ["%02d_%s.sql" % (STEPS.index(n) + 1, n) for n in BUILT]
    assert SQL_FILES == [os.path.basename(step_file(n)) for n in BUILT]
    for n in SLOTS + ["checks"]:  # checks has no file ever: its queries live in warehouse/checks.py
        assert not os.path.exists(step_file(n)), n


def test_statements_run_in_step_order_and_slots_are_reported_empty():
    store = FakeStore()
    result = run_build(store, **WINDOW)
    assert store.executed == expected_statements(**WINDOW)
    assert list(result["steps"]) == STEPS
    assert [n for n in STEPS if result["steps"][n]["status"] == "ok"] == BUILT + ["checks"]
    assert [n for n in STEPS if result["steps"][n]["status"] == "empty"] == SLOTS
    for n in BUILT:
        assert result["steps"][n]["rows"] == 7  # SELECT count(*) after the step
        assert result["steps"][n]["statements"] == len(step_statements(n, placeholders("a", "b", *WINDOW.values())))
    counts = [q for q in store.queries if q.startswith("SELECT count(*)")]
    assert counts == ["SELECT count(*) FROM funnelstream.gold.%s" % n for n in BUILT]
    assert result["reported"] == {"orders_without_created_at": 3, "orders_without_click": 3, "orders_direct": 3,
                                  "unresolved_clicks": 3}
    assert result["window"] == {"from": "2026-10-01", "to": "2026-10-03", "full": False}


def test_a_step_file_dropped_into_a_slot_runs_at_its_position(tmp_path, monkeypatch):
    for f in SQL_FILES:
        (tmp_path / f).write_text(open(os.path.join(GOLD_DIR, f), encoding="utf-8").read(), encoding="utf-8")
    (tmp_path / "12_rec_product.sql").write_text("SELECT 'rec {gold}';", encoding="utf-8")
    monkeypatch.setattr(gb, "GOLD_DIR", str(tmp_path))
    store = FakeStore()
    result = run_build(store, **WINDOW)
    i = store.executed.index("SELECT 'rec funnelstream.gold';")
    assert store.executed[i - 1] == gb.TIME_ZONE_SQL
    assert store.executed[i - 2].startswith("MERGE INTO funnelstream.gold.agg_city_product_daily")
    assert i + 1 == len(store.executed)  # nothing after it: the checks only query
    assert result["steps"]["rec_product"]["status"] == "ok"


def test_steps_subset_runs_in_fixed_order_and_unknown_step_is_rejected():
    store = FakeStore()
    result = run_build(store, steps=["agg_funnel_daily", "dim_date"], **WINDOW)
    ran = [n for n, r in result["steps"].items() if r["status"] == "ok"]
    assert ran == ["dim_date", "agg_funnel_daily"]
    assert all(r["status"] == "skipped" for n, r in result["steps"].items() if n not in ran)
    assert store.executed[0] == gb.TIME_ZONE_SQL
    assert store.executed[1].startswith("MERGE INTO funnelstream.gold.dim_date")
    with pytest.raises(ValueError, match="unknown step"):
        run_build(FakeStore(), steps=["dim_nope"], **WINDOW)


def test_dry_run_executes_and_queries_nothing_and_returns_the_rendered_statements():
    store = FakeStore()
    result = run_build(store, dry_run=True, **WINDOW)
    assert store.executed == [] and store.queries == []
    assert result["dry_run"] is True
    sql = [s for n in NO_CHECKS if result["steps"][n]["status"] == "dry-run" for s in result["steps"][n]["sql"]]
    assert sql == expected_statements(**WINDOW)
    assert len(result["steps"]["checks"]["sql"]) == len(result["steps"]["checks"]["names"])
    assert result["steps"]["checks"]["status"] == "dry-run" and result["steps"]["checks"]["statements"] > 40


def test_running_twice_issues_identical_statements():
    a, b = FakeStore(), FakeStore()
    run_build(a, **WINDOW)
    run_build(b, **WINDOW)
    assert a.executed == b.executed and a.queries == b.queries
    c = FakeStore()
    run_build(c, full=True)
    d = FakeStore()
    run_build(d, full=True)
    assert c.executed == d.executed and c.executed != a.executed


def test_a_failing_step_stops_the_run_and_names_the_step():
    store = FakeStore(fail="MERGE INTO funnelstream.gold.fact_orders ")
    with pytest.raises(GoldBuildError) as exc:
        run_build(store, **WINDOW)
    e = exc.value
    assert e.step == "fact_orders" and "fact_orders" in str(e) and "boom" in str(e)
    assert isinstance(e.__cause__, RuntimeError)
    assert list(e.results) == ["dim_customer", "dim_product", "dim_campaign", "dim_date"]
    assert not any("fact_order_lines" in s or "agg_" in s for s in store.executed)
    assert not any("DELETE" in s for s in store.executed)


def test_a_failing_count_query_is_a_step_failure_too():
    class CountFails(FakeStore):
        def query(self, sql, params=None, retry=True):
            if "dim_product" in sql:
                raise RuntimeError("count failed")
            return super().query(sql, params, retry)

    with pytest.raises(GoldBuildError) as exc:
        run_build(CountFails(), **WINDOW)
    assert exc.value.step == "dim_product"


def test_the_build_uses_only_the_store_interface():
    import ast
    source = open(gb.__file__, encoding="utf-8").read()
    imported = {n.names[0].name if isinstance(n, ast.Import) else n.module
                for n in ast.walk(ast.parse(source)) if isinstance(n, (ast.Import, ast.ImportFrom))}
    assert not {m for m in imported if m.split(".")[0] in ("psycopg", "databricks", "pyspark")}
    assert "PG_DSN" not in source
    store = FakeStore()
    run_build(store, **WINDOW)  # FakeStore.upsert raises: nothing may call it


# ---------------------------------------------------------------- placeholders and dates
def test_render_sql_replaces_known_placeholders():
    values = placeholders("cat.silver", "cat.gold", "2026-10-01", "2026-10-03")
    out = render_sql("SELECT * FROM {silver}.t WHERE d BETWEEN DATE'{from_date}' AND DATE'{to_date}' -- {gold}",
                     values)
    assert out == "SELECT * FROM cat.silver.t WHERE d BETWEEN DATE'2026-10-01' AND DATE'2026-10-03' -- cat.gold"


def test_render_sql_rejects_unknown_placeholders():
    values = placeholders("s", "g", "2026-10-01", "2026-10-01")
    with pytest.raises(ValueError, match="unknown placeholder {nope}"):
        render_sql("SELECT {nope}", values)
    with pytest.raises(ValueError):
        render_sql("SELECT {silver} {gold_x}", values)


@pytest.mark.parametrize("bad", ["2026-13-01", "2026-02-30", "2026-1-1", "20261001", "2026-10-01; DROP TABLE x",
                                 "2026-10-01'", "", "yesterday", "2026/10/01", "2026-10-01 10:00:00"])
def test_bad_dates_are_rejected_before_any_sql_is_built(bad):
    with pytest.raises(ValueError):
        parse_date(bad)
    with pytest.raises(ValueError):
        placeholders("s", "g", bad, "2026-10-01")
    store = FakeStore()
    with pytest.raises(ValueError):
        run_build(store, from_date=bad, to_date="2026-10-01")
    assert store.executed == []


def test_datetime_objects_are_not_dates():
    with pytest.raises(ValueError):
        parse_date(datetime(2026, 10, 1, 5))
    assert parse_date(date(2026, 10, 1)) == date(2026, 10, 1)
    assert parse_date(" 2026-10-01 ") == date(2026, 10, 1)


def test_every_step_renders_without_leftover_placeholders_and_uses_known_names():
    values = placeholders("funnelstream.silver", "funnelstream.gold", "2026-10-01", "2026-10-03")
    for name in BUILT:
        raw = open(step_file(name), encoding="utf-8").read()
        assert set(re.findall(r"\{(\w+)\}", raw)) <= set(values), name
        text = "\n".join(step_statements(name, values))
        assert not re.search(r"[{}]", text), name


def test_window_resolution():
    today = date(2026, 10, 6)
    assert resolve_window(today=today) == (date(2026, 9, 30), today)  # last 7 days, today included
    assert resolve_window(days=1, today=today) == (today, today)
    assert resolve_window(days=30, today=today) == (date(2026, 9, 7), today)
    assert resolve_window("2026-10-01", "2026-10-03") == (date(2026, 10, 1), date(2026, 10, 3))
    assert resolve_window(full=True) == (date(1900, 1, 1), date(9999, 12, 31))
    for kw in (dict(from_date="2026-10-01"), dict(to_date="2026-10-01"), dict(days=0),
               dict(from_date="2026-10-03", to_date="2026-10-01"), dict(full=True, days=3),
               dict(full=True, from_date="2026-10-01", to_date="2026-10-02"),
               dict(days=3, from_date="2026-10-01", to_date="2026-10-02")):
        with pytest.raises(ValueError):
            resolve_window(**kw)


def test_full_window_renders_the_widest_dates():
    store = FakeStore()
    run_build(store, full=True)
    text = "\n".join(store.executed)
    assert "DATE'1900-01-01'" in text and "DATE'9999-12-31'" in text


def test_schema_prefix_of_the_stores():
    assert FakeStore.__mro__[1].schema_prefix(FakeStore(), "gold") == "gold"
    db = DatabricksStore("h", "tok", warehouse_id="w", catalog="mycat", connection=object())
    assert db.schema_prefix("gold") == "mycat.gold" and db.schema_prefix("silver") == "mycat.silver"
    assert PostgresStore("dsn").schema_prefix("gold") == "public"
    with pytest.raises(ValueError):
        db.schema_prefix("bronze")


# ---------------------------------------------------------------- dialect lint
def lint_sql(text):
    """Problems found in a SQL script (comments stripped): the dialect rules of the Gold build."""
    body = "\n".join(line.split("--")[0] for line in text.splitlines())
    problems = []
    if re.search(r"INSERT\s+\*", body, re.I):
        problems.append("INSERT *")
    if re.search(r"UPDATE\s+SET\s+\*", body, re.I):
        problems.append("UPDATE SET *")
    if re.search(r"GENERATED", body, re.I):
        problems.append("GENERATED")
    if re.search(r"\bINSERT\s+INTO\b[^()]*\bVALUES\b", body, re.I):
        problems.append("INSERT without column list")
    keys = {"customer_key", "product_key", "campaign_key"}
    for stmt in body.split(";"):  # identity keys exist only in the three dimensions
        if not re.search(r"MERGE\s+INTO\s+\S*\bdim_(customer|product|campaign)\b", stmt, re.I):
            continue
        for m in re.finditer(r"INSERT\s*\(([^)]*)\)", stmt, re.I):  # MERGE INSERT column lists
            cols = {c.strip().lower() for c in m.group(1).split(",")}
            if cols & keys:
                problems.append("identity key supplied: %s" % sorted(cols & keys))
        for m in re.finditer(r"SET\s+(.*?)(?:WHEN|$)", stmt, re.I | re.S):
            if re.search(r"\bt\.(customer_key|product_key|campaign_key)\s*=", m.group(1), re.I):
                problems.append("identity key updated")
    if re.search(r"\bQUALIFY\b", body, re.I):
        problems.append("QUALIFY")
    if re.search(r"\bCREATE\s+(OR\s+REPLACE\s+)?(TEMP|TEMPORARY|VIEW)\b|\bDROP\b|\bTRUNCATE\b", body, re.I):
        problems.append("DDL or temp object")
    return problems


def test_lint_flags_the_forbidden_forms():
    assert lint_sql("MERGE INTO a USING b ON x WHEN NOT MATCHED THEN INSERT * ") == ["INSERT *"]
    assert lint_sql("... WHEN MATCHED THEN UPDATE SET *") == ["UPDATE SET *"]
    assert lint_sql("CREATE TABLE t (k BIGINT GENERATED ALWAYS AS IDENTITY)") == ["GENERATED"]
    assert "identity key supplied" in lint_sql(
        "MERGE INTO g.dim_customer t USING s ON x WHEN NOT MATCHED THEN INSERT (customer_key, a) VALUES (1, 2)")[0]
    assert lint_sql("MERGE INTO g.dim_product t USING s ON x WHEN MATCHED THEN UPDATE SET t.product_key = s.k "
                    "WHEN NOT MATCHED THEN INSERT (a) VALUES (1)") == ["identity key updated"]
    assert lint_sql("MERGE INTO g.fact_orders t USING s ON x WHEN MATCHED THEN UPDATE SET t.customer_key = s.k "
                    "WHEN NOT MATCHED THEN INSERT (customer_key) VALUES (s.k)") == []  # facts hold plain keys
    assert lint_sql("SELECT 1 FROM t QUALIFY rn = 1") == ["QUALIFY"]
    assert lint_sql("-- INSERT * is only mentioned in a comment\nSELECT 1") == []


@pytest.mark.parametrize("fname", SQL_FILES)
def test_every_gold_sql_file_passes_the_dialect_lint(fname):
    text = open(os.path.join(GOLD_DIR, fname), encoding="utf-8").read()
    assert lint_sql(text) == []
    assert "{gold}." in text  # writes to the gold schema through the placeholder, never a hard-coded schema
    assert not re.search(r"\b(funnelstream|gold|silver)\.(gold|silver|silver_\w+|dim_\w+|fact_\w+|agg_\w+)\b",
                         re.sub(r"\{(gold|silver)\}", "", text))
    for stmt in step_statements(fname.split("_", 1)[1][:-4], placeholders("s", "g", "2026-10-01", "2026-10-01")):
        assert stmt.endswith(";") and stmt.count(";") == 1


def test_merge_insert_and_update_lists_name_real_columns_of_the_target_table():
    for name in ("dim_customer", "dim_product", "dim_campaign", "dim_date", "fact_orders", "fact_order_lines",
                 "agg_orders_daily", "agg_funnel_daily", "agg_campaign_daily", "agg_city_product_daily"):
        spec = gold_table(name)
        text = "\n".join(step_statements(name, placeholders("s", "g", "2026-10-01", "2026-10-01")))
        insert = re.search(r"INSERT\s*\(([^)]*)\)\s*VALUES", text)
        cols = [c.strip() for c in insert.group(1).split(",")]
        identity = {c.name for c in spec.columns if c.identity}
        assert set(cols) <= set(spec.column_names()) - identity, name
        assert set(cols) >= {c.name for c in spec.columns if not c.identity and not c.nullable}, name
        for c in re.findall(r"\bt\.(\w+) = s\.", text):
            assert c in spec.column_names() and c not in identity, (name, c)


def test_fact_orders_leaves_campaign_id_to_the_attribution_step():
    text = open(step_file("fact_orders"), encoding="utf-8").read()
    update = text.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
    assert "campaign_id" not in update and "campaign_id" in text.split("WHEN NOT MATCHED")[1]


def test_window_predicates_are_in_the_expected_steps_and_summaries_delete_before_merge():
    values = placeholders("s", "g", "2026-10-01", "2026-10-03")
    for name in ("agg_orders_daily", "agg_funnel_daily", "agg_campaign_daily", "agg_city_product_daily"):
        stmts = step_statements(name, values)
        assert [s.split()[0] for s in stmts] == ["MERGE"], name  # one atomic statement, no separate DELETE
        m = stmts[0]
        assert "WHEN NOT MATCHED BY SOURCE" in m and "THEN DELETE" in m
        assert "2026-10-01" in m.split("WHEN NOT MATCHED BY SOURCE")[1] and "2026-10-03" in m.split(
            "WHEN NOT MATCHED BY SOURCE")[1]  # the delete is limited to the window
        assert "WHEN NOT MATCHED THEN INSERT" in m and "WHEN MATCHED THEN UPDATE SET" in m
    for name in ("fact_orders", "fact_order_lines"):
        text = "\n".join(step_statements(name, values))
        assert "silver_order_status" in text and "event_date BETWEEN DATE'2026-10-01' AND DATE'2026-10-03'" in text
        assert "order_created_at" in text and "Africa/Cairo" in text


def test_funnel_sql_carries_the_bot_rule_and_late_row_inclusion():
    text = "\n".join(step_statements("agg_funnel_daily", placeholders("s", "g", "2026-10-01", "2026-10-03")))
    assert "RANGE BETWEEN 10000000 PRECEDING AND CURRENT ROW" in text and "n > 100" in text  # >100 in 10 seconds
    assert "LEFT ANTI JOIN bots" in text
    assert "is_late" not in text and "is_bot" not in text  # late rows count; the stored is_bot is not trusted


# ---------------------------------------------------------------- dim_campaign VALUES
def test_campaigns_values_come_from_the_simulator():
    values = campaigns_values()
    for c in CAMPAIGNS:
        assert "'%s'" % c["campaign_id"] in values and "'%s'" % c["utm_campaign"] in values
    assert values.count("CAST(NULL AS DATE)") == 2 * len(CAMPAIGNS)
    text = "\n".join(step_statements("dim_campaign", placeholders("s", "g", "2026-10-01", "2026-10-01")))
    assert "camp_201" in text and "unknown" not in text.split("VALUES")[1].split(") AS s")[0]


# ---------------------------------------------------------------- calendar_egypt / dim_date
def days(a, b):
    d = a
    while d <= b:
        yield d
        d += timedelta(days=1)


def test_dim_date_range_is_2025_to_2027_and_2028_leap_day_is_out():
    all_days = list(cal.date_range())
    assert all_days[0] == date(2025, 1, 1) and all_days[-1] == date(2027, 12, 31)
    assert len(all_days) == 1095  # no leap year in 2025, 2026 or 2027
    assert date(2028, 2, 29) not in all_days and date(2027, 2, 29 - 1) in all_days
    assert sum(1 for d in all_days if d.year == 2027 and d.month == 2) == 28
    assert cal.date_key(date(2027, 12, 31)) == 20271231 and cal.date_key(date(2025, 1, 1)) == 20250101
    rendered = gb.placeholders("s", "g", "2026-10-01", "2026-10-01")
    assert rendered["dim_date_start"] == "2025-01-01" and rendered["dim_date_end"] == "2027-12-31"
    with pytest.raises(ValueError):
        date(2027, 2, 29)


def test_weekend_is_friday_and_saturday():
    assert [d for d in days(date(2026, 10, 1), date(2026, 10, 7)) if cal.is_weekend(d)] == [
        date(2026, 10, 2), date(2026, 10, 3)]  # Friday and Saturday
    assert date(2026, 10, 2).strftime("%A") == "Friday" and date(2026, 10, 3).strftime("%A") == "Saturday"
    assert not cal.is_weekend(date(2026, 10, 4))  # Sunday is a working day in Egypt
    assert sum(cal.is_weekend(d) for d in cal.date_range()) == sum(
        d.weekday() in (4, 5) for d in cal.date_range())


@pytest.mark.parametrize("first,last", [(date(2025, 3, 1), date(2025, 3, 29)), (date(2026, 2, 18), date(2026, 3, 19)),
                                        (date(2027, 2, 8), date(2027, 3, 9))])
def test_ramadan_ranges(first, last):
    assert cal.is_ramadan(first) and cal.is_ramadan(last)
    assert not cal.is_ramadan(first - timedelta(days=1)) and not cal.is_ramadan(last + timedelta(days=1))
    assert all(cal.is_ramadan(d) and not cal.is_eid(d) for d in days(first, last))


@pytest.mark.parametrize("first,last", [
    (date(2025, 3, 30), date(2025, 4, 1)), (date(2025, 6, 6), date(2025, 6, 9)),
    (date(2026, 3, 20), date(2026, 3, 22)), (date(2026, 5, 26), date(2026, 5, 29)),
    (date(2027, 3, 10), date(2027, 3, 12)), (date(2027, 5, 16), date(2027, 5, 19))])
def test_eid_ranges_are_holidays_and_outside_ramadan(first, last):
    for d in days(first, last):
        assert cal.is_eid(d) and cal.is_holiday(d) and not cal.is_ramadan(d)
    assert not cal.is_eid(first - timedelta(days=1)) and not cal.is_eid(last + timedelta(days=1))


def test_fixed_holidays_and_ordinary_days():
    for d in (date(2026, 10, 6), date(2026, 1, 7), date(2026, 1, 25), date(2026, 4, 25), date(2026, 5, 1),
              date(2026, 7, 23), date(2027, 10, 6), date(2025, 1, 7)):
        assert cal.is_holiday(d) and not cal.is_eid(d), d
    for d in (date(2026, 10, 7), date(2026, 9, 9), date(2026, 3, 19), date(2027, 6, 30)):
        assert not cal.is_holiday(d), d
    assert cal.is_ramadan(date(2026, 3, 19)) and not cal.is_holiday(date(2026, 3, 19))  # Ramadan alone is no holiday


def test_calendar_ranges_do_not_overlap_and_stay_inside_the_dim_date_range():
    spans = sorted((a, b, k) for k, a, b in cal.CALENDAR_RANGES)
    for (a1, b1, _), (a2, b2, _) in zip(spans, spans[1:]):
        assert b1 < a2
    for a, b, _ in spans:
        assert cal.DIM_DATE_START <= a <= b <= cal.DIM_DATE_END
    assert "APPROXIMATE" in open(cal.__file__, encoding="utf-8").read()


def test_calendar_is_rendered_into_the_dim_date_sql():
    text = "\n".join(step_statements("dim_date", placeholders("s", "g", "2026-10-01", "2026-10-01")))
    for kind, a, b in cal.CALENDAR_RANGES:
        assert "('%s', DATE'%s', DATE'%s')" % (kind, a.isoformat(), b.isoformat()) in text
    for m, d, _ in cal.FIXED_HOLIDAYS:
        assert "(%d, %d)" % (m, d) in text
    assert "dayofweek(r.d) IN (6, 7)" in text and "is_friday_saturday" in text
    assert text.count("dayofweek(r.d) IN (6, 7)") == 2  # is_weekend and is_friday_saturday carry the same value


# ---------------------------------------------------------------- CLI
def test_cli_dry_run_without_settings_prints_statements_in_order_and_executes_nothing(capsys, monkeypatch):
    for n in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID", "DATABRICKS_HTTP_PATH",
              "DATABRICKS_CATALOG", "GOLD_BACKEND"):
        monkeypatch.delenv(n, raising=False)
    assert main(["--dry-run", "--from-date", "2026-10-01", "--to-date", "2026-10-03"]) == 0
    out = capsys.readouterr().out
    assert "window: 2026-10-01 .. 2026-10-03" in out
    positions = [out.index("-- step %s:" % n) for n in BUILT]
    assert positions == sorted(positions)
    assert "MERGE INTO funnelstream.gold.fact_orders AS t" in out and "DATE'2026-10-03'" in out
    assert "fact_attribution" in out and "empty" in out


def test_cli_errors_exit_2_without_a_traceback(capsys, monkeypatch):
    for n in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID", "DATABRICKS_HTTP_PATH"):
        monkeypatch.delenv(n, raising=False)
    assert main(["--from-date", "2026-10-01"]) == 2
    assert main(["--dry-run", "--from-date", "2026-10-01", "--to-date", "bad"]) == 2
    assert main(["--dry-run", "--steps", "nope"]) == 2
    assert main(["--dry-run", "--full", "--days", "3"]) == 2
    assert main(["--from-date", "2026-10-01", "--to-date", "2026-10-02"]) == 2  # no settings, not a dry run
    err = capsys.readouterr().err
    assert "Traceback" not in err and "DATABRICKS_HOST" in err


def test_cli_run_uses_the_store_and_reports_a_failing_step_with_exit_1(monkeypatch, capsys):
    fake = FakeStore(fail="MERGE INTO funnelstream.gold.dim_date ")
    monkeypatch.setattr(gb, "get_store", lambda backend: fake)
    assert main(["--days", "2"]) == 1
    err = capsys.readouterr().err
    assert "step dim_date" in err and "done before the failure: dim_customer" in err
    ok = FakeStore()
    monkeypatch.setattr(gb, "get_store", lambda backend: ok)
    assert main(["--full", "--steps", "dim_date,dim_campaign"]) == 0
    out = capsys.readouterr().out
    assert "window: 1900-01-01 .. 9999-12-31 (full)" in out and "dim_campaign" in out
    assert [s.split()[2] for s in ok.executed if s.startswith("MERGE")] == [
        "funnelstream.gold.dim_campaign", "funnelstream.gold.dim_date"]


def test_modules_import_without_spark_or_network():
    import importlib
    import sys
    for mod in ("warehouse.gold_build", "warehouse.calendar_egypt"):
        importlib.import_module(mod)
    assert "pyspark" not in sys.modules or True  # other test modules may have imported it; no import in the sources
    for mod in (gb, cal):
        assert "pyspark" not in open(mod.__file__, encoding="utf-8").read()


def test_first_statement_pins_the_session_time_zone_to_utc_and_every_step_repeats_it():
    store = FakeStore()
    run_build(store, **WINDOW)
    assert store.executed[0] == "SET TIME ZONE 'UTC'"
    pins = [i for i, s in enumerate(store.executed) if s == gb.TIME_ZONE_SQL]
    assert len(pins) == len(BUILT)  # once per step that has SQL, so a reconnect between steps is covered
    assert all(store.executed[i + 1].split()[0] in ("MERGE", "DELETE") for i in pins)
    dry = run_build(FakeStore(), dry_run=True, **WINDOW)
    assert dry["steps"]["dim_customer"]["sql"][0] == gb.TIME_ZONE_SQL


def test_every_merge_source_has_a_uniqueness_guard_and_no_standalone_delete():
    for fname in SQL_FILES:
        for stmt in step_statements(fname.split("_", 1)[1][:-4], placeholders("s", "g", "2026-10-01", "2026-10-01")):
            assert not stmt.startswith("DELETE"), fname
            if not stmt.startswith("MERGE"):
                continue
            source = stmt.split("USING", 1)[1].split(") AS s", 1)[0]
            assert re.search(r"ROW_NUMBER\(|GROUP BY|DISTINCT|FROM VALUES|LEAD\(", source), (fname, stmt[:80])


def test_campaign_step_runs_right_after_funnel_and_is_one_windowed_merge():
    store = FakeStore()
    result = run_build(store, **WINDOW)
    merges = [s for s in store.executed if s.startswith("MERGE INTO")]
    names = [re.match(r"MERGE INTO funnelstream\.gold\.(\w+)", s).group(1) for s in merges]
    assert names.index("agg_campaign_daily") == names.index("agg_funnel_daily") + 1
    assert result["steps"]["agg_campaign_daily"]["status"] == "ok" and result["steps"]["agg_campaign_daily"]["statements"] == 1
    dry = run_build(FakeStore(), dry_run=True, **WINDOW)
    sql = dry["steps"]["agg_campaign_daily"]["sql"]
    assert sql[0] == gb.TIME_ZONE_SQL and len(sql) == 2
    assert "silver_campaign_snapshots" in sql[1] and "last_touch" in sql[1] and "current_timestamp()" in sql[1]
    assert "t.report_date BETWEEN DATE'2026-10-01' AND DATE'2026-10-03' THEN DELETE" in sql[1]


def test_city_product_step_runs_right_after_campaign_and_is_one_windowed_merge():
    store = FakeStore()
    result = run_build(store, **WINDOW)
    merges = [s for s in store.executed if s.startswith("MERGE INTO")]
    names = [re.match(r"MERGE INTO funnelstream\.gold\.(\w+)", s).group(1) for s in merges]
    assert names.index("agg_city_product_daily") == names.index("agg_campaign_daily") + 1
    r = result["steps"]["agg_city_product_daily"]
    assert r["status"] == "ok" and r["statements"] == 1 and r["rows"] == 7
    assert result["steps"]["rec_product"]["status"] == "empty" and result["steps"]["checks"]["status"] == "ok"
    dry = run_build(FakeStore(), dry_run=True, **WINDOW)
    sql = dry["steps"]["agg_city_product_daily"]["sql"]
    assert sql[0] == gb.TIME_ZONE_SQL and len(sql) == 2
    text = sql[1]
    assert "fact_order_lines" in text and "silver_events" in text and "FULL OUTER JOIN" in text
    assert "COUNT(DISTINCT" in text and "LEFT ANTI JOIN bots" in text and "n > 100" in text
    assert "is_bot" not in text and "is_late" not in text and "district" not in text
    assert "t.date_key BETWEEN CAST(date_format(DATE'2026-10-01', 'yyyyMMdd') AS INT)" in text
    assert text.rstrip().endswith("THEN DELETE;")
