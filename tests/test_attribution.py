"""Host tests for the attribution Gold step (warehouse/gold/07_fact_attribution.sql and its runner wiring).

Fake store only: nothing here runs Spark. The SQL itself and its parity with streaming/gold/attribution.py are
tested in tests/test_attribution_spark.py.
"""
import os
import re

import pytest

import warehouse.gold_build as gb
from tests.test_gold_build import FakeStore, NO_CHECKS, WINDOW, lint_sql
from warehouse.gold_build import GoldBuildError, STEPS, placeholders, run_build, step_file, step_statements
from warehouse.tables import gold_table

VALUES = placeholders("s", "g", "2026-10-01", "2026-10-03")


def statements():
    return step_statements("fact_attribution", VALUES)


def test_the_step_file_is_in_its_slot_between_order_lines_and_the_orders_summary():
    assert os.path.basename(step_file("fact_attribution")) == "07_fact_attribution.sql"
    assert os.path.isfile(step_file("fact_attribution"))
    assert STEPS.index("fact_order_lines") + 1 == STEPS.index("fact_attribution") == STEPS.index("agg_orders_daily") - 1


def test_executed_statement_order_is_order_lines_then_attribution_then_orders_summary():
    store = FakeStore()
    result = run_build(store, steps=NO_CHECKS, **WINDOW)
    merges = [s.split()[2] for s in store.executed if s.startswith("MERGE")]
    i = merges.index("funnelstream.gold.fact_attribution")
    assert merges[i - 1] == "funnelstream.gold.fact_order_lines"
    assert merges[i:i + 3] == ["funnelstream.gold.fact_attribution", "funnelstream.gold.fact_orders",
                               "funnelstream.gold.agg_orders_daily"]
    assert result["steps"]["fact_attribution"] == {"status": "ok", "statements": 2, "rows": 7}
    pins = [k for k, s in enumerate(store.executed) if s == gb.TIME_ZONE_SQL]
    first = next(k for k, s in enumerate(store.executed) if s.startswith("MERGE INTO funnelstream.gold.fact_attribution"))
    assert first - 1 in pins and not any(first < p < first + 2 for p in pins)  # one pin, then both statements


def test_two_statements_the_attribution_merge_then_the_campaign_update_of_fact_orders():
    stmts = statements()
    assert len(stmts) == 2 and all(s.startswith("MERGE INTO") and s.endswith(";") for s in stmts)
    assert stmts[0].startswith("MERGE INTO g.fact_attribution AS t")
    assert stmts[1].startswith("MERGE INTO g.fact_orders AS t")


def test_dry_run_renders_both_statements_and_executes_nothing():
    store = FakeStore()
    result = run_build(store, dry_run=True, **WINDOW)
    r = result["steps"]["fact_attribution"]
    assert store.executed == [] and store.queries == []
    assert r["status"] == "dry-run" and r["statements"] == 2
    assert r["sql"][0] == gb.TIME_ZONE_SQL and len(r["sql"]) == 3
    assert "DATE'2026-10-01'" in r["sql"][1] and "DATE'2026-10-03'" in r["sql"][1]
    assert "funnelstream.gold.fact_attribution" in r["sql"][1] and "funnelstream.gold.fact_orders" in r["sql"][2]
    assert not re.search(r"[{}]", "\n".join(r["sql"]))
    assert result["reported"] == {}


def test_the_file_passes_the_dialect_lint_and_every_merge_source_is_unique_on_its_keys():
    text = open(step_file("fact_attribution"), encoding="utf-8").read()
    assert lint_sql(text) == []
    assert "{gold}." in text and "{silver}." in text
    for stmt in statements():
        source = stmt.split("USING", 1)[1].split(") AS s\nON", 1)[0]
        assert re.search(r"ROW_NUMBER\(", source), stmt[:60]
    assert "INSERT *" not in text and "UPDATE SET *" not in text


def test_merge_column_lists_name_real_columns_and_the_campaign_update_is_the_only_fact_orders_write():
    spec = gold_table("fact_attribution")
    first, second = statements()
    insert = re.search(r"INSERT\s*\(([^)]*)\)\s*VALUES", first)
    cols = [c.strip() for c in insert.group(1).split(",")]
    assert cols == spec.column_names()
    assert set(re.findall(r"\bt\.(\w+) = s\.", first)) <= set(spec.column_names())
    assert tuple(spec.keys) == ("order_id", "model")
    assert re.search(r"ON t\.order_id = s\.order_id AND t\.model = s\.model", first)
    # second statement: UPDATE only campaign_id of fact_orders, never insert or delete
    assert re.findall(r"\bt\.(\w+) = s\.", second) == ["order_id", "campaign_id"] or \
        re.findall(r"UPDATE SET (.*);", second, re.S)[0].strip() == "t.campaign_id = s.campaign_id"
    assert "INSERT" not in second and "DELETE" not in second
    assert "campaign_id" in gold_table("fact_orders").column_names()


def test_vanished_orders_are_deleted_in_the_same_atomic_merge_without_a_standalone_delete():
    first = statements()[0]
    assert "WHEN MATCHED AND s.gone THEN DELETE" in first
    assert "WHEN NOT MATCHED AND NOT s.gone THEN INSERT" in first
    assert first.index("THEN DELETE") < first.index("THEN UPDATE SET") < first.index("THEN INSERT")  # grammar order
    assert not any(s.startswith("DELETE") for s in statements())


def test_the_sql_carries_the_rules_of_the_model():
    text = statements()[0]
    assert "RANGE BETWEEN 10000000 PRECEDING AND CURRENT ROW" in text and "n > 100" in text  # bot rule
    assert "LEFT ANTI JOIN bots" in text
    assert "e.event_type IN ('page_view', 'product_view')" in text and "e.utm_campaign IS NOT NULL" in text
    assert "ORDER BY c.event_time, c.event_id" in text  # the first event is the click
    assert "d.campaign_id <> 'unknown'" in text  # the unknown member never resolves a click
    assert "COALESCE(f.campaign_id, m.campaign_id)" in text
    assert text.count("INTERVAL 7 DAYS") >= 2
    assert "k.click_time >= o.order_created_at - INTERVAL 7 DAYS AND k.click_time < o.order_created_at" in text
    assert "ORDER BY k.click_time DESC, k.click_id ASC NULLS LAST" in text  # last_touch tie rule
    assert "ORDER BY k.click_time ASC, k.click_id ASC NULLS LAST" in text  # first_touch tie rule
    assert "'last_touch' AS model" in text and "'first_touch' AS model" in text
    assert "is_bot" not in text  # the stored column is not trusted


def test_fact_orders_step_still_leaves_campaign_id_alone():
    text = open(step_file("fact_orders"), encoding="utf-8").read()
    update = text.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
    assert "campaign_id" not in update


def test_runner_reports_the_attribution_counts_after_the_step():
    store = FakeStore()
    result = run_build(store, steps=NO_CHECKS, **WINDOW)
    assert {k: result["reported"][k] for k in ("orders_without_click", "orders_direct", "unresolved_clicks")} == {
        "orders_without_click": 3, "orders_direct": 3, "unresolved_clicks": 3}
    queries = [q for q in store.queries if "fact_attribution" in q]
    assert len(queries) == 3  # the row count and the two gold-only reports (the third report reads silver_events)
    # both statements of the step run before the next step
    k_att = max(i for i, s in enumerate(store.executed) if s.startswith("MERGE INTO funnelstream.gold.fact_orders"))
    assert store.executed[k_att + 2].startswith("MERGE INTO funnelstream.gold.agg_orders_daily")
    for q in store.queries:
        assert not re.search(r"[{}]", q)


def test_reported_counts_are_cheap_selects_over_the_gold_table_only():
    for key in ("orders_without_click", "orders_direct"):
        q = gb.ATTRIBUTION_REPORTED_SQL[key]
        assert q.startswith("SELECT COUNT(*)") and "{silver}" not in q
    assert "{silver}.silver_events" in gb.ATTRIBUTION_REPORTED_SQL["unresolved_clicks"]


def test_other_steps_do_not_report_attribution_counts_and_dry_runs_report_nothing():
    result = run_build(FakeStore(), steps=["fact_orders"], **WINDOW)
    assert list(result["reported"]) == ["orders_without_created_at"]


def test_a_failing_report_query_is_a_step_failure():
    class Boom(FakeStore):
        def query(self, sql, params=None, retry=True):
            if sql.startswith("SELECT COUNT(*) FROM") and "last_touch" in sql:
                raise RuntimeError("boom")
            return super().query(sql, params, retry)

    with pytest.raises(GoldBuildError) as e:
        run_build(Boom(), **WINDOW)
    assert e.value.step == "fact_attribution"


def test_reference_module_exposes_the_documented_api():
    from streaming.gold import attribution, build, orders
    assert attribution.FACT_ATTRIBUTION_COLUMNS == gold_table("fact_attribution").column_names()
    assert callable(attribution.build_clicks) and callable(attribution.build_fact_attribution)
    assert "attribution" in build.__doc__.lower() or "fact_attribution" in build.__doc__
    import inspect
    assert inspect.signature(orders.build_fact_orders).parameters["attribution"].default is None
    assert tuple(attribution.MODELS) == ("last_touch", "first_touch")
