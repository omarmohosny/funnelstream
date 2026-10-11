"""Host tests for scripts/smoke_test.py and scripts/smoke_spark_checks.py: no Docker, no Kafka, no Spark, no Postgres,
no Databricks. A fake runner, a fake clock and a fake Postgres stand in for them; the files of the repo (dashboards,
alert rules, MODEL.md, measures.dax) are read for real."""
import datetime
import json
import os
import re
import shutil
from types import SimpleNamespace

import pytest

from scripts import smoke_spark_checks as sc
from scripts import smoke_test as sm
from warehouse.live_tables import LIVE_TABLES

NOW = datetime.datetime(2026, 10, 11, 12, 0, 0, tzinfo=datetime.timezone.utc)
SIM_OUT = "noise written\n{'app.events': 19316, 'orders': 667, 'delivery.events': 383, 'inventory.events': 4000} total 24366\n"
TOPICS_OUT = "".join("created %s\n" % t for t in sm.TOPICS)
FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "noise_manifest_fixture.json")


def _args(*extra):
    return sm.parse_args(list(extra))


def counts_json(bronze_total=24366, empty=()):
    silver = {t: (0 if t in empty else 10) for t in sm.SILVER_NON_EMPTY}
    return {"bronze": {"bronze_app_events": bronze_total - 100, "bronze_orders": 50, "bronze_delivery_events": 25,
                       "bronze_inventory_events": 25}, "silver": silver, "data_day": "2026-10-04"}


class FakeRunner:
    """Answers by command text; every call is recorded. `rc` maps a substring of the command to an exit code."""

    def __init__(self, rc=None, outputs=None, containers=""):
        self.calls, self.rc, self.outputs, self.containers = [], rc or {}, outputs or {}, containers
        self.counts = counts_json()

    def __call__(self, argv, **kw):
        self.calls.append(list(argv))
        line = " ".join(argv)
        out, code = "", 0
        if argv[:2] == ["docker", "ps"]:
            out = self.containers
        elif argv[:2] == ["docker", "info"]:
            out = "2026-10-11T12:00:00.500123456Z|8318783488\n"
        elif argv[:2] == ["git", "rev-parse"]:
            out = "abc1234\n"
        elif "create_topics.py" in line:
            out = TOPICS_OUT
        elif "run_simulator.py" in line:
            out = SIM_OUT
        elif "smoke_spark_checks.py express-backfill" in line:
            out = 'SMOKE_JSON {"express_rows": 120, "written": 120}\n'
        elif "smoke_spark_checks.py counts" in line:
            out = "SMOKE_JSON " + json.dumps(self.counts) + "\n"
        elif "pytest" in line:
            out = "1200 passed, 3 skipped in 30.1s\n"
        elif "run_batch.py" in line:
            out = "checks: 56 run, 0 failed\nBATCH OK in 12.0s\n"
        for key, text in self.outputs.items():
            if key in line:
                out = text
        for key, value in self.rc.items():
            if key in line:
                code = value
        return SimpleNamespace(returncode=code, stdout=out, stderr="")


class Clock:
    def __init__(self, step=2.0):
        self.t, self.step = 0.0, step

    def __call__(self):
        self.t += self.step
        return self.t


class FakePg:
    """query(who, sql): the first handler whose key is in the SQL answers (rows, or a callable(sql) -> rows)."""

    def __init__(self, handlers=None, default=None):
        self.handlers, self.default, self.calls, self.created = handlers or [], default, [], False

    def query(self, who, sql):
        self.calls.append((who, sql))
        for key, answer in self.handlers:
            if key in sql:
                return answer(sql) if callable(answer) else answer
        if self.default is None:
            raise AssertionError("unexpected SQL: " + sql)
        return self.default

    def recreate_database(self):
        self.created = True


def make_ctx(args=None, runner=None, pg=None, http=None, kafka=None, env=None, clock=None, **kw):
    args = args or _args()
    return sm.Ctx(args, 4242, "2026-10-05T12:00:00+00:00", runner=runner or FakeRunner(), clock=clock or Clock(),
                  sleep=lambda s: None, rmtree=kw.pop("rmtree", lambda p: None), utcnow=lambda: NOW,
                  env=env if env is not None else {"PG_DSN": "postgresql://u:p@localhost:5432/funnelstream",
                                                   "GRAFANA_DB_PASSWORD": "g", "GRAFANA_ADMIN_PASSWORD": "a"},
                  pg=pg or FakePg(default=[(1,)]), http=http or (lambda url, auth=None, timeout=5: (200, '{"status": "OK"}')),
                  kafka=kafka or (lambda bootstrap: "1 broker(s), 4 topic(s)"), memory=lambda: "6.0 GB free of 16.0 GB")


# ---------------------------------------------------------------------------------------------- plan and safety
def test_the_plan_lists_the_stages_in_order():
    keys = [s.key for s in sm.plan(_args(), 7, "2026-10-05T12:00:00+00:00")]
    assert keys == ["preflight", "host_tests", "spark_tests", "static", "reset_generate", "bronze_silver", "invariants",
                    "daily_marts", "dashboards", "alerts", "replay", "databricks", "d1_marketing", "power_bi"]
    text = sm.format_plan(sm.plan(_args(), 7, "2026-10-05T12:00:00+00:00"), _args(), 7)
    positions = [text.index("%2d. " % n) for n in range(1, 16)]
    assert positions == sorted(positions) and "15. Report" in text and "run_simulator.py" in text


def test_skip_flags_show_in_the_plan_and_the_skipped_stages_say_why():
    stages = {s.key: s for s in sm.plan(_args("--skip-gold", "--skip-spark-tests"), 7, "x")}
    assert stages["databricks"].skip_reason == "--skip-gold" and stages["spark_tests"].skip_reason == "--skip-spark-tests"
    assert stages["d1_marketing"].skip_reason == "--skip-gold"
    assert "--skip-gold" in sm.format_plan(list(stages.values()), _args("--skip-gold"), 7)
    full = {s.key: s for s in sm.plan(_args(), 7, "x")}
    assert full["d1_marketing"].skip_reason is None
    # C3: the D-1 build reads the Gold ad spend, so it runs after the Databricks batch, never in stage 8
    assert full["d1_marketing"].depends == ("bronze_silver", "databricks")
    assert "build_d1_marketing.py" in " ".join(" ".join(c) for c in full["d1_marketing"].commands)
    marts = " ".join(" ".join(c) for c in full["daily_marts"].commands)
    assert "build_d1_marketing.py" not in marts and "apply_ddl.py" not in marts
    keys = [k.key for k in sm.plan(_args(), 7, "x")]
    assert keys.index("databricks") < keys.index("d1_marketing")


def test_dry_run_runs_nothing(capsys):
    runner = FakeRunner()
    assert sm.main(["--dry-run"], runner=runner) == 0 and runner.calls == []
    assert "DRY RUN" in capsys.readouterr().out


def test_without_yes_it_prints_the_plan_and_exits_2(capsys, tmp_path):
    runner = FakeRunner()
    report = tmp_path / "smoke-test.md"
    assert sm.main(["--report", str(report)], runner=runner) == 2
    assert runner.calls == [] and not report.exists()
    assert "Kafka" in capsys.readouterr().out


def test_without_yes_it_warns_that_the_demo_checkpoints_become_invalid(capsys):
    assert sm.main([], runner=FakeRunner()) == 2
    out = " ".join(capsys.readouterr().out.split())
    assert "data/checkpoints" in out and "Repeat with --yes" in out and "fresh demo lake" in out


def test_refuses_when_demo_containers_run(capsys):
    runner = FakeRunner(containers="fs-pipeline\n")
    assert sm.main(["--yes"], runner=runner) == 3
    assert all(c[:2] == ["docker", "ps"] for c in runner.calls)
    assert "fs-pipeline" in capsys.readouterr().err


@pytest.mark.parametrize("extra", [["--lake", "data/lake"], ["--checkpoints", "data/checkpoints"], ["--lake", "data/smoke/../lake"],
                                   ["--lake", "C:/Windows"], ["--lake", "data/smoke"], ["--catalog", "funnelstream"],
                                   ["--catalog", "FunnelStream"]])
def test_refuses_unsafe_targets(extra, capsys):
    runner = FakeRunner()
    assert sm.main(["--yes", *extra], runner=runner) == 3 and runner.calls == []
    assert capsys.readouterr().err


def test_nothing_in_the_plan_touches_the_demo_lake_the_real_catalog_or_the_demo_database():
    for s in sm.plan(_args(), 7, "x"):
        for c in s.commands:
            for part in c:
                assert part.lower() != "funnelstream" and "data/lake" not in part and "data/checkpoints" not in part
    assert sm.SMOKE_DB == "funnelstream_smoke"


def test_docker_commands_point_the_containers_at_the_smoke_database():
    seen = []

    def runner(argv, **kw):
        seen.append((argv, kw.get("env")))
        return SimpleNamespace(returncode=0, stdout="", stderr="")
    ctx = make_ctx(runner=runner)
    ctx.run(["docker", "compose", "run", "--rm", "spark", "python", "x.py"])
    ctx.run([sm.sys.executable, "x.py"])
    assert seen[0][1]["POSTGRES_DB"] == "funnelstream_smoke"
    assert seen[1][1].get("POSTGRES_DB") == os.environ.get("POSTGRES_DB")  # host commands keep the caller's environment


# ---------------------------------------------------------------------------------------------- the runner
def _stage(key, func, depends=(), skip=None):
    return sm.Stage(key, key.upper(), func, depends, skip_reason=skip)


def test_a_failing_stage_does_not_stop_independent_stages_and_skips_its_dependants():
    ran = []

    def ok(name):
        return lambda ctx: ran.append(name) or "fine"

    def bad(ctx):
        raise sm.StageFail("boom\nsecond line")
    stages = [_stage("a", ok("a")), _stage("b", bad, ("a",)), _stage("c", ok("c"), ("b",)), _stage("d", ok("d"), ("c",)),
              _stage("e", ok("e"), ("a",)), _stage("f", ok("f"), (), skip="--skip-gold")]
    results = {r.name: r for r in sm.run_stages(make_ctx(), stages)}
    assert ran == ["a", "e"]
    assert results["B"].status == sm.FAIL and results["B"].evidence == "boom" and "second line" in results["B"].detail
    assert results["C"].status == sm.SKIPPED and results["C"].evidence == "skipped: b failed"
    assert results["D"].evidence == "skipped: b failed"  # the root cause, not the stage in between
    assert results["E"].status == sm.PASS and results["F"].evidence == "skipped: --skip-gold"


def test_an_unexpected_exception_is_a_failed_stage_with_its_type():
    results = sm.run_stages(make_ctx(), [_stage("a", lambda ctx: 1 / 0)])
    assert results[0].status == sm.FAIL and "ZeroDivisionError" in results[0].evidence


def test_a_stage_may_return_several_rows():
    rows = [sm.Result("x one", sm.PASS, "1"), sm.Result("x two", sm.FAIL, "2")]
    stages = [_stage("a", lambda ctx: rows), _stage("b", lambda ctx: "z", ("a",))]
    results = sm.run_stages(make_ctx(), stages)
    assert [r.status for r in results] == [sm.PASS, sm.FAIL, sm.SKIPPED] and results[2].evidence == "skipped: a failed"


def test_main_writes_the_report_and_the_exit_code_follows_the_verdict(monkeypatch, tmp_path, capsys):
    stage_list = [_stage("a", lambda ctx: "fine"), _stage("b", lambda ctx: (_ for _ in ()).throw(sm.StageFail("broken")), ("a",))]
    monkeypatch.setattr(sm, "plan", lambda *a, **k: stage_list)
    report = tmp_path / "smoke-test.md"
    rc = sm.main(["--yes", "--report", str(report)], ctx_factory=lambda a, s, st: make_ctx(a), runner=FakeRunner(), utcnow=lambda: NOW)
    text = report.read_text(encoding="utf-8")
    assert rc == 1 and text.rstrip().endswith("SMOKE TEST FAILED: 1 stage failed") and "broken" in text
    stage_list[1] = _stage("b", lambda ctx: "now fine", ("a",))
    rc = sm.main(["--yes", "--report", str(report)], ctx_factory=lambda a, s, st: make_ctx(a), runner=FakeRunner(), utcnow=lambda: NOW)
    assert rc == 0 and report.read_text(encoding="utf-8").rstrip().endswith("SMOKE TEST PASSED")


# ---------------------------------------------------------------------------------------------- the report
META = {"date": "2026-10-11", "command": "python scripts/smoke_test.py --yes", "commit": "abc1234", "os": "Windows-11",
        "python": "3.11.9", "docker_memory": "7.7 GB", "host_memory": "6.0 GB free of 16.0 GB", "clock_offset": "0.50 s",
        "ntp": "Source: time.windows.com"}


def test_the_report_has_one_row_per_result_and_the_verdict_line():
    results = [sm.Result("Preflight", sm.PASS, "all fine", 1.5), sm.Result("spark tests: test_a_spark.py", sm.PASS, "3 passed", 40.0)]
    text = sm.render_report(results, META)
    assert text.startswith("# Smoke test") and "| 1 | Preflight | pass | all fine | 1.5 |" in text
    assert "| 2 | spark tests: test_a_spark.py | pass | 3 passed | 40.0 |" in text
    assert text.rstrip().endswith("SMOKE TEST PASSED") and "abc1234" in text and "python scripts/smoke_test.py --yes" in text
    for bad in ("TODO", "TBD", "XXX", "None", "placeholder", "<", "{"):
        assert bad not in text, bad


def test_a_failed_or_skipped_stage_is_never_reported_as_a_pass():
    results = [sm.Result("A", sm.PASS, "ok"), sm.Result("B", sm.FAIL, "broke | badly", 1.0, "full\ntext"),
               sm.Result("C", sm.SKIPPED, "skipped: B failed")]
    text = sm.render_report(results, META)
    assert "| 2 | B | fail | broke / badly | 1.0 |" in text and "| 3 | C | skipped | skipped: B failed | 0.0 |" in text
    assert "SMOKE TEST FAILED: 1 stage failed" in text and "SMOKE TEST PASSED" not in text
    assert "full\ntext" in text and "## Failures" in text
    assert sm.verdict(results + [sm.Result("D", sm.FAIL, "x")]) == "SMOKE TEST FAILED: 2 stages failed"
    only_skips = [sm.Result("A", sm.PASS, "ok"), sm.Result("B", sm.SKIPPED, "skipped: --skip-gold")]
    text = sm.render_report(only_skips, META)
    assert "1 stage skipped" in text and "| 2 | B | skipped |" in text


# ---------------------------------------------------------------------------------------------- Grafana macros
@pytest.mark.parametrize("sql,expected", [
    ("WHERE $__timeFilter(minute_start)", "WHERE minute_start >= now() - interval '1 day'"),
    ("WHERE $__timeFilter(i.report_date::timestamp)", "WHERE i.report_date::timestamp >= now() - interval '1 day'"),
    ("SELECT $__timeGroupAlias(minute_start, '1m', 0), 1", "SELECT date_trunc('minute', minute_start) AS \"time\", 1"),
    ("SELECT $__timeGroup(minute_start, '1h')", "SELECT date_trunc('hour', minute_start)"),
    ("SELECT $__timeGroupAlias(created_at, '5m')", "SELECT to_timestamp(floor(extract(epoch from created_at) / 300) * 300) AS \"time\""),
    ("BETWEEN $__timeFrom() AND $__timeTo()", "BETWEEN now() - interval '1 day' AND now()"),
    ("SELECT 1", "SELECT 1")])
def test_macros_expand_to_plain_postgres(sql, expected):
    assert sm.expand_macros(sql) == expected


def test_the_window_of_the_macros_can_be_widened():
    assert sm.expand_macros("WHERE $__timeFilter(t) AND x > $__timeFrom()", days=30) == \
        "WHERE t >= now() - interval '30 days' AND x > now() - interval '30 days'"


def test_an_unknown_macro_is_an_error_not_a_silent_pass():
    with pytest.raises(ValueError, match="unexpanded"):
        sm.expand_macros("SELECT $__unixEpochFilter(t)")
    with pytest.raises(ValueError):
        sm.expand_macros("SELECT $__timeFilter(t")


def test_every_macro_used_by_the_provisioned_dashboards_expands_and_leaves_no_dollar_name():
    used, total = set(), 0
    for name in sorted(os.listdir(sm.GRAFANA_DASHBOARDS)):
        with open(os.path.join(sm.GRAFANA_DASHBOARDS, name), encoding="utf-8") as fh:
            queries = sm.dashboard_queries(json.load(fh))
        assert queries, name
        for title, sql in queries:
            used |= set(re.findall(r"\$__\w+", sql))
            out = sm.expand_macros(sql)
            assert "$__" not in out and out.strip(), (name, title)
            total += 1
    assert {"$__timeFilter", "$__timeGroupAlias"} <= used and total >= 30


def test_the_four_dashboards_and_six_alert_rules_are_found():
    assert sorted(f for f in os.listdir(sm.GRAFANA_DASHBOARDS) if f.endswith(".json")) == [
        "inventory-daily.json", "inventory-ops.json", "marketing-d1.json", "pipeline-live.json"]
    assert sorted(r["uid"] for r in sm.alert_rules()) == sorted([
        "fs-late-rate", "fs-quarantine-share", "fs-silver-silent", "fs-no-events", "fs-oos-demand", "fs-express-sla"])


# ---------------------------------------------------------------------------------------------- Power BI sources
SAMPLE_DAX = """// header with 'Fake'[ignored] in a comment
Spend = SUM ( 'Campaign Daily'[spend] )

Orders on Spend Days =
CALCULATE ( [Attributed Orders], KEEPFILTERS ( 'Campaign Daily'[spend] > 0 ) )

Stock Units =
VAR n = SUM ( 'Stock Now'[available] )
RETURN
    IF ( n > 0, n, MAX ( Store[store_id] ) )
"""


def test_the_dax_parser_finds_table_column_pairs_and_names_the_measure():
    assert sm.parse_dax_refs(SAMPLE_DAX) == [
        ("Spend", "Campaign Daily", "spend"), ("Orders on Spend Days", "Campaign Daily", "spend"),
        ("Stock Units", "Stock Now", "available"), ("Stock Units", "Store", "store_id")]


def _model_sources():
    with open(sm.MODEL_MD, encoding="utf-8") as fh:
        return sm.parse_model_sources(fh.read())


def test_model_md_lists_eleven_gold_and_seven_postgres_source_tables():
    src = _model_sources()
    assert [t for t, _ in src["gold"]] == ["dim_date", "dim_campaign", "dim_product", "agg_campaign_daily", "agg_funnel_daily",
                                           "agg_orders_daily", "fact_orders", "agg_city_product_daily", "fact_order_lines",
                                           "dim_customer", "customer_360"]
    assert [t for t, _ in src["postgres"]] == ["dim_store", "live_stock_now", "inventory_daily", "oos_demand_daily",
                                               "express_daily", "marketing_d1_campaign", "marketing_d1_product"]
    assert dict(src["postgres"])["live_stock_now"] == "Stock Now"


def test_every_table_measures_dax_references_is_a_friendly_name_of_the_model():
    with open(sm.MEASURES_DAX, encoding="utf-8") as fh:
        refs = sm.parse_dax_refs(fh.read())
    src = _model_sources()
    names = {f for _, f in src["gold"]} | {f for _, f in src["postgres"]}
    assert len(refs) > 30
    assert {t for _, t, _ in refs} <= names | {"_Measures"}


def test_a_missing_table_or_column_fails_the_check_with_its_name():
    src = {"gold": [("dim_date", "Date")], "postgres": [("live_stock_now", "Stock Now"), ("express_daily", "Express Daily")]}
    refs = [("Units", "Stock Now", "available"), ("Late", "Express Daily", "breached"), ("Ghost", "Stock Now", "nope"),
            ("Odd", "Nowhere", "x")]
    problems = sm.check_power_bi_sources(src, refs, {"dim_date": {"date"}}, {"live_stock_now": {"available"}})
    assert "Postgres table express_daily (Power BI name Express Daily) does not exist" in problems
    assert "measure Ghost names the missing column Stock Now[nope]" in problems
    assert any("measure Odd" in p and "Nowhere" in p for p in problems)
    assert not any("Units" in p for p in problems)
    assert sm.check_power_bi_sources(src, refs[:1], {"dim_date": {"date"}}, {"live_stock_now": {"available"}, "express_daily": set()}) == []


def information_schema(skip=()):
    rows = [(t.name, c) for t in LIVE_TABLES if t.name not in skip for c in t.column_names()]
    return rows


def test_the_power_bi_stage_reads_the_postgres_columns_and_fails_naming_a_missing_table():
    ok = make_ctx(pg=FakePg([("information_schema", information_schema())]))
    assert "source tables exist" in sm.stage_power_bi(ok)
    gone = make_ctx(pg=FakePg([("information_schema", information_schema(skip=("express_daily",)))]))
    with pytest.raises(sm.StageFail) as e:
        sm.stage_power_bi(gone)
    assert "Postgres table express_daily (Power BI name Express Daily) does not exist" in str(e.value)


# ---------------------------------------------------------------------------------------------- helpers
def test_parse_pytest_summary_and_simulator_output():
    assert sm.parse_pytest_summary("..\n5 failed, 1200 passed, 3 skipped, 1 error in 3.2s") == {
        "passed": 1200, "failed": 5, "skipped": 3, "errors": 1}
    assert sm.parse_pytest_summary("1 passed in 0.1s")["passed"] == 1
    assert sm.parse_pytest_summary("no summary here") is None
    counts, total = sm.parse_simulator_output(SIM_OUT)
    assert total == 24366 and counts["inventory.events"] == 4000
    with pytest.raises(ValueError):
        sm.parse_simulator_output("nothing")
    assert sm.parse_json_line('log\nSMOKE_JSON {"a": 1}\n') == {"a": 1}


def test_dsn_for_changes_only_the_database_and_optionally_the_login():
    base = "postgresql://user:p%40ss@localhost:5432/funnelstream"
    assert sm.dsn_for(base, "funnelstream_smoke") == "postgresql://user:p%40ss@localhost:5432/funnelstream_smoke"
    assert sm.dsn_for(base, "postgres") .endswith("/postgres")
    assert sm.dsn_for(base, "funnelstream_smoke", user="grafana_ro", password="x y") == \
        "postgresql://grafana_ro:x%20y@localhost:5432/funnelstream_smoke"
    with pytest.raises(ValueError):
        sm.dsn_for("host=localhost dbname=x")


def test_pin_now_and_thresholds():
    assert sm.pin_now("WHERE t >= now() - interval '5 minutes' AND now() > x", "2026-10-04T10:00:00+00:00") == \
        "WHERE t >= (TIMESTAMPTZ '2026-10-04T10:00:00+00:00') - interval '5 minutes' AND (TIMESTAMPTZ '2026-10-04T10:00:00+00:00') > x"
    rules = {r["uid"]: r for r in sm.alert_rules()}
    assert sm.rule_threshold(rules["fs-late-rate"]) == ("gt", 5) and sm.rule_threshold(rules["fs-silver-silent"]) == ("lt", 1)
    assert sm.apply_threshold("gt", 5, 5.1) is True and sm.apply_threshold("lt", 1, 0) is True and sm.apply_threshold("gt", 5, None) is None


# ---------------------------------------------------------------------------------------------- spark-side logic
def snap(store, product, t, on_hand, eid):
    return dict(store_id=store, product_id=product, event_time=t, event_id=eid, on_hand=on_hand)


def move(store, product, t, delta, eid):
    return dict(store_id=store, product_id=product, event_time=t, event_id=eid, qty_delta=delta)


def test_replay_applies_movements_after_a_snapshot_and_ties_go_before_the_snapshot():
    s, p = "s1", "p1"
    snaps = [snap(s, p, 0, 100, "a"), snap(s, p, 10, 80, "d")]
    moves = [move(s, p, 5, -4, "b"), move(s, p, 6, -6, "c"), move(s, p, 10, -10, "e"), move(s, p, 12, -2, "f"), move("s2", "p9", 1, -5, "x")]
    out = sc.replay_stock(snaps, moves)
    assert out == {(s, p): (78, 78)}  # the movement at t=10 is inside the snapshot of t=10; the one at t=12 is after it
    assert sc.compare_stock(out, {(s, p): 78}) == ([], [])


def test_replay_reports_a_pair_that_goes_below_zero_and_a_pair_the_live_rule_disagrees_on():
    out = sc.replay_stock([snap("s", "p", 0, 3, "a"), snap("s", "q", 0, 10, "b")], [move("s", "p", 1, -5, "c")])
    negative, mismatch = sc.compare_stock(out, {("s", "p"): -2, ("s", "q"): 11})
    assert negative == [("s", "p")] and mismatch == [("s", "q")]


def test_express_backfill_computes_from_the_far_past_and_upserts_by_order_id(monkeypatch):
    """C2: the live sink only writes the last 3 hours; the smoke test fills the table from all of Silver."""
    import warehouse.live_express as live_express
    import warehouse.store as store_mod

    calls, stored = {}, {}

    class Store:
        def upsert(self, table, rows, keys):
            stored.update(table=table, rows=rows, keys=keys)
            return len(rows)

        def close(self):
            stored["closed"] = True
    rows = [{"order_id": "o1"}, {"order_id": "o2"}]

    def fake_compute(spark, root, since):
        calls.update(root=root, since=since)
        return rows
    monkeypatch.setattr(live_express, "compute_express_rows", fake_compute)
    monkeypatch.setattr(store_mod, "get_store", lambda backend=None, env=None: Store())
    out = sc.express_backfill(object(), "data/smoke/lake")
    assert out == {"express_rows": 2, "written": 2}
    assert stored["table"] == "live_express_orders" and stored["keys"] == ["order_id"] and stored["closed"]
    assert calls["since"].tzinfo is not None
    assert calls["since"] < datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=365)
    assert os.path.isabs(calls["root"])
    monkeypatch.setattr(live_express, "compute_express_rows", lambda *a: [])
    assert sc.express_backfill(object(), "x") == {"express_rows": 0, "written": 0}


# ---------------------------------------------------------------------------------------------- the stages
def test_preflight_passes_and_collects_the_environment():
    ctx = make_ctx()
    out = sm.stage_preflight(ctx)
    assert "clock offset 0.50 s" in out and "Kafka handshake: 1 broker(s)" in out and "data source fs-postgres OK" in out
    assert ctx.state["env"]["docker_memory"] == "7.7 GB"


def test_preflight_names_every_problem_it_finds():
    def http(url, auth=None, timeout=5):
        return (503, "down") if url.endswith("/api/health") else (200, '{"status": "OK"}')

    def kafka(bootstrap):
        raise RuntimeError("no broker")
    ctx = make_ctx(runner=FakeRunner(containers="fs-silver\n"), http=http, kafka=kafka)
    ctx.utcnow = lambda: NOW + datetime.timedelta(seconds=30)
    with pytest.raises(sm.StageFail) as e:
        sm.stage_preflight(ctx)
    text = str(e.value)
    for part in ("clocks differ", "fs-silver", "no broker", "/api/health answered 503"):
        assert part in text


def test_preflight_fails_when_the_data_source_is_not_ok():
    ctx = make_ctx(http=lambda url, auth=None, timeout=5: (200, '{"status": "ERROR"}') if "datasources" in url else (200, "{}"))
    with pytest.raises(sm.StageFail, match="fs-postgres"):
        sm.stage_preflight(ctx)


def test_host_tests_need_zero_failures_and_zero_errors():
    assert sm.stage_host_tests(make_ctx()) == "1200 passed, 0 failed, 0 errors, 3 skipped"
    for out, rc in (("2 failed, 5 passed in 1s", 1), ("5 passed, 1 error in 1s", 1), ("5 passed in 1s", 1), ("garbage", 0)):
        with pytest.raises(sm.StageFail):
            sm.stage_host_tests(make_ctx(runner=FakeRunner(outputs={"pytest": out}, rc={"pytest": rc})))


def test_spark_tests_give_one_row_per_file_and_run_one_container_each(monkeypatch):
    monkeypatch.setattr(sm, "spark_test_files", lambda root=sm.ROOT: ["test_a_spark.py", "test_b_spark.py"])
    runner = FakeRunner(outputs={"test_b_spark": "1 failed, 4 passed in 9s"}, rc={"test_b_spark": 1})
    rows = sm.stage_spark_tests(make_ctx(runner=runner))
    assert [(r.name, r.status) for r in rows] == [("spark tests: test_a_spark.py", sm.PASS), ("spark tests: test_b_spark.py", sm.FAIL)]
    assert "1200 passed" in rows[0].evidence and "1 failed, 0 errors" in rows[1].evidence
    assert all("RUN_SPARK_TESTS=1" in " ".join(c) and "PG_DSN=" in c for c in runner.calls)
    assert len(runner.calls) == 2


def test_the_real_spark_test_files_are_found():
    files = sm.spark_test_files()
    assert files and all("spark" in f for f in files) and "test_bronze_once_spark.py" in files


def test_static_checks_pass_on_the_repo_and_fail_on_a_generated_ddl_drift():
    assert "4 dashboards valid" in sm.stage_static(make_ctx())
    with pytest.raises(sm.StageFail, match="gen_ddl.py --check exited with code 1"):
        sm.stage_static(make_ctx(runner=FakeRunner(rc={"gen_ddl.py": 1})))


def test_reset_and_generate_recreates_kafka_the_database_and_the_data(monkeypatch, tmp_path):
    monkeypatch.setattr(sm, "SMOKE_DIR", str(tmp_path / "smoke"))
    removed = []
    runner, pg = FakeRunner(), FakePg(default=[(1,)])
    ctx = make_ctx(runner=runner, pg=pg, rmtree=removed.append)
    (tmp_path / "smoke").mkdir()
    out = sm.stage_reset_generate(ctx)
    lines = [" ".join(c) for c in runner.calls]
    order = [next(i for i, l in enumerate(lines) if k in l) for k in ("rm -sf kafka", "up -d kafka", "kafka-topics.sh", "create_topics.py",
                                                                    "apply_live_ddl.py", "run_simulator.py")]
    assert order == sorted(order) and pg.created and removed == [str(tmp_path / "smoke")]
    sim = next(c for c in runner.calls if "run_simulator.py" in " ".join(c))
    assert "--inventory" in sim and "--noise-off" not in sim and sim[sim.index("--seed") + 1] == "4242"
    assert sim[sim.index("--manifest") + 1] == "data/smoke/noise_manifest.json"
    assert "inventory.events=4000" in out and "total 24366" in out and ctx.state["sim_total"] == 24366


def test_reset_fails_when_a_topic_is_missing_or_inventory_is_empty(monkeypatch, tmp_path):
    monkeypatch.setattr(sm, "SMOKE_DIR", str(tmp_path / "smoke"))
    with pytest.raises(sm.StageFail, match="topics not created: inventory.events"):
        sm.stage_reset_generate(make_ctx(runner=FakeRunner(outputs={"create_topics": "created app.events\ncreated orders\ncreated delivery.events\n"})))
    zero = SIM_OUT.replace("'inventory.events': 4000", "'inventory.events': 0")
    with pytest.raises(sm.StageFail, match="inventory.events has no message"):
        sm.stage_reset_generate(make_ctx(runner=FakeRunner(outputs={"run_simulator": zero})))


def test_kafka_that_never_answers_fails_the_reset(monkeypatch, tmp_path):
    monkeypatch.setattr(sm, "SMOKE_DIR", str(tmp_path / "smoke"))
    with pytest.raises(sm.StageFail, match="did not answer"):
        sm.stage_reset_generate(make_ctx(runner=FakeRunner(rc={"kafka-topics.sh": 1})))


def test_bronze_and_silver_compare_the_counts():
    ctx = make_ctx(runner=FakeRunner())
    ctx.state["sim_total"] = 24366
    out = sm.stage_bronze_silver(ctx)
    assert "Bronze 24366 rows" in out and ctx.state["data_day"] == "2026-10-04"
    runner = FakeRunner()
    runner.counts = counts_json(bronze_total=24000, empty=("silver_stock_snapshots",))
    ctx = make_ctx(runner=runner)
    ctx.state["sim_total"] = 24366
    with pytest.raises(sm.StageFail) as e:
        sm.stage_bronze_silver(ctx)
    assert "differ from the simulator total" in str(e.value) and "silver_stock_snapshots" in str(e.value)


def invariants_pg(express=(100, 0, 0, 12.5, 20), negative=0, empty=(), beat=1):
    def count(sql):
        return [(0 if any(t in sql for t in empty) else 5,)]
    return FakePg([("available < 0", [(negative,)]), ("FROM live_express_orders", [express]), ("component = 'silver_job'", [(beat,)]),
                   ("count(*) FROM", count)])


def invariants_ctx(monkeypatch, tmp_path, inv=None, pg=None, dup_rc=0, runner_rc=None):
    monkeypatch.setattr(sm, "SMOKE_DIR", str(tmp_path))
    shutil.copy(FIXTURE, tmp_path / "noise_manifest.json")
    inv = inv or {"quarantine": 50, "pairs": 100, "negative_count": 0, "mismatch_count": 0, "negative": [], "mismatch": []}
    return make_ctx(runner=FakeRunner(outputs={"smoke_spark_checks.py invariants": "SMOKE_JSON " + json.dumps(inv)},
                                      rc=dict({"check_duplicates": dup_rc}, **(runner_rc or {}))), pg=pg or invariants_pg())


def test_invariants_pass_on_a_clean_run(monkeypatch, tmp_path):
    out = sm.stage_invariants(invariants_ctx(monkeypatch, tmp_path))
    for part in ("no duplicate key", "quarantine 50 (manifest 50)", "live rule = replay", "express: 100 orders", "heartbeat"):
        assert part in out


def test_invariants_fill_live_express_orders_from_silver_before_reading_it(monkeypatch, tmp_path):
    """C2: a history run is days old, the live sink only writes the last 3 hours, so the stage backfills the table."""
    ctx = invariants_ctx(monkeypatch, tmp_path)
    sm.stage_invariants(ctx)
    lines = [" ".join(c) for c in ctx.runner.calls]
    fills = [l for l in lines if "smoke_spark_checks.py express-backfill" in l]
    assert len(fills) == 1 and "--lake data/smoke/lake" in fills[0]
    with pytest.raises(sm.StageFail, match="express-backfill"):
        sm.stage_invariants(invariants_ctx(monkeypatch, tmp_path, runner_rc={"express-backfill": 1}))


def test_each_invariant_can_fail_on_its_own(monkeypatch, tmp_path):
    bad = {"quarantine": 80, "pairs": 100, "negative_count": 2, "mismatch_count": 1, "negative": ["s/p", "s/q"], "mismatch": ["s/r replay=1 live=2"]}
    with pytest.raises(sm.StageFail) as e:
        sm.stage_invariants(invariants_ctx(monkeypatch, tmp_path, inv=bad, pg=invariants_pg(express=(10, 2, 3, 25.0, 20), negative=4,
                                                                                           empty=("live_metrics_minute",), beat=0), dup_rc=1))
    text = str(e.value)
    for part in ("check_duplicates.py exited", "expected 50", "below zero for 2", "ledger replay for 1", "available < 0",
                 "no promised_minutes", "at or after their promise", "live_metrics_minute is empty", "no silver_job row"):
        assert part in text, part


def test_daily_marts_compare_the_closing_stock_with_the_live_stock():
    def pg(closing, live):
        return FakePg([("sum(closing_on_hand)", [(closing,)]), ("sum(l.on_hand)", [(live,)]), ("count(*) FROM", [(8,)])])
    ctx = make_ctx(args=_args("--skip-gold"), pg=pg(500, 500))
    ctx.state["data_day"] = "2026-10-04"
    out = sm.stage_daily_marts(ctx)
    assert "inventory_daily: 8 rows" in out and "D-1" not in out
    bad = make_ctx(args=_args("--skip-gold"), pg=pg(500, 499))
    bad.state["data_day"] = "2026-10-04"
    with pytest.raises(sm.StageFail, match="closing stock of 2026-10-04 is 500"):
        sm.stage_daily_marts(bad)
    with pytest.raises(sm.StageFail, match="unknown"):
        sm.stage_daily_marts(make_ctx())


def test_daily_marts_never_touch_databricks_even_without_skip_gold():
    runner = FakeRunner()
    ctx = make_ctx(runner=runner, pg=FakePg([("sum(closing_on_hand)", [(5,)]), ("sum(l.on_hand)", [(5,)]), ("count(*) FROM", [(8,)])]))
    ctx.state["data_day"] = "2026-10-04"
    sm.stage_daily_marts(ctx)
    lines = [" ".join(c) for c in runner.calls]
    assert any("build_inventory_daily.py" in l for l in lines)
    assert not any("build_d1_marketing.py" in l or "apply_ddl.py" in l or "DATABRICKS" in l for l in lines)
    assert "dbx_ddl_done" not in ctx.state


D1_OUT = "D-1 2026-10-04: %d campaign rows, %d product rows\n"


def d1_ctx(campaign_rows, product_rows, table_rows, out=None, rc=0):
    runner = FakeRunner(outputs={"build_d1_marketing": out if out is not None else D1_OUT % (campaign_rows, product_rows)},
                        rc={"build_d1_marketing": rc})
    pg = FakePg([("FROM marketing_d1_campaign", [(table_rows,)]), ("FROM marketing_d1_product", [(product_rows,)])])
    ctx = make_ctx(runner=runner, pg=pg)
    ctx.state["data_day"] = "2026-10-04"
    return ctx


def test_d1_stage_runs_the_build_against_the_scratch_catalog_and_counts_the_rows():
    ctx = d1_ctx(4, 3, 4)
    out = sm.stage_d1_marketing(ctx)
    line = " ".join(next(c for c in ctx.runner.calls if "build_d1_marketing.py" in " ".join(c)))
    assert "build_d1_marketing.py --date 2026-10-04" in line and "DATABRICKS_CATALOG=workspace" in line
    assert "marketing_d1_campaign: 4 rows" in out and "marketing_d1_product: 3 rows" in out


def test_d1_stage_accepts_an_empty_campaign_mart_only_with_the_stated_reason():
    out = sm.stage_d1_marketing(d1_ctx(0, 0, 0))
    assert "no last-touch ad row" in out and "2026-10-04" in out and "empty by design" in out
    with pytest.raises(sm.StageFail, match="prints 4 campaign rows but marketing_d1_campaign holds 2"):
        sm.stage_d1_marketing(d1_ctx(4, 3, 2))
    with pytest.raises(sm.StageFail, match="exited with code 1"):
        sm.stage_d1_marketing(d1_ctx(4, 3, 4, rc=1))
    with pytest.raises(sm.StageFail, match="no 'D-1"):
        sm.stage_d1_marketing(d1_ctx(4, 3, 4, out="garbage\n"))
    with pytest.raises(sm.StageFail, match="unknown"):
        sm.stage_d1_marketing(make_ctx())


def test_dashboards_run_every_panel_query_and_name_the_failures():
    pg = FakePg(default=[])
    ctx = make_ctx(pg=pg)
    out = sm.stage_dashboards(ctx)
    assert re.search(r"\d+ panel queries ran", out) and all(who == "ro" and "$__" not in sql for who, sql in pg.calls)
    # M5: the data is days old, so a 1-day window would leave most panels empty; the window must cover it
    assert any("interval '30 days'" in sql for _, sql in pg.calls) and not any("interval '1 day'" in sql for _, sql in pg.calls)

    def broken(sql):
        raise RuntimeError('relation "nope" does not exist')
    ctx = make_ctx(pg=FakePg([("live_stock_now", broken)], default=[]))
    with pytest.raises(sm.StageFail) as e:
        sm.stage_dashboards(ctx)
    assert "failed" in str(e.value) and "inventory-ops.json /" in str(e.value) and "does not exist" in str(e.value)


class AlertPg:
    """Fake smoke database for the alert stage. The express rule answers from the backfilled clean data and from the
    tiny run (`state`); the demand rule answers from the controlled rows the stage writes itself."""

    def __init__(self, state, clean_breach=False):
        self.state, self.clean_breach, self.calls, self.carts = state, clean_breach, [], None

    def query(self, who, sql):
        self.calls.append((who, sql))
        if sql.startswith("SELECT max(coalesce(delivered_at"):
            if "promised_minutes = 2" in sql:
                return [(NOW,)] if self.state.get("sim2") else [(None,)]
            return [(NOW,)] if self.state.get("backfill") else [(None,)]
        if sql.startswith("INSERT INTO live_oos_demand_minute"):
            assert who == "smoke" and "true" in sql
            self.carts = 5
            return []
        if sql.startswith("UPDATE live_oos_demand_minute"):
            assert who == "smoke"
            self.carts = 4
            return []
        if sql.startswith("DELETE FROM live_oos_demand_minute"):
            assert who == "smoke"
            self.carts = None
            return []
        if "FROM live_express_orders" in sql:
            assert "TIMESTAMPTZ" in sql and "now()" not in sql
            return [(NOW, 3 if self.state.get("sim2") or self.clean_breach else 0)]
        if "FROM live_oos_demand_minute" in sql:
            pinned = "TIMESTAMPTZ" in sql
            return [(NOW, 2 if pinned and (self.carts or 0) >= 5 else 0)]
        if "FROM pipeline_health_minute" in sql and "late_events" in sql:
            return [(NOW, 100.0)]
        return [(NOW, 7)]


def alert_ctx(clean_breach=False, sim2_rc=0):
    state = {}

    class R(FakeRunner):
        def __call__(self, argv, **kw):
            line = " ".join(argv)
            if "run_simulator.py" in line:
                state["sim2"] = True
            if "express-backfill" in line:
                state["backfill"] = True
            return super().__call__(argv, **kw)
    ctx = make_ctx(runner=R(rc={"--scenario": sim2_rc}), pg=AlertPg(state, clean_breach))
    return ctx, state


def test_alerts_stay_quiet_on_the_clean_run_and_fire_on_the_breach_run():
    ctx, state = alert_ctx()
    out = sm.stage_alerts(ctx)
    assert "clean:" in out and "breach run:" in out and "demand rule on controlled rows:" in out
    clean, rest = out.split(" | breach run: ")
    after, control = rest.split(" | demand rule on controlled rows: ")
    assert "fs-express-sla=0 not firing" in clean and "fs-express-sla=3 firing" in after
    assert "fires with 5 add-to-carts" in control and "quiet with 4" in control
    assert "fs-late-rate=100 firing" in after  # reported, not asserted: history data is late by rule
    sim = next(c for c in ctx.runner.calls if "run_simulator.py" in " ".join(c))
    for flag, value in (("--express-promise-minutes", "2"), ("--late-express-every", "1")):
        assert sim[sim.index(flag) + 1] == value
    assert "--scenario" in sim and sim[sim.index("--scenario") + 1] == "hero-stockout" and "--noise-off" in sim
    assert sim[sim.index("--start") + 1] == "2026-10-05T12:00:00+00:00"
    lines = [" ".join(c) for c in ctx.runner.calls]
    fills = [i for i, l in enumerate(lines) if "express-backfill" in l]
    sim_at = next(i for i, l in enumerate(lines) if "run_simulator.py" in l)
    assert len(fills) == 2 and fills[0] < sim_at < fills[1]  # the clean data before the tiny run, the tiny data after it
    assert "silver_clean" in lines[fills[1] - 1]
    assert ctx.pg.carts is None  # the controlled rows are removed again


def test_alerts_fail_when_a_rule_fires_on_the_clean_run():
    ctx, _ = alert_ctx(clean_breach=True)
    with pytest.raises(sm.StageFail, match="fs-express-sla fires on the clean run"):
        sm.stage_alerts(ctx)


def test_alerts_fail_when_the_breach_does_not_fire():
    class Quiet(AlertPg):
        def query(self, who, sql):
            if "FROM live_express_orders" in sql and not sql.startswith("SELECT max"):
                return [(NOW, 0)]
            return super().query(who, sql)
    ctx, state = alert_ctx()
    ctx.pg = Quiet(state)
    with pytest.raises(sm.StageFail, match="fs-express-sla does not fire on the late express orders"):
        sm.stage_alerts(ctx)


def test_alerts_do_not_pass_when_there_is_no_express_data_to_evaluate():
    ctx, state = alert_ctx()

    class Empty(AlertPg):
        def query(self, who, sql):
            if sql.startswith("SELECT max(coalesce(delivered_at"):
                return [(None,)]
            if "FROM live_express_orders" in sql:  # no instant to pin: the rule runs at the real now() and finds no row
                return []
            return super().query(who, sql)
    ctx.pg = Empty(state)
    with pytest.raises(sm.StageFail) as e:
        sm.stage_alerts(ctx)
    assert "no express order" in str(e.value)


def test_the_demand_rule_failing_on_the_controlled_rows_is_reported_and_the_rows_are_removed():
    class Dead(AlertPg):
        def query(self, who, sql):
            if "FROM live_oos_demand_minute" in sql and "TIMESTAMPTZ" in sql:
                return [(NOW, 0)]
            return super().query(who, sql)
    ctx, state = alert_ctx()
    ctx.pg = Dead(state)
    with pytest.raises(sm.StageFail, match="fs-oos-demand does not fire on 5 add-to-carts"):
        sm.stage_alerts(ctx)
    assert ctx.pg.carts is None


def test_the_demand_rule_firing_below_its_threshold_is_reported():
    class Loud(AlertPg):
        def query(self, who, sql):
            if "FROM live_oos_demand_minute" in sql and "TIMESTAMPTZ" in sql:
                return [(NOW, 2)]
            return super().query(who, sql)
    ctx, state = alert_ctx()
    ctx.pg = Loud(state)
    with pytest.raises(sm.StageFail, match="fs-oos-demand fires on 4 add-to-carts"):
        sm.stage_alerts(ctx)


def test_replay_deletes_only_the_silver_checkpoint_and_compares_the_tables():
    removed = []
    runner = FakeRunner()
    ctx = make_ctx(runner=runner, rmtree=removed.append)
    out = sm.stage_replay(ctx)
    assert removed == [os.path.join(sm.ROOT, "data/smoke/checkpoints", "silver_clean")] and "unchanged" in out

    class Drift(FakeRunner):
        n = 0

        def __call__(self, argv, **kw):
            if "smoke_spark_checks.py counts" in " ".join(argv):
                Drift.n += 1
                self.counts = counts_json()
                if Drift.n == 2:
                    self.counts["silver"]["silver_events"] = 11
            return super().__call__(argv, **kw)
    with pytest.raises(sm.StageFail, match="silver_events: 10 rows before the replay, 11 after"):
        sm.stage_replay(make_ctx(runner=Drift()))


def test_databricks_stage_needs_batch_ok_and_passing_checks():
    runner = FakeRunner()
    out = sm.stage_databricks(make_ctx(runner=runner))
    assert "BATCH OK in catalog workspace" in out and "56 run, 0 failed" in out
    publish = next(c for c in runner.calls if "publish_silver.py" in " ".join(c))
    assert "DATABRICKS_CATALOG=workspace" in publish and "PG_DSN=" in publish
    with pytest.raises(sm.StageFail, match="no BATCH OK"):
        sm.stage_databricks(make_ctx(runner=FakeRunner(outputs={"run_batch": "BATCH FAILED at checks"})))
    with pytest.raises(sm.StageFail, match="exited with code 1"):
        sm.stage_databricks(make_ctx(runner=FakeRunner(rc={"run_batch": 1})))
    ctx = make_ctx(runner=FakeRunner())
    ctx.state["dbx_ddl_done"] = True
    sm.stage_databricks(ctx)
    assert not any("apply_ddl.py" in " ".join(c) for c in ctx.runner.calls)
