"""Host tests for the SQL checks and the manifest check (warehouse/checks.py). Fake store only: no Spark, no Databricks.

The SQL itself runs on local Spark with Delta in tests/test_checks_spark.py.
"""
import json
import os
import re
import types

import pytest

import scripts.run_batch as run_batch
import warehouse.checks as ck
import warehouse.gold_build as gb
from tests.test_gold_build import FakeStore as BuildStore, WINDOW
from warehouse.checks import CHECKS, Check, run_checks
from warehouse.ddl_render import split_statements
from warehouse.gold_build import GoldBuildError, main, placeholders, run_build
from warehouse.tables import GOLD_TABLES, SILVER_MIRROR_TABLES

FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "noise_manifest_fixture.json")  # fixed numbers, not the last run
VALUES = placeholders("funnelstream.silver", "funnelstream.gold", "2026-10-01", "2026-10-03")
TAG = re.compile(r"^/\* funnelstream check (\S+) \*/ ")
WRITE_WORDS = re.compile(r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|SET|GRANT|COPY|OPTIMIZE|VACUUM)\b",
                         re.I)


class CheckStore:
    """Answers every check query from a dict {check name: value} (default 0); records the queries."""

    def __init__(self, values=None, fail_on=None, default=0):
        self.values, self.fail_on, self.default = values or {}, fail_on, default
        self.queries = []

    def schema_prefix(self, group):
        return "funnelstream." + group

    def query(self, sql, params=None, retry=True):
        self.queries.append(sql)
        name = TAG.match(sql).group(1)
        if name == self.fail_on:
            raise RuntimeError("boom in %s" % name)
        return [(self.values.get(name, self.default),)]

    def execute(self, sql, retry=True):
        raise AssertionError("checks must never execute a statement: %s" % sql)

    def close(self):
        pass


def names():
    return [c.name for c in CHECKS]


# ---------------------------------------------------------------- the check list
def test_names_are_unique_and_groups_and_severities_are_valid():
    assert len(set(names())) == len(CHECKS) > 40
    for c in CHECKS:
        assert c.severity in ("fail", "report") and c.group in ck.GROUPS and c.description
        assert c.name == c.name.strip() and " " not in c.name
    with pytest.raises(ValueError):
        Check("x", "keys", "warn", "SELECT 1", "d")
    with pytest.raises(ValueError):
        Check("x", "nope", "fail", "SELECT 1", "d")


def test_the_five_groups_are_all_there_and_the_reported_group_never_fails():
    by = {}
    for c in CHECKS:
        by.setdefault(c.group, []).append(c)
    assert set(by) == {"keys", "reconciliation", "silver_hygiene", "hierarchy_history", "reported"}
    assert all(c.severity == "report" for c in by["reported"])
    assert [c.name for c in by["keys"] if c.severity == "report"] == ["unknown_customer_share"]
    assert all(c.severity == "fail" for g in ("reconciliation", "silver_hygiene", "hierarchy_history") for c in by[g])
    reported = {c.name for c in by["reported"]}
    assert {"report.late_rate", "report.quarantine_share", "report.negative_delivery_lead_hours",
            "report.cancelled_with_shipment", "report.open_over_14_days",
            "report.direct_attribution_share"} <= reported
    assert {c.name for c in by["reported"] if c.name.startswith("rows.")} == {"rows." + t for t in ck.GOLD_KEYS}


def test_every_check_renders_without_stray_placeholders_and_is_one_read_only_select():
    full = ck.check_values(VALUES)
    for c in CHECKS:
        assert set(re.findall(r"\{(\w+)\}", c.sql)) <= set(ck.PLACEHOLDERS), c.name
        text = ck.render_check(c, VALUES)
        assert TAG.match(text), c.name
        body = TAG.sub("", text)
        assert not re.search(r"[{}]", body), c.name
        assert body.startswith("SELECT "), c.name
        assert len(split_statements(body)) == 1 and ";" not in body, c.name
        assert not WRITE_WORDS.search(re.sub(r"'[^']*'", "''", body)), (c.name, WRITE_WORDS.search(body))
        assert "%" not in body, c.name  # no percent signs: nothing for a driver to format
    assert set(full) >= {"silver", "gold", "district_pairs"}


def test_unknown_placeholder_is_rejected():
    bad = Check("bad", "keys", "fail", "SELECT COUNT(*) FROM {nope}.t", "d")
    with pytest.raises(ValueError, match="unknown placeholder"):
        ck.render_check(bad, VALUES)


def test_every_table_and_key_column_named_by_the_checks_exists():
    gold = {t.name: t for t in GOLD_TABLES}
    silver = {t.name: t for t in SILVER_MIRROR_TABLES}
    for c in CHECKS:
        for group, table in re.findall(r"\{(silver|gold)\}\.(\w+)", c.sql):
            assert table in (silver if group == "silver" else gold), (c.name, table)
    assert {t: tuple(k) for t, k in ck.GOLD_KEYS.items()} == {
        t.name: t.keys for t in GOLD_TABLES if t.name in ck.GOLD_KEYS}
    assert ck.SILVER_KEYS == {t.name: t.keys for t in SILVER_MIRROR_TABLES if t.keys}
    assert [t.name for t in SILVER_MIRROR_TABLES if not t.keys] == ["silver_quarantine"]
    assert set(ck.GOLD_KEYS) == {t.name for t in GOLD_TABLES} - {"rec_product", "customer_360"}  # not built yet
    for t, keys in list(ck.GOLD_KEYS.items()) + list(ck.SILVER_KEYS.items()):
        spec = gold.get(t) or silver[t]
        assert set(keys) <= set(spec.column_names()), t


def test_status_list_comes_from_the_silver_contract():
    from streaming.common.status import STATUS_RANK
    sql = next(c.sql for c in CHECKS if c.name == "silver.order_status_not_allowed")
    assert set(re.findall(r"'(\w+)'", sql)) == set(STATUS_RANK)


def test_district_pairs_are_the_pairs_normalize_district_accepts():
    from contracts.geo import DISTRICTS, normalize_district
    pairs = set(re.findall(r"\('([^']+)', '([^']+)'\)", ck.district_pairs_values()))
    assert ("Cairo", "maadi") in pairs and ("Cairo", "el maadi") in pairs and ("Giza", "maadi") not in pairs
    assert ("Other", "other") in pairs and ("Giza", "giza centre") in pairs
    for city, key in pairs:
        assert normalize_district(city, key) != "unknown"
    for city, districts in DISTRICTS.items():  # and every canonical spelling is in, in its cleaned form
        for d in districts:
            assert (city, re.sub(r"[^a-z0-9]+", " ", d.lower()).strip()) in pairs


# ---------------------------------------------------------------- run_checks
def test_all_zero_is_ok_and_runs_everything_read_only():
    store = CheckStore()
    out = run_checks(store, VALUES, heartbeat=False)
    assert out["status"] == "ok" and out["failed"] == [] and out["rows"] is None
    assert out["statements"] == len(CHECKS) + 0 and len(store.queries) == len(CHECKS)
    assert [r["name"] for r in out["results"]][:len(CHECKS)] == names()
    assert out["results"][-1]["message"] == "manifest: skipped (no --manifest)"
    assert out["results"][-1]["status"] == "skipped"
    assert all(r["status"] in ("ok", "report", "skipped") for r in out["results"])


def test_one_failing_check_fails_the_step_and_the_others_still_run():
    store = CheckStore({"recon.orders_count": 2, "dup_key.silver_events": 1})
    out = run_checks(store, VALUES, heartbeat=False)
    assert out["status"] == "failed" and out["failed"] == ["dup_key.silver_events", "recon.orders_count"]
    assert len(store.queries) == len(CHECKS)  # it did not stop at the first failure
    failed = {r["name"]: r["value"] for r in out["results"] if r["status"] == "failed"}
    assert failed == {"dup_key.silver_events": 1, "recon.orders_count": 2}


def test_a_report_check_with_a_value_never_fails():
    store = CheckStore({"report.late_rate": 0.9, "report.negative_delivery_lead_hours": 12, "rows.fact_orders": 5,
                        "unknown_customer_share": 0.5})
    out = run_checks(store, VALUES, heartbeat=False)
    assert out["status"] == "ok" and out["failed"] == []
    got = {r["name"]: (r["status"], r["value"]) for r in out["results"]}
    assert got["report.late_rate"] == ("report", 0.9) and got["report.negative_delivery_lead_hours"] == ("report", 12)


def test_decimal_values_and_null_reports_are_handled_and_a_null_fail_check_is_an_error():
    from decimal import Decimal
    out = run_checks(CheckStore({"report.late_rate": Decimal("0.0123"), "rows.dim_date": None}), VALUES,
                     heartbeat=False)
    got = {r["name"]: r["value"] for r in out["results"]}
    assert got["report.late_rate"] == pytest.approx(0.0123) and got["rows.dim_date"] is None
    with pytest.raises(GoldBuildError, match="returned NULL"):
        run_checks(CheckStore({"recon.orders_count": None}), VALUES, heartbeat=False)


def test_a_query_error_raises_a_gold_build_error_naming_the_check():
    store = CheckStore(fail_on="scd2.dim_product.no_overlap")
    with pytest.raises(GoldBuildError) as exc:
        run_checks(store, VALUES, heartbeat=False)
    e = exc.value
    assert e.step == "checks" and "scd2.dim_product.no_overlap" in str(e) and "boom" in str(e)
    assert isinstance(e.__cause__, RuntimeError)
    assert [r["name"] for r in e.check_results] == names()[:names().index("scd2.dim_product.no_overlap")]
    with pytest.raises(GoldBuildError, match="one row"):
        run_checks(types.SimpleNamespace(query=lambda sql, params=None, retry=True: []), VALUES, heartbeat=False)


# ---------------------------------------------------------------- heartbeat
def test_heartbeat_is_written_once_with_the_details(monkeypatch):
    beats = []
    monkeypatch.setattr("warehouse.heartbeat.beat", lambda c, d=None, **k: beats.append((c, d)) or True)
    run_checks(CheckStore(), VALUES)
    assert beats == [("checks", "ok")]
    beats.clear()
    run_checks(CheckStore({"recon.orders_count": 1, "orphan.fact_orders.customer_key": 4}), VALUES)
    assert beats == [("checks", "failed: orphan.fact_orders.customer_key,recon.orders_count")]


def test_no_heartbeat_when_a_query_cannot_run_and_a_heartbeat_error_is_swallowed(monkeypatch):
    beats = []
    monkeypatch.setattr("warehouse.heartbeat.beat", lambda c, d=None, **k: beats.append(c) or True)
    with pytest.raises(GoldBuildError):
        run_checks(CheckStore(fail_on="recon.orders_count"), VALUES)
    assert beats == []

    def broken(*a, **k):
        raise RuntimeError("pg down")
    monkeypatch.setattr("warehouse.heartbeat.beat", broken)
    assert run_checks(CheckStore(), VALUES)["status"] == "ok"


def test_the_real_beat_function_never_raises():
    from warehouse.heartbeat import beat

    def factory():
        raise RuntimeError("down")
    assert beat("checks", "ok", store_factory=factory) is False


# ---------------------------------------------------------------- manifest
def manifest(path, **injected):
    base = dict(duplicate=25, malformed_json=0, missing_field=0, unknown_type=0, bad_version=0, negative_price=0,
                future_timestamp=0, null_product_id=0, unknown_product_id=0, cancel_after_ship=0, late=49, tz_offset=28)
    base.update(injected)
    path.write_text(json.dumps({"total_messages": 100, "injected": base}), encoding="utf-8")
    return str(path)


def test_expected_quarantine_is_the_sum_of_the_nine_kinds_and_ignores_the_rest():
    repo = json.load(open(FIXTURE, encoding="utf-8"))["injected"]
    assert ck.expected_quarantine(repo) == 9 + 6 + 2 + 3 + 2 + 4 + 16 + 8 + 0 == 50
    assert ck.MANIFEST_QUARANTINE_KINDS == ("malformed_json", "missing_field", "unknown_type", "bad_version",
                                            "negative_price", "future_timestamp", "null_product_id",
                                            "unknown_product_id", "cancel_after_ship")
    assert ck.expected_quarantine(dict(repo, duplicate=999, late=999, dirty_text=999)) == 50


@pytest.mark.parametrize("observed,expected,ok", [
    (100, 100, True), (102, 100, True), (98, 100, True), (103, 100, False), (97, 100, False),
    (50, 50, True), (51, 50, True), (52, 50, False), (49, 50, True), (48, 50, False),  # 2% of 50 is exactly 1
    (0, 0, True), (1, 0, False),  # expected 0: exactly 0 is allowed
    (1, 49, False), (49, 49, True), (0, 49, False), (0, 50, False), (51, 100, False)])
def test_tolerance_is_two_percent_of_the_manifest_count_inclusive(observed, expected, ok):
    assert ck.within_tolerance(observed, expected) is ok


def manifest_run(tmp_path, observed, **injected):
    store = CheckStore({"manifest.quarantine_count": observed})
    path = manifest(tmp_path / "m.json", **injected)
    return run_checks(store, VALUES, manifest_path=path, heartbeat=False), store


def test_manifest_within_tolerance_passes_and_outside_fails_the_step(tmp_path):
    out, store = manifest_run(tmp_path, 51, malformed_json=50)
    q = [r for r in out["results"] if r["name"] == "manifest.quarantine_count"][0]
    assert out["status"] == "ok" and q["status"] == "ok" and q["value"] == 51 and q["expected"] == 50
    assert "expected 50, observed 51" in q["message"]
    assert len(store.queries) == len(CHECKS) + 1 and out["statements"] == len(CHECKS) + 1
    assert store.queries[-1].endswith("SELECT COUNT(*) FROM funnelstream.silver.silver_quarantine")
    out, _ = manifest_run(tmp_path, 40, malformed_json=50)
    assert out["status"] == "failed" and out["failed"] == ["manifest.quarantine_count"]


def test_manifest_with_zero_expected_allows_only_zero(tmp_path):
    assert manifest_run(tmp_path, 0)[0]["status"] == "ok"
    assert manifest_run(tmp_path, 1)[0]["failed"] == ["manifest.quarantine_count"]


def test_manifest_duplicates_are_reported_as_not_checkable(tmp_path):
    out, _ = manifest_run(tmp_path, 0, duplicate=25)
    d = [r for r in out["results"] if r["name"] == "manifest.duplicates"][0]
    assert d["status"] == "not checkable" and d["message"] == "duplicates: not checkable on the mirror"
    assert "25" in d["description"] and out["status"] == "ok"


def test_manifest_absent_is_skipped_and_reported():
    out = run_checks(CheckStore(), VALUES, heartbeat=False)
    last = out["results"][-1]
    assert last["status"] == "skipped" and last["message"] == "manifest: skipped (no --manifest)"
    assert "manifest.quarantine_count" not in [r["name"] for r in out["results"]]


def test_manifest_missing_file_and_malformed_json_and_wrong_shape(tmp_path):
    with pytest.raises(ck.ManifestError, match="not found"):
        ck.load_manifest(str(tmp_path / "nope.json"))
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    with pytest.raises(ck.ManifestError, match="cannot be read"):
        ck.load_manifest(str(bad))
    for content in ("[]", '{"injected": 3}', '{"injected": {"duplicate": 1}}',
                    json.dumps({"injected": {k: -1 for k in ck.MANIFEST_QUARANTINE_KINDS + ("duplicate",)}}),
                    json.dumps({"injected": {k: "5" for k in ck.MANIFEST_QUARANTINE_KINDS + ("duplicate",)}})):
        bad.write_text(content, encoding="utf-8")
        with pytest.raises(ck.ManifestError):
            ck.load_manifest(str(bad))
    assert issubclass(ck.ManifestError, ValueError)
    with pytest.raises(GoldBuildError):  # inside the run it is a technical error of the checks step
        run_checks(CheckStore(), VALUES, manifest_path=str(bad), heartbeat=False)


def test_the_repo_manifest_loads():
    assert ck.load_manifest(FIXTURE)["duplicate"] == 25


# ---------------------------------------------------------------- the build step
def test_checks_step_in_run_build_returns_ok_and_runs_after_everything(monkeypatch):
    monkeypatch.setattr("warehouse.heartbeat.beat", lambda *a, **k: True)
    store = BuildStore()
    result = run_build(store, **WINDOW)
    step = result["steps"]["checks"]
    assert step["status"] == "ok" and step["failed"] == [] and step["statements"] == len(CHECKS)
    check_queries = [q for q in store.queries if q.startswith("/* funnelstream check ")]
    assert len(check_queries) == len(CHECKS)
    assert store.queries.index(check_queries[0]) > max(i for i, q in enumerate(store.queries)
                                                      if q.startswith("SELECT count(*) FROM funnelstream.gold."))
    assert not any(s.startswith("/*") for s in store.executed)


def test_checks_step_skipped_when_not_selected_and_a_failed_check_does_not_raise(monkeypatch):
    monkeypatch.setattr("warehouse.heartbeat.beat", lambda *a, **k: True)
    r = run_build(BuildStore(), steps=["dim_date"], **WINDOW)
    assert r["steps"]["checks"] == {"status": "skipped", "statements": 0, "rows": None}

    class Bad(BuildStore):
        def query(self, sql, params=None, retry=True):
            if "check recon.orders_count " in sql:
                return [(3,)]
            return super().query(sql, params, retry)
    r = run_build(Bad(), steps=["checks"], **WINDOW)
    assert r["steps"]["checks"]["status"] == "failed" and r["steps"]["checks"]["failed"] == ["recon.orders_count"]


def test_a_query_error_in_the_checks_step_is_a_gold_build_error_with_the_finished_steps(monkeypatch):
    monkeypatch.setattr("warehouse.heartbeat.beat", lambda *a, **k: True)

    class Boom(BuildStore):
        def query(self, sql, params=None, retry=True):
            if "check recon.lines_count " in sql:
                raise RuntimeError("no such table")
            return super().query(sql, params, retry)
    with pytest.raises(GoldBuildError) as exc:
        run_build(Boom(), **WINDOW)
    assert exc.value.step == "checks" and "recon.lines_count" in str(exc.value)
    assert "agg_city_product_daily" in exc.value.results


def test_dry_run_prints_check_names_and_sql_and_runs_nothing(capsys, monkeypatch):
    for n in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID", "DATABRICKS_HTTP_PATH",
              "DATABRICKS_CATALOG", "GOLD_BACKEND"):
        monkeypatch.delenv(n, raising=False)
    beats = []
    monkeypatch.setattr("warehouse.heartbeat.beat", lambda *a, **k: beats.append(a))
    assert main(["--dry-run", "--steps", "checks"]) == 0
    out = capsys.readouterr().out
    assert "-- step checks: %d check query(ies), nothing executed; manifest: skipped (no --manifest)" % len(CHECKS) in out
    for c in CHECKS:
        assert "-- check %s\n/* funnelstream check %s */ SELECT" % (c.name, c.name) in out
    assert "funnelstream.silver.silver_events" in out and beats == []
    store = BuildStore()
    r = run_build(store, dry_run=True, steps=["checks"], manifest_path="noise_manifest.json", **WINDOW)
    assert store.queries == [] and store.executed == []
    assert r["steps"]["checks"]["names"][-1] == "manifest.quarantine_count"
    assert r["steps"]["checks"]["manifest"] == "manifest: noise_manifest.json"


# ---------------------------------------------------------------- gold_build main
def test_main_exits_1_on_a_failed_check_and_prints_each_failed_check(monkeypatch, capsys):
    beats = []
    monkeypatch.setattr("warehouse.heartbeat.beat", lambda c, d=None, **k: beats.append((c, d)) or True)

    class Bad(BuildStore):
        def query(self, sql, params=None, retry=True):
            for n in ("recon.orders_count", "orphan.fact_orders.customer_key"):
                if "check %s " % n in sql:
                    return [(2,)]
            return super().query(sql, params, retry)
    monkeypatch.setattr(gb, "get_store", lambda backend: Bad())
    assert main(["--days", "2"]) == 1
    c = capsys.readouterr()
    assert "failed check: recon.orders_count" in c.err and "failed check: orphan.fact_orders.customer_key" in c.err
    assert "2 check(s) failed" in c.err
    assert "FAILED" in c.out and "checks: %d run, 2 failed" % (len(CHECKS) + 1) in c.out
    assert ("checks", "failed: orphan.fact_orders.customer_key,recon.orders_count") in beats
    assert [b for b in beats if b[0] == "gold_build"]  # the build steps that finished still beat


def test_main_exits_0_when_all_checks_pass_and_checks_only_run_does_not_beat_gold_build(monkeypatch, capsys):
    beats = []
    monkeypatch.setattr("warehouse.heartbeat.beat", lambda c, d=None, **k: beats.append(c) or True)
    monkeypatch.setattr(gb, "get_store", lambda backend: BuildStore())
    assert main(["--steps", "checks"]) == 0
    out = capsys.readouterr().out
    assert "checks                   ok" in out and "checks: %d run, 0 failed" % (len(CHECKS) + 1) in out
    assert beats == ["checks"]  # not gold_build: nothing was built


def test_main_with_a_bad_manifest_is_a_usage_error(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(gb, "get_store", lambda backend: BuildStore())
    assert main(["--steps", "checks", "--manifest", str(tmp_path / "nope.json")]) == 2
    assert "manifest file not found" in capsys.readouterr().err


# ---------------------------------------------------------------- scripts/run_batch.py integration
@pytest.fixture
def batch(monkeypatch):
    e = types.SimpleNamespace(store=None, beats=[])
    monkeypatch.setattr(run_batch, "get_store", lambda b: e.store)
    monkeypatch.setattr("warehouse.heartbeat.beat", lambda c, d=None, **k: e.beats.append((c, d)) or True)
    e.store = BuildStore()
    return e


def test_run_batch_succeeds_when_the_real_checks_pass(batch, capsys):
    assert run_batch.main(["--skip-publish", "--days", "3"]) == 0
    out = capsys.readouterr().out
    assert "checks status: ok" in out and "BATCH OK" in out and "checks: %d run, 0 failed" % (len(CHECKS) + 1) in out
    assert ("checks", "ok") in batch.beats


def test_run_batch_fails_at_checks_on_a_failed_check_and_shows_which(batch, capsys):
    class Bad(BuildStore):
        def query(self, sql, params=None, retry=True):
            if "check silver.snapshots_negative_spend " in sql:
                return [(7,)]
            return super().query(sql, params, retry)
    batch.store = Bad()
    assert run_batch.main(["--skip-publish", "--days", "3"]) == 1
    c = capsys.readouterr()
    assert "BATCH FAILED at checks: failed checks: silver.snapshots_negative_spend" in c.err
    assert "phases finished: gold" in c.err and "BATCH OK" not in c.out
    assert "check silver.snapshots_negative_spend" in c.out and "FAILED" in c.out and "value=7" in c.out


def test_run_batch_fails_at_checks_when_a_check_query_cannot_run(batch, capsys):
    class Boom(BuildStore):
        def query(self, sql, params=None, retry=True):
            if "check scd2.dim_customer.one_current " in sql:
                raise RuntimeError("table not found")
            return super().query(sql, params, retry)
    batch.store = Boom()
    assert run_batch.main(["--skip-publish"]) == 1
    assert "BATCH FAILED at checks" in capsys.readouterr().err


def test_run_batch_manifest_failure_fails_the_batch(batch, capsys, tmp_path):
    class Q(BuildStore):
        def query(self, sql, params=None, retry=True):
            if sql.endswith("FROM funnelstream.silver.silver_quarantine") and "manifest.quarantine_count" in sql:
                return [(80,)]
            return super().query(sql, params, retry)
    batch.store = Q()
    path = manifest(tmp_path / "m.json", malformed_json=50)
    assert run_batch.main(["--skip-publish", "--manifest", path]) == 1
    assert "failed checks: manifest.quarantine_count" in capsys.readouterr().err
    assert run_batch.main(["--skip-publish"]) == 0  # without the manifest the check is skipped


def test_run_batch_dry_run_plans_the_checks_and_reports_the_manifest_as_skipped(batch, capsys):
    assert run_batch.main(["--skip-publish", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "checks status: dry-run" in out
    assert "checks planned: %d query(ies), manifest: skipped (no --manifest)" % len(CHECKS) in out
    assert batch.store.queries == [] and batch.store.executed == [] and batch.beats == []
