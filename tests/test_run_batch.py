"""Host tests for scripts/run_batch.py (fake store, fake publish, no Spark, no Databricks)."""
import types

import pytest

import scripts.publish_silver as publish_silver
import scripts.run_batch as run_batch
from warehouse import gold_build

TOKEN = "dapi-SECRET-TOKEN-123"


class FakeStore:
    closed = False

    def schema_prefix(self, group):
        return "funnelstream." + group

    def close(self):
        self.closed = True


def _step(status="ok", rows=None, **extra):
    d = {"status": status, "statements": 1, "rows": rows}
    d.update(extra)
    return d


def _build_result(steps, dry_run=False):
    out = {"window": {"from": "2026-10-01", "to": "2026-10-07", "full": False}, "dry_run": dry_run,
           "steps": {n: _step("skipped") for n in gold_build.STEPS}, "reported": {}}
    for n in steps:
        out["steps"][n] = _step("dry-run" if dry_run else "ok", None if dry_run else 5)
    return out


def _pub_args(argv):
    return types.SimpleNamespace(dry_run="--dry-run" in argv, export_only=False)


@pytest.fixture
def env(monkeypatch):
    e = types.SimpleNamespace(calls=[], beats=[], store=FakeStore(), checks=None)
    monkeypatch.setenv("DATABRICKS_TOKEN", TOKEN)
    monkeypatch.setattr(run_batch, "get_store", lambda b: e.store)
    monkeypatch.setattr("warehouse.heartbeat.beat", lambda c, d=None, **k: e.beats.append(c) or True)

    def fake_pub(argv):
        e.calls.append(("publish", list(argv)))
        return 0, {"silver_events": {"rows": 10, "files": 1, "merged": True}}, _pub_args(argv)

    def fake_build(store, steps=None, **kw):
        e.calls.append(("build", list(steps), kw))
        if steps == ["checks"]:
            r = _build_result([], kw["dry_run"])
            r["steps"]["checks"] = _step("empty") if e.checks is None else e.checks
            return r
        r = _build_result(steps, kw["dry_run"])
        r["reported"] = {"orders_direct": 3}
        return r

    monkeypatch.setattr(publish_silver, "run", fake_pub)
    monkeypatch.setattr(gold_build, "run_build", fake_build)
    return e


def test_order_and_summary(env, capsys):
    assert run_batch.main([]) == 0
    assert [c[0] for c in env.calls] == ["publish", "build", "build"]
    assert env.calls[1][1] == [s for s in gold_build.STEPS if s != "checks"]
    assert env.calls[2][1] == ["checks"]
    out = capsys.readouterr().out
    assert "BATCH OK" in out and "rows=10" in out and "reported orders_direct: 3" in out
    assert "phase publish" in out and "phase gold" in out and "checks: not built yet (T16)" in out
    assert env.store.closed


def test_heartbeats_written_once_each(env):
    assert run_batch.main([]) == 0
    assert env.beats == ["publish", "gold_build"]


def test_publish_report_beats_once_and_not_on_dry_run(monkeypatch):
    beats = []
    monkeypatch.setattr("warehouse.heartbeat.beat", lambda c, d=None, **k: beats.append(c))
    res = {"silver_events": {"rows": 1, "files": 1, "merged": True}}
    publish_silver.report(res, _pub_args([]))
    assert beats == ["publish"]
    publish_silver.report(res, _pub_args(["--dry-run"]))
    assert beats == ["publish"]


def test_heartbeat_failure_never_fails_batch(env, monkeypatch):
    def broken(*a, **k):
        raise RuntimeError("pg down")
    monkeypatch.setattr("warehouse.heartbeat.beat", broken)
    assert run_batch.main([]) == 0


def test_real_beat_swallows_errors():
    from warehouse.heartbeat import beat

    def factory():
        raise RuntimeError("down")
    assert beat("x", store_factory=factory) is False


def test_skip_publish(env):
    assert run_batch.main(["--skip-publish"]) == 0
    assert [c[0] for c in env.calls] == ["build", "build"]
    assert env.beats == ["gold_build"]


def test_dry_run_passes_to_both(env):
    assert run_batch.main(["--dry-run"]) == 0
    assert "--dry-run" in env.calls[0][1]
    assert env.calls[1][2]["dry_run"] and env.calls[2][2]["dry_run"]
    assert env.beats == []


def test_publish_args_forwarded(env):
    run_batch.main(["--lake", "L", "--export-dir", "E"])
    assert env.calls[0][1][:4] == ["--lake", "L", "--export-dir", "E"]


def test_publish_failure_stops_before_gold(env, monkeypatch, capsys):
    monkeypatch.setattr(publish_silver, "run", lambda argv: (1, None, _pub_args(argv)))
    assert run_batch.main([]) == 1
    assert env.calls == []
    err = capsys.readouterr().err
    assert "BATCH FAILED at publish" in err and "phases finished: none" in err


def test_gold_failure_stops_before_checks(env, monkeypatch, capsys):
    def boom(store, steps=None, **kw):
        env.calls.append(("build", steps))
        raise gold_build.GoldBuildError("fact_orders", RuntimeError("bad " + TOKEN), {})
    monkeypatch.setattr(gold_build, "run_build", boom)
    assert run_batch.main([]) == 1
    assert len([c for c in env.calls if c[0] == "build"]) == 1
    err = capsys.readouterr().err
    assert "BATCH FAILED at gold" in err and "fact_orders" in err and "phases finished: publish" in err
    assert TOKEN not in err
    assert "gold_build" not in env.beats


def test_window_forwarded_unchanged(env):
    assert run_batch.main(["--skip-publish", "--from-date", "2026-10-01", "--to-date", "2026-10-05",
                           "--steps", "fact_orders,dim_date"]) == 0
    kw = env.calls[0][2]
    assert (kw["from_date"], kw["to_date"], kw["full"], kw["days"]) == ("2026-10-01", "2026-10-05", False, None)
    assert env.calls[0][1] == ["fact_orders", "dim_date"]
    run_batch.main(["--skip-publish", "--days", "3"])
    assert env.calls[2][2]["days"] == 3


@pytest.mark.parametrize("argv", [["--full", "--days", "3"], ["--from-date", "2026-10-01"],
                                  ["--days", "2", "--from-date", "2026-10-01", "--to-date", "2026-10-02"],
                                  ["--steps", "checks"], ["--steps", "nope"]])
def test_bad_args_exit_2(env, argv, capsys):
    assert run_batch.main(argv) == 2
    assert env.calls == []
    assert "Traceback" not in capsys.readouterr().err


def test_missing_store_settings_exit_2(monkeypatch, capsys):
    for v in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH", "GOLD_BACKEND", "PG_DSN"):
        monkeypatch.delenv(v, raising=False)
    called = []
    monkeypatch.setattr(publish_silver, "run", lambda a: called.append(1))
    assert run_batch.main(["--skip-publish"]) == 2
    err = capsys.readouterr().err
    assert err.startswith("error: ") and "Traceback" not in err and called == []


def test_checks_empty_ok_and_failing_exit_1(env, capsys):
    assert run_batch.main(["--skip-publish"]) == 0
    assert "checks: not built yet (T16)" in capsys.readouterr().out
    env.checks = _step("failed", failed=["orders_total"])
    assert run_batch.main(["--skip-publish"]) == 1
    err = capsys.readouterr().err
    assert "BATCH FAILED at checks" in err and "orders_total" in err
    env.checks = _step("ok", failed=["x"])
    assert run_batch.main(["--skip-publish"]) == 1


def test_checks_raising_exit_1(env, monkeypatch, capsys):
    real = gold_build.run_build

    def build(store, steps=None, **kw):
        if steps == ["checks"]:
            raise gold_build.GoldBuildError("checks", RuntimeError("check x failed"), {})
        return real(store, steps=steps, **kw)
    monkeypatch.setattr(gold_build, "run_build", build)
    assert run_batch.main(["--skip-publish"]) == 1
    assert "BATCH FAILED at checks" in capsys.readouterr().err


def test_no_secret_in_output(env, capsys):
    run_batch.main([])
    run_batch.main(["--dry-run"])
    c = capsys.readouterr()
    assert TOKEN not in c.out and TOKEN not in c.err
