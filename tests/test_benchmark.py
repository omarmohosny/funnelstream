"""Host tests for scripts/benchmark.py: no Docker, no Kafka, no Databricks. A fake runner records every command."""
import json
import subprocess
from types import SimpleNamespace

import pytest

from scripts import benchmark as bm

SIM_OUT = "noise written\n{'app.events': 19316, 'orders': 667, 'delivery.events': 383} total 20366\n"
PUBLISH_OUT = ("silver_events                 rows=18822    files=1 merged=True\n"
               "silver_orders                 rows=153      files=1 merged=True\n")


def _args(*extra):
    return bm.parse_args(["--events", "20000", *extra])


class FakeRunner:
    def __init__(self, fail_on=None, containers=""):
        self.calls, self.fail_on, self.containers = [], fail_on, containers

    def __call__(self, argv, **kw):
        self.calls.append(list(argv))
        line = " ".join(argv)
        out, rc = "", 0
        if argv[:2] == ["docker", "ps"]:
            out = self.containers
        elif "run_simulator.py" in line:
            out = SIM_OUT
        elif "publish_silver.py" in line:
            out = PUBLISH_OUT
        elif argv[:2] == ["git", "rev-parse"]:
            out = "abc1234\n"
        elif argv[:2] == ["docker", "info"]:
            out = "8318783488\n"
        if self.fail_on and self.fail_on in line:
            rc = 1
        return SimpleNamespace(returncode=rc, stdout=out, stderr="")


class Clock:
    def __init__(self, step=10.0):
        self.t, self.step = 0.0, step

    def __call__(self):
        self.t += self.step
        return self.t


def _run(args, runner=None, **kw):
    runner = runner or FakeRunner()
    return bm.run(args, runner=runner, clock=Clock(), rmtree=lambda p: None, sleep=lambda s: None, **kw), runner


def test_summarise():
    assert bm.summarise(2_000_000, 400.0) == {"events": 2000000, "seconds": 400.0, "events_per_second": 5000.0}
    assert bm.summarise(10, 0) == {"events": 10, "seconds": 0, "events_per_second": None}
    assert bm.summarise(1000, 3.14159)["seconds"] == 3.14


def test_parse_simulator_total():
    assert bm.parse_simulator_total(SIM_OUT) == 20366
    assert bm.parse_simulator_total("total 5\nother\n{'a': 1} total 77") == 77
    with pytest.raises(ValueError):
        bm.parse_simulator_total("nothing here")


def test_plan_stage_counts_and_names():
    full = bm.plan(_args())
    assert [s.name for s in full if not s.skip] == ["reset", "simulate", "bronze", "seed", "silver", "ddl", "publish", "gold"]
    short = bm.plan(_args("--skip-gold"))
    assert [s.name for s in short if not s.skip] == ["reset", "simulate", "bronze", "seed", "silver"]
    assert len([s for s in short if not s.skip]) == 5 and len([s for s in full if not s.skip]) == 8


def _all_text(stages):
    return "\n".join(" ".join(c) for s in stages for c in s.commands)


def test_plan_never_touches_the_real_catalog_or_demo_lake():
    text = _all_text(bm.plan(_args()))
    for s in bm.plan(_args()):
        for c in s.commands:
            for part in c:
                assert part.lower() != "funnelstream"
                assert "data/lake" not in part and "data/checkpoints" not in part
    assert "workspace" in text


def test_spark_stages_blank_the_postgres_dsn():
    for s in bm.plan(_args()):
        for c in s.commands:
            if c[:3] == ["docker", "compose", "run"]:
                i = c.index("-e")
                assert ["-e", "PG_DSN="] == c[i:i + 2], s.name


def test_plan_commands_match_the_brief():
    stages = {s.name: s for s in bm.plan(_args())}
    sim = stages["simulate"].argv
    assert sim[1].endswith("run_simulator.py") and "--events" in sim and "20000" in sim
    assert sim[sim.index("--manifest") + 1] == "data/bench/noise_manifest.json"
    assert sim[sim.index("--universe-seed") + 1] == "7"
    assert "--once" in stages["bronze"].argv
    gold = stages["gold"].argv
    assert gold[-4:] == ["--skip-publish", "--full", "--manifest", "data/bench/noise_manifest.json"]
    assert "DATABRICKS_CATALOG=workspace" in stages["ddl"].argv


def test_dry_run_runs_nothing(capsys):
    runner = FakeRunner()
    rc = bm.main(["--events", "20000", "--dry-run"], runner=runner)
    assert rc == 0 and runner.calls == []
    out = capsys.readouterr().out
    assert "1. reset" in out and "8. gold" in out and "run_simulator.py" in out


def test_without_yes_it_prints_the_plan_and_exits_2(capsys):
    runner = FakeRunner()
    rc = bm.main(["--events", "20000"], runner=runner)
    assert rc == 2 and runner.calls == []
    assert "Kafka" in capsys.readouterr().out


def test_refuses_when_demo_containers_run(capsys):
    runner = FakeRunner(containers="fs-pipeline\n")
    rc = bm.main(["--events", "20000", "--yes"], runner=runner)
    assert rc == 3
    assert all(c[:2] == ["docker", "ps"] for c in runner.calls)
    assert "fs-pipeline" in capsys.readouterr().err


@pytest.mark.parametrize("extra", [["--lake", "data/lake"], ["--checkpoints", "data/checkpoints"],
                                   ["--lake", "data/bench/../lake"], ["--lake", "C:/Windows"],
                                   ["--catalog", "funnelstream"], ["--catalog", "FunnelStream"]])
def test_refuses_unsafe_targets(extra, capsys):
    runner = FakeRunner()
    rc = bm.main(["--events", "20000", "--yes", *extra], runner=runner)
    assert rc == 3 and runner.calls == []
    assert capsys.readouterr().err


def test_a_failing_stage_stops_the_run_and_names_it(capsys, tmp_path):
    runner = FakeRunner(fail_on="jobs.silver_clean")
    report = tmp_path / "benchmark.md"
    rc = bm.main(["--events", "20000", "--yes", "--report", str(report)], runner=runner, clock=Clock(),
                 rmtree=lambda p: None, sleep=lambda s: None)
    assert rc == 1
    assert "BENCHMARK FAILED at silver" in capsys.readouterr().err
    assert not any("apply_ddl.py" in " ".join(c) for c in runner.calls)
    assert not report.exists()


def test_run_collects_rows_and_times():
    result, runner = _run(_args())
    assert result["messages"] == 20366
    by = {s["name"]: s for s in result["stages"]}
    assert list(by) == ["reset", "simulate", "bronze", "seed", "silver", "ddl", "publish", "gold"]
    assert by["simulate"]["rows"] == 20366 and by["bronze"]["rows"] == 20366 and by["silver"]["rows"] == 20366
    assert by["publish"]["rows"] == 18822 + 153
    assert by["gold"]["rows"] is None and by["seed"]["rows"] is None
    assert by["silver"]["seconds"] == 10.0  # fake clock: two reads, 10 s apart
    assert by["bronze"]["rows_per_second"] == round(20366 / 10.0, 2)
    assert result["databricks"] is True
    assert result["total_seconds"] == sum(s["seconds"] for s in result["stages"])


def test_run_waits_for_kafka_then_creates_topics():
    calls = []

    class R(FakeRunner):
        def __call__(self, argv, **kw):
            calls.append(list(argv))
            r = super().__call__(argv, **kw)
            if "kafka-topics.sh" in " ".join(argv) and sum("kafka-topics.sh" in " ".join(c) for c in calls) < 3:
                r.returncode = 1
            return r

    _run(_args("--skip-gold"), runner=R())
    lines = [" ".join(c) for c in calls]
    first_list = next(i for i, l in enumerate(lines) if "kafka-topics.sh" in l)
    assert sum("kafka-topics.sh" in l for l in lines) == 3
    assert "create_topics.py" in lines[first_list + 3]
    assert any("rm -sf kafka" in l for l in lines) and any("up -d kafka" in l for l in lines)


def test_kafka_that_never_comes_up_fails_the_reset(capsys):
    class R(FakeRunner):
        def __call__(self, argv, **kw):
            r = super().__call__(argv, **kw)
            if "kafka-topics.sh" in " ".join(argv):
                r.returncode = 1
            return r

    with pytest.raises(bm.BenchmarkFailed) as e:
        _run(_args("--skip-gold"), runner=R())
    assert e.value.stage == "reset"


def test_skip_gold_leaves_out_databricks_stages():
    result, runner = _run(_args("--skip-gold"))
    assert [s["name"] for s in result["stages"]] == ["reset", "simulate", "bronze", "seed", "silver"]
    assert result["databricks"] is False
    assert not any("apply_ddl.py" in " ".join(c) for c in runner.calls)


MACHINE = {"cpus": 8, "docker_memory": "7.7 GB", "commit": "abc1234"}


def test_render_report():
    result, _ = _run(_args())
    result["date"] = "2026-10-11"
    result["command"] = "python scripts/benchmark.py --events 20000 --yes"
    text = bm.render_report(result, MACHINE)
    assert text.startswith("# ")
    assert "2026-10-11" in text and "python scripts/benchmark.py --events 20000 --yes" in text
    assert "abc1234" in text and "8" in text and "7.7 GB" in text
    assert ("Synthetic data on one laptop, Docker limited to about 8 GB. "
            "Numbers are a measurement of this setup, not a capacity claim.") in text
    assert "The Databricks stages ran in the scratch catalog `workspace` on a Databricks Free Edition SQL warehouse." in text
    assert "| stage | seconds | rows | rows per second |" in text
    assert "Total" in text
    for bad in ("TODO", "TBD", "XXX", "None", "placeholder", "<"):
        assert bad not in text, bad


def test_render_report_without_databricks_omits_that_sentence():
    result, _ = _run(_args("--skip-gold"))
    result["date"], result["command"] = "2026-10-11", "x"
    text = bm.render_report(result, MACHINE)
    assert "Databricks" not in text
    assert "Synthetic data on one laptop" in text


def test_successful_main_writes_the_report_once(tmp_path, capsys):
    runner = FakeRunner()
    report = tmp_path / "benchmark.md"
    rc = bm.main(["--events", "20000", "--yes", "--report", str(report)], runner=runner, clock=Clock(),
                 rmtree=lambda p: None, sleep=lambda s: None)
    assert rc == 0
    text = report.read_text(encoding="utf-8")
    assert "abc1234" in text and "--events 20000" in text
