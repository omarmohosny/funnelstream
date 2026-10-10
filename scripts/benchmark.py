"""End to end benchmark: how fast does the whole pipeline move N simulated events, stage by stage.

  python scripts/benchmark.py --events 1000000 --dry-run     (prints the numbered plan, runs nothing)
  python scripts/benchmark.py --events 1000000 --yes         (the real run; writes docs/benchmark.md when it succeeds)

Stages: reset (Kafka container recreated, data/bench deleted), simulate, bronze, seed, silver and, unless --skip-gold,
ddl, publish and gold (Databricks, scratch catalog). Each stage is one timed command. The real run recreates the Kafka
container, so every message in Kafka is lost; it writes only below data/bench and into the scratch catalog.
Exit codes: 0 success, 1 a stage failed, 2 plan printed but --yes missing, 3 refused (a demo container is running,
or --lake / --checkpoints are outside data/bench, or --catalog is the real catalog).
"""
import argparse
import datetime
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BENCH_REL = "data/bench"
BENCH_DIR = os.path.join(ROOT, "data", "bench")
REAL_CATALOG = "funnelstream"
DEMO_CONTAINERS = ("fs-pipeline", "fs-sync", "fs-bronze", "fs-silver")
KAFKA_WAIT_SECONDS = 90
KAFKA_LIST = ["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
              "--bootstrap-server", "kafka:19092", "--list"]

SENTENCE_SETUP = ("Synthetic data on one laptop, Docker limited to about 8 GB. "
                  "Numbers are a measurement of this setup, not a capacity claim.")
SENTENCE_DBX = "The Databricks stages ran in the scratch catalog `workspace` on a Databricks Free Edition SQL warehouse."


class BenchmarkFailed(Exception):
    def __init__(self, stage, text):
        super().__init__(text)
        self.stage, self.text = stage, text


@dataclass
class Stage:
    name: str
    commands: list            # list of argv lists, run in order inside one timed stage
    rows: str = None          # None (seconds only), "messages" (the simulator total) or "publish" (parsed from output)
    skip: bool = False
    note: str = ""

    @property
    def argv(self):
        return self.commands[-1] if self.name != "reset" else self.commands[0]


def summarise(events, seconds):
    return {"events": events, "seconds": round(seconds, 2),
            "events_per_second": round(events / seconds, 2) if seconds else None}


def parse_simulator_total(text):
    found = re.findall(r"\btotal\s+(\d+)", text or "")
    if not found:
        raise ValueError("no 'total <n>' in the simulator output")
    return int(found[-1])


def parse_publish_rows(text):
    found = re.findall(r"\brows=(\d+)", text or "")
    return sum(int(n) for n in found) if found else None


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="End to end pipeline benchmark (recreates Kafka; see the module doc).")
    ap.add_argument("--events", type=int, required=True, help="approximate number of simulated events")
    ap.add_argument("--catalog", default="workspace", help="scratch Databricks catalog (never funnelstream)")
    ap.add_argument("--lake", default=BENCH_REL + "/lake")
    ap.add_argument("--checkpoints", default=BENCH_REL + "/checkpoints")
    ap.add_argument("--seed", type=int, default=4242)
    ap.add_argument("--skip-gold", action="store_true", help="leave out ddl, publish and gold (no Databricks)")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and exit 0, run nothing")
    ap.add_argument("--yes", action="store_true", help="really run (recreates Kafka)")
    ap.add_argument("--report", default=os.path.join("docs", "benchmark.md"), help="where the report is written")
    args = ap.parse_args(argv)
    if args.events <= 0:
        ap.error("--events must be positive")
    return args


def _rel(path):
    """Path below data/bench as a repo-relative posix path (the repo is /app in the container); else None."""
    full = os.path.normcase(os.path.realpath(os.path.join(ROOT, path)))
    bench = os.path.normcase(os.path.realpath(BENCH_DIR))
    if full != bench and not full.startswith(bench + os.sep):
        return None
    return os.path.relpath(os.path.realpath(os.path.join(ROOT, path)), ROOT).replace(os.sep, "/")


def guard(args):
    """Return a refusal text, or None when the targets are safe."""
    if args.catalog.strip().lower() == REAL_CATALOG:
        return "refusing --catalog %s: it is the real catalog" % REAL_CATALOG
    for flag, value in (("--lake", args.lake), ("--checkpoints", args.checkpoints)):
        rel = _rel(value)
        if rel is None or rel == BENCH_REL:
            return "refusing %s %s: it must be a folder inside %s" % (flag, value, BENCH_REL)
    return None


def _spark(args, *cmd, databricks=False):
    argv = ["docker", "compose", "run", "--rm", "-e", "PG_DSN="]
    if databricks:
        argv += ["-e", "DATABRICKS_CATALOG=" + args.catalog]
    return argv + ["spark", "python", *cmd]


def plan(args):
    lake, ck = _rel(args.lake) or args.lake, _rel(args.checkpoints) or args.checkpoints
    manifest = BENCH_REL + "/noise_manifest.json"
    py = sys.executable
    gold = args.skip_gold
    return [
        Stage("reset", [["docker", "compose", "rm", "-sf", "kafka"], ["docker", "compose", "up", "-d", "kafka"],
                        KAFKA_LIST, [py, "kafka/create_topics.py"]],
              note="deletes %s, recreates Kafka (all messages lost), waits up to %d s, creates topics"
                   % (BENCH_REL, KAFKA_WAIT_SECONDS)),
        Stage("simulate", [[py, "scripts/run_simulator.py", "--events", str(args.events), "--seed", str(args.seed),
                            "--universe-seed", "7", "--bootstrap", "localhost:9092", "--manifest", manifest]],
              rows="messages"),
        Stage("bronze", [_spark(args, "-m", "streaming.jobs.bronze_ingest", "--once", "--lake", lake,
                                "--checkpoints", ck)], rows="messages"),
        Stage("seed", [_spark(args, "scripts/seed_silver_reference.py", "--lake", lake)],
              note="reference data, listed but its rows are not counted"),
        Stage("silver", [_spark(args, "-m", "streaming.jobs.silver_clean", "--once", "--lake", lake,
                                "--checkpoints", ck)], rows="messages"),
        Stage("ddl", [_spark(args, "scripts/apply_ddl.py", "--backend", "databricks", "--catalog", args.catalog,
                             databricks=True)], skip=gold),
        Stage("publish", [_spark(args, "scripts/publish_silver.py", "--lake", lake, "--export-dir",
                                 BENCH_REL + "/export", databricks=True)], rows="publish", skip=gold),
        Stage("gold", [_spark(args, "scripts/run_batch.py", "--skip-publish", "--full", "--manifest", manifest,
                              databricks=True)], skip=gold),
    ]


def format_plan(stages, args):
    lines = ["Benchmark plan: %d events, seed %d, catalog %s, lake %s" % (args.events, args.seed, args.catalog, args.lake)]
    n = 0
    for s in stages:
        if s.skip:
            lines.append("   -  %s: skipped (--skip-gold)" % s.name)
            continue
        n += 1
        lines.append("%2d. %s%s" % (n, s.name, "  (%s)" % s.note if s.note else ""))
        for c in s.commands:
            lines.append("      " + subprocess.list2cmdline(c))
    return "\n".join(lines)


def _echo(res):
    for text, stream in ((getattr(res, "stdout", ""), sys.stdout), (getattr(res, "stderr", ""), sys.stderr)):
        if text:
            stream.write(text if text.endswith("\n") else text + "\n")
            stream.flush()


def _exec(runner, argv):
    return runner(argv, check=False, capture_output=True, text=True, encoding="utf-8", errors="replace", cwd=ROOT)


def run(args, runner=subprocess.run, clock=time.monotonic, rmtree=shutil.rmtree, sleep=time.sleep, today=None):
    """Run the stages; returns the result dict. Raises BenchmarkFailed (stage, text) on the first non-zero exit."""
    stages, done, messages = plan(args), [], None
    for stage in stages:
        if stage.skip:
            continue
        print("--- stage %s ---" % stage.name, flush=True)
        t0 = clock()
        out = ""
        if stage.name == "reset":
            if os.path.isdir(BENCH_DIR):
                rmtree(BENCH_DIR)
            rm, up, wait, create = stage.commands
            for argv in (rm, up):
                res = _exec(runner, argv)
                _echo(res)
                if res.returncode != 0:
                    raise BenchmarkFailed(stage.name, "exit code %d from: %s" % (res.returncode, " ".join(argv[:5])))
            start = clock()
            while _exec(runner, wait).returncode != 0:
                if clock() - start > KAFKA_WAIT_SECONDS:
                    raise BenchmarkFailed(stage.name, "Kafka did not answer within %d s" % KAFKA_WAIT_SECONDS)
                sleep(2)
            res = _exec(runner, create)
            _echo(res)
            if res.returncode != 0:
                raise BenchmarkFailed(stage.name, "create_topics.py exited with code %d" % res.returncode)
        else:
            for argv in stage.commands:
                res = _exec(runner, argv)
                _echo(res)
                out += res.stdout or ""
                if res.returncode != 0:
                    raise BenchmarkFailed(stage.name, "exit code %d" % res.returncode)
        seconds = clock() - t0
        rows = None
        if stage.rows == "messages":
            if messages is None:
                try:
                    messages = parse_simulator_total(out)
                except ValueError as e:
                    raise BenchmarkFailed(stage.name, str(e)) from e
            rows = messages
        elif stage.rows == "publish":
            rows = parse_publish_rows(out)
        s = summarise(rows, seconds) if rows is not None else {"seconds": round(seconds, 2), "events_per_second": None}
        done.append({"name": stage.name, "seconds": s["seconds"], "rows": rows, "rows_per_second": s["events_per_second"]})
    return {"events": args.events, "messages": messages, "stages": done,
            "total_seconds": round(sum(d["seconds"] for d in done), 2),
            "databricks": not args.skip_gold, "catalog": args.catalog,
            "date": (today or datetime.date.today()).isoformat()}


def collect_machine(runner=subprocess.run):
    def one(argv):
        try:
            res = _exec(runner, argv)
            return res.stdout.strip() if res.returncode == 0 and res.stdout.strip() else None
        except Exception:
            return None
    mem = one(["docker", "info", "--format", "{{.MemTotal}}"])
    try:
        mem = "%.1f GB" % (int(mem) / 1024 ** 3)
    except (TypeError, ValueError):
        mem = "unknown"
    return {"cpus": os.cpu_count() or "unknown", "docker_memory": mem,
            "commit": one(["git", "rev-parse", "--short", "HEAD"]) or "unknown"}


def _fmt(n):
    return "-" if n is None else ("{:,}".format(n) if isinstance(n, int) else "{:,.2f}".format(n))


def render_report(result, machine):
    lines = ["# Benchmark", "",
             "Date: %s" % result["date"], "",
             "Command: `%s`" % result["command"], "",
             "Git commit: %s" % machine["commit"], "",
             "Machine: %s CPUs, Docker memory %s" % (machine["cpus"], machine["docker_memory"]), "",
             "| stage | seconds | rows | rows per second |", "|---|---|---|---|"]
    for s in result["stages"]:
        lines.append("| %s | %s | %s | %s |" % (s["name"], _fmt(s["seconds"]), _fmt(s["rows"]), _fmt(s["rows_per_second"])))
    msgs = result.get("messages")
    total = "Total: %s seconds for %s simulated events (%s Kafka messages), including the Kafka reset." % (
        _fmt(result["total_seconds"]), _fmt(result["events"]), _fmt(msgs))
    lines += ["", total, "", SENTENCE_SETUP]
    if result["databricks"]:
        lines += ["", SENTENCE_DBX]
    return "\n".join(lines) + "\n"


def _running_demo_containers(runner):
    """Names of demo containers that are running; raises RuntimeError when docker cannot be asked."""
    found = []
    for name in DEMO_CONTAINERS:
        res = _exec(runner, ["docker", "ps", "--filter", "name=" + name, "--format", "{{.Names}}"])
        if res.returncode != 0:
            raise RuntimeError("docker ps failed; cannot check for running demo containers")
        found += [n for n in (res.stdout or "").split() if n]
    return sorted(set(found))


def main(argv=None, runner=subprocess.run, clock=time.monotonic, rmtree=shutil.rmtree, sleep=time.sleep):
    raw = list(sys.argv[1:] if argv is None else argv)
    args = parse_args(raw)
    refusal = guard(args)
    if refusal:
        print("error: " + refusal, file=sys.stderr)
        return 3
    stages = plan(args)
    print(format_plan(stages, args))
    if args.dry_run:
        print("DRY RUN: nothing was executed.")
        return 0
    if not args.yes:
        print("\nThis run recreates the Kafka container (all messages in Kafka are lost), deletes %s and writes to the "
              "catalog %s. Repeat with --yes to run it." % (BENCH_REL, args.catalog if not args.skip_gold else "(none)"))
        return 2
    try:
        running = _running_demo_containers(runner)
    except RuntimeError as e:
        print("error: %s" % e, file=sys.stderr)
        return 3
    if running:
        print("error: stop the demo first (.\\scripts\\stop_demo.ps1); it would consume the benchmark messages. "
              "Running: %s" % ", ".join(running), file=sys.stderr)
        return 3
    try:
        result = run(args, runner=runner, clock=clock, rmtree=rmtree, sleep=sleep)
    except BenchmarkFailed as e:
        print("BENCHMARK FAILED at %s: %s" % (e.stage, e.text), file=sys.stderr)
        return 1
    result["command"] = "python scripts/benchmark.py " + subprocess.list2cmdline(raw)
    text = render_report(result, collect_machine(runner))
    os.makedirs(os.path.dirname(os.path.abspath(args.report)), exist_ok=True)
    with open(args.report, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
    print("BENCHMARK OK in %.1fs; report written to %s" % (result["total_seconds"], args.report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
