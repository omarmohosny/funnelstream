"""End to end smoke test of the whole project: simulator, Kafka, Bronze, Silver, live tables, daily marts, Grafana
dashboards and alerts, Databricks Gold and the Power BI source tables, in one report.

  python scripts/smoke_test.py --dry-run             (prints the numbered plan, runs nothing)
  python scripts/smoke_test.py --yes                 (the real run; writes docs/smoke-test.md)
  python scripts/smoke_test.py --yes --skip-gold --skip-spark-tests

A smoke test finds out what is broken. It never retries a stage until it passes, never hides a failure and never edits
data to make a check green. A failing stage does not stop the stages that do not depend on it; the stages that do are
reported as "skipped: <prerequisite> failed".

Safety (the same model as scripts/benchmark.py): the real run recreates the Kafka container, so every message in Kafka
is lost. It writes only below data/smoke, into the separate Postgres database funnelstream_smoke (dropped and created
again at the start, so no earlier run leaks into the counts) and into the scratch Databricks catalog. The demo lake, the
demo Postgres tables and the real catalog are never touched.
Exit codes: 0 nothing failed, 1 a stage failed, 2 plan printed but --yes missing, 3 refused (a demo container is
running, --lake or --checkpoints outside data/smoke, or --catalog is the real catalog).
"""
import argparse
import ast
import datetime
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from urllib.parse import quote, unquote, urlsplit, urlunsplit

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

SMOKE_REL = "data/smoke"
SMOKE_DIR = os.path.join(ROOT, "data", "smoke")
SMOKE_DB = "funnelstream_smoke"
RO_ROLE = "grafana_ro"
REAL_CATALOG = "funnelstream"
DEMO_PREFIX = "fs-"
KAFKA_WAIT_SECONDS = 90
CLOCK_TOLERANCE_SECONDS = 2.0
TINY_EVENTS = 4000
TINY_DAYS_AGO = 6
TOPICS = ("app.events", "orders", "delivery.events", "inventory.events")
KAFKA_LIST = ["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
              "--bootstrap-server", "kafka:19092", "--list"]
GRAFANA_DASHBOARDS = os.path.join(ROOT, "monitoring", "grafana", "dashboards")
ALERTING = os.path.join(ROOT, "monitoring", "grafana", "provisioning", "alerting", "alerting.yaml")
MODEL_MD = os.path.join(ROOT, "dashboards", "powerbi", "MODEL.md")
MEASURES_DAX = os.path.join(ROOT, "dashboards", "powerbi", "measures.dax")
SILVER_NON_EMPTY = ("silver_events", "silver_orders", "silver_order_status", "silver_order_items", "silver_quarantine",
                    "silver_products", "silver_customers", "silver_stock_movements", "silver_stock_snapshots",
                    "silver_express_orders")
# live_metrics_minute is left out on purpose: the run replays days of history, every event arrives late, and that table
# counts only events that are not late. The live rehearsals prove it with events produced in real time.
LIVE_POPULATED = ("pipeline_health_minute", "live_stock_now", "live_express_orders")
DASHBOARD_DAYS = 30  # the smoke data is days old: the panels must look back far enough to find it
TINY_PROMISE_MINUTES = 2
CONTROL_STORE, CONTROL_PRODUCT = "smoke_control_store", "smoke_control_product"
PASS, FAIL, SKIPPED = "pass", "fail", "skipped"


class StageFail(Exception):
    """A stage found something broken (the text is the evidence)."""


@dataclass
class Result:
    name: str
    status: str
    evidence: str = ""
    seconds: float = 0.0
    detail: str = ""


@dataclass
class Stage:
    key: str
    title: str
    func: object = None
    depends: tuple = ()
    commands: list = field(default_factory=list)
    note: str = ""
    skip_reason: str = None


# ---------------------------------------------------------------------------------------------- arguments and safety
def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="End to end smoke test (recreates Kafka; see the module doc).")
    ap.add_argument("--events", type=int, default=20000, help="approximate number of simulated events (default 20000)")
    ap.add_argument("--catalog", default="workspace", help="scratch Databricks catalog (never funnelstream)")
    ap.add_argument("--lake", default=SMOKE_REL + "/lake")
    ap.add_argument("--checkpoints", default=SMOKE_REL + "/checkpoints")
    ap.add_argument("--seed", type=int, default=None, help="simulator seed (default: unique, taken from the clock)")
    ap.add_argument("--skip-spark-tests", action="store_true", help="leave out the Spark test files (stage 3)")
    ap.add_argument("--skip-gold", action="store_true", help="leave out Databricks (the batch and the D-1 marketing build after it)")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and exit 0, run nothing")
    ap.add_argument("--yes", action="store_true", help="really run (recreates Kafka)")
    ap.add_argument("--report", default=os.path.join("docs", "smoke-test.md"), help="where the report is written")
    args = ap.parse_args(argv)
    if args.events <= 0:
        ap.error("--events must be positive")
    return args


def _rel(path):
    """Path below data/smoke as a repo-relative posix path (the repo is /app in the container); else None."""
    full = os.path.normcase(os.path.realpath(os.path.join(ROOT, path)))
    base = os.path.normcase(os.path.realpath(SMOKE_DIR))
    if full != base and not full.startswith(base + os.sep):
        return None
    return os.path.relpath(os.path.realpath(os.path.join(ROOT, path)), ROOT).replace(os.sep, "/")


def guard(args):
    """Return a refusal text, or None when the targets are safe."""
    if args.catalog.strip().lower() == REAL_CATALOG:
        return "refusing --catalog %s: it is the real catalog" % REAL_CATALOG
    for flag, value in (("--lake", args.lake), ("--checkpoints", args.checkpoints)):
        rel = _rel(value)
        if rel is None or rel == SMOKE_REL:
            return "refusing %s %s: it must be a folder inside %s" % (flag, value, SMOKE_REL)
    return None


# ---------------------------------------------------------------------------------------------- pure helpers
def parse_pytest_summary(text):
    """{'passed': n, 'failed': n, 'skipped': n, 'errors': n} from the last pytest summary line, or None."""
    for line in reversed((text or "").splitlines()):
        found = re.findall(r"(\d+) (passed|failed|skipped|errors?|xfailed|xpassed|deselected)", line)
        if found and re.search(r"\d+ (passed|failed|errors?)", line):
            counts = {"passed": 0, "failed": 0, "skipped": 0, "errors": 0}
            for n, word in found:
                key = "errors" if word.startswith("error") else word
                if key in counts:
                    counts[key] += int(n)
            return counts
    return None


def parse_simulator_output(text):
    """(per topic counts, total) from the last '{...} total N' line of run_simulator.py."""
    found = re.findall(r"(\{[^{}]*\})\s+total\s+(\d+)", text or "")
    if not found:
        raise ValueError("no '{...} total <n>' line in the simulator output")
    counts = ast.literal_eval(found[-1][0])
    return {str(k): int(v) for k, v in counts.items()}, int(found[-1][1])


def parse_json_line(text, marker="SMOKE_JSON "):
    for line in reversed((text or "").splitlines()):
        if line.startswith(marker):
            return json.loads(line[len(marker):])
    raise ValueError("no %s line in the output" % marker.strip())


def tail(text, lines=12, limit=1500):
    out = "\n".join((text or "").strip().splitlines()[-lines:])
    return out[-limit:]


def _balanced_args(sql, start):
    """Index after the ')' that closes the '(' at sql[start], and the text between them."""
    depth, i = 0, start
    while i < len(sql):
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                return i + 1, sql[start + 1:i]
        i += 1
    raise ValueError("unbalanced parenthesis in: %s" % sql[start:start + 60])


def _split_args(text):
    out, depth, cur = [], 0, ""
    for ch in text:
        if ch == "," and depth == 0:
            out.append(cur.strip())
            cur = ""
            continue
        depth += (ch == "(") - (ch == ")")
        cur += ch
    out.append(cur.strip())
    return out


def _bucket(col, spec):
    """SQL for a time bucket of `col`: date_trunc for one minute, hour or day, an epoch floor for anything else."""
    m = re.fullmatch(r"'?(\d+)([smhd])'?", spec.strip())
    if not m:
        raise ValueError("unsupported Grafana interval: %s" % spec)
    n, unit = int(m.group(1)), m.group(2)
    names = {"m": "minute", "h": "hour", "d": "day", "s": "second"}
    if n == 1:
        return "date_trunc('%s', %s)" % (names[unit], col)
    seconds = n * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    return "to_timestamp(floor(extract(epoch from %s) / %d) * %d)" % (col, seconds, seconds)


def expand_macros(sql, days=1):
    """Replace the Grafana macros the dashboards use by plain Postgres (a dashboard window of `days` days).

    $__timeFilter(col)            col >= now() - interval '1 day'
    $__timeGroupAlias(col, '1m')  date_trunc('minute', col) AS "time"      ($__timeGroup: the same without the alias)
    $__timeFrom() / $__timeTo()   now() - interval '1 day' / now()"""
    window = "now() - interval '%d day%s'" % (days, "" if days == 1 else "s")
    out = sql
    for name in ("timeGroupAlias", "timeGroup", "timeFilter", "timeFrom", "timeTo"):
        token = "$__" + name + "("
        while token in out:
            at = out.index(token)
            end, inner = _balanced_args(out, at + len(token) - 1)
            args = _split_args(inner)
            if name == "timeFilter":
                new = "%s >= %s" % (args[0], window)
            elif name == "timeFrom":
                new = "(%s)" % window if out[end:].startswith("::") else window  # a cast must apply to the window, not to the interval
            elif name == "timeTo":
                new = "now()"
            else:
                new = _bucket(args[0], args[1]) + (' AS "time"' if name == "timeGroupAlias" else "")
            out = out[:at] + new + out[end:]
    out = out.replace("$store", "SELECT store_id FROM dim_store")  # the dashboards' store variable: all stores
    left = re.findall(r"\$__\w+", out)
    if left:
        raise ValueError("unexpanded Grafana macro: %s" % ", ".join(sorted(set(left))))
    return out


def dashboard_queries(doc):
    """[(panel title, rawSql)] of every panel of a dashboard (rows with nested panels included)."""
    out = []

    def walk(panels):
        for p in panels:
            for t in p.get("targets", []) or []:
                if t.get("rawSql"):
                    out.append((p.get("title", "(no title)"), t["rawSql"]))
            walk(p.get("panels", []) or [])
    walk(doc.get("panels", []))
    return out


def panel_ids(doc):
    ids = []

    def walk(panels):
        for p in panels:
            ids.append(p.get("id"))
            walk(p.get("panels", []) or [])
    walk(doc.get("panels", []))
    return ids


def parse_model_sources(md):
    """{'gold': [(table, power bi name)], 'postgres': [(table, power bi name)]} from the two source tables of MODEL.md."""
    out, section = {"gold": [], "postgres": []}, None
    for line in md.splitlines():
        low = line.lower()
        if line.startswith("|") and "power bi name" in low:
            section = "gold" if "gold table" in low else "postgres" if "postgres table" in low else None
            continue
        if not line.startswith("|"):
            section = None
            continue
        m = re.match(r"^\|\s*`([a-z][a-z0-9_]*)`\s*\|\s*`([^`]+)`\s*\|", line)
        if m and section:
            out[section].append((m.group(1), m.group(2)))
    return out


def parse_dax_refs(dax):
    """[(measure, table, column)] for every 'Table'[column] reference of the measures file (comments removed)."""
    refs, name = [], None
    for raw in dax.splitlines():
        line = re.sub(r"//.*$", "", raw)
        m = re.match(r"^(?!VAR\b|RETURN\b)([A-Za-z][A-Za-z0-9 ()%._-]*?)\s+=(?:\s|$)", line)
        if m:
            name = m.group(1).strip()
        for table, col in re.findall(r"'([^']+)'\[([^\]]+)\]", line):
            refs.append((name or "(file header)", table, col))
        for table, col in re.findall(r"(?<!['\w])([A-Za-z_]\w*)\[([^\]]+)\]", line):
            if table.upper() not in ("VAR", "RETURN"):
                refs.append((name or "(file header)", table, col))
    return refs


def check_power_bi_sources(sources, refs, gold_columns, pg_columns):
    """Failure texts (empty when all is well). gold_columns / pg_columns: {table: set(columns)}."""
    problems, where = [], {}
    for table, friendly in sources["gold"]:
        if table not in gold_columns:
            problems.append("Gold table %s (Power BI name %s) does not exist" % (table, friendly))
        where[friendly] = gold_columns.get(table)
    for table, friendly in sources["postgres"]:
        if table not in pg_columns:
            problems.append("Postgres table %s (Power BI name %s) does not exist" % (table, friendly))
        where[friendly] = pg_columns.get(table)
    seen = set()
    for measure, table, col in refs:
        if table == "_Measures" or (measure, table, col) in seen:
            continue
        seen.add((measure, table, col))
        if table not in where:
            problems.append("measure %s names the table %s, which MODEL.md does not list" % (measure, table))
        elif where[table] is not None and col.lower() not in {c.lower() for c in where[table]}:  # DAX names ignore case
            problems.append("measure %s names the missing column %s[%s]" % (measure, table, col))
    return problems


def rule_threshold(rule):
    """(operator, number) of the rule's threshold expression (refId C)."""
    for item in rule["data"]:
        if item.get("refId") == "C":
            ev = item["model"]["conditions"][0]["evaluator"]
            return ev["type"], ev["params"][0]
    raise ValueError("rule %s has no refId C" % rule.get("uid"))


def apply_threshold(kind, limit, value):
    if value is None:
        return None
    if kind == "gt":
        return value > limit
    if kind == "lt":
        return value < limit
    raise ValueError("unsupported threshold type %s" % kind)


def pin_now(sql, instant):
    """The rule query evaluated as of `instant` (an ISO string): every now() becomes that timestamp."""
    return re.sub(r"\bnow\(\)", "(TIMESTAMPTZ '%s')" % instant, sql)


def _number(value):
    return None if value is None else float(value)


# ---------------------------------------------------------------------------------------------- real adapters
def load_env(path=os.path.join(ROOT, ".env"), base=None):
    env = {}
    if os.path.isfile(path):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip().strip('"').strip("'")
    env.update(os.environ if base is None else base)
    return env


def dsn_for(dsn, database=None, user=None, password=None):
    """The same server with another database and, optionally, another login (URL style DSNs only)."""
    parts = urlsplit(dsn)
    if parts.scheme not in ("postgresql", "postgres") or not parts.hostname:
        raise ValueError("PG_DSN must be a postgresql:// URL")
    host = parts.hostname + (":%d" % parts.port if parts.port else "")
    login = quote(unquote(parts.username or ""), safe="") + (":" + quote(unquote(parts.password), safe="") if parts.password else "")
    if user is not None:
        login = quote(user, safe="") + (":" + quote(password, safe="") if password else "")
    path = "/" + database if database else parts.path
    return urlunsplit((parts.scheme, (login + "@" if login else "") + host, path, parts.query, ""))


class PgAccess:
    """The three ways the smoke test talks to Postgres: admin (maintenance database), smoke (owner) and ro (grafana_ro)."""

    def __init__(self, env):
        self.env = env

    def _dsn(self, who):
        base = self.env.get("PG_DSN")
        if not base:
            raise StageFail("PG_DSN is not set (.env)")
        if who == "admin":
            return dsn_for(base, "postgres")
        if who == "smoke":
            return dsn_for(base, SMOKE_DB)
        password = self.env.get("GRAFANA_DB_PASSWORD")
        if not password:
            raise StageFail("GRAFANA_DB_PASSWORD is not set (.env): the read-only role cannot be used")
        return dsn_for(base, SMOKE_DB, user=RO_ROLE, password=password)

    def query(self, who, sql):
        import psycopg
        with psycopg.connect(self._dsn(who), connect_timeout=5, autocommit=True) as conn:
            if who == "ro":
                conn.read_only = True
            with conn.cursor() as cur:
                cur.execute(sql)
                return [tuple(r) for r in cur.fetchall()] if cur.description else []

    def recreate_database(self):
        import psycopg
        with psycopg.connect(self._dsn("admin"), connect_timeout=5, autocommit=True) as conn:
            conn.execute("DROP DATABASE IF EXISTS %s WITH (FORCE)" % SMOKE_DB)
            conn.execute("CREATE DATABASE %s" % SMOKE_DB)


def http_get(url, auth=None, timeout=5):
    import base64
    import urllib.error
    import urllib.request
    req = urllib.request.Request(url)
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(("%s:%s" % auth).encode()).decode())
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


def kafka_handshake(bootstrap):
    from confluent_kafka.admin import AdminClient
    md = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=10)
    return "%d broker(s), %d topic(s)" % (len(md.brokers), len(md.topics))


def host_free_memory():
    """Free physical memory of the host as text, or 'unknown'."""
    try:
        if sys.platform == "win32":
            import ctypes

            class MemoryStatus(ctypes.Structure):
                _fields_ = [("length", ctypes.c_ulong), ("load", ctypes.c_ulong), ("total", ctypes.c_ulonglong),
                            ("avail", ctypes.c_ulonglong), ("tpage", ctypes.c_ulonglong), ("apage", ctypes.c_ulonglong),
                            ("tvirt", ctypes.c_ulonglong), ("avirt", ctypes.c_ulonglong), ("ext", ctypes.c_ulonglong)]
            st = MemoryStatus()
            st.length = ctypes.sizeof(MemoryStatus)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(st))
            return "%.1f GB free of %.1f GB" % (st.avail / 1024 ** 3, st.total / 1024 ** 3)
        pages, size, total = (os.sysconf("SC_AVPHYS_PAGES"), os.sysconf("SC_PAGE_SIZE"), os.sysconf("SC_PHYS_PAGES"))
        return "%.1f GB free of %.1f GB" % (pages * size / 1024 ** 3, total * size / 1024 ** 3)
    except Exception:
        return "unknown"


class Ctx:
    """Everything a stage touches, so the tests can replace it: the command runner, the clock, Postgres, HTTP, Kafka."""

    def __init__(self, args, seed, start2, runner=subprocess.run, clock=time.monotonic, sleep=time.sleep,
                 rmtree=shutil.rmtree, utcnow=None, env=None, pg=None, http=http_get, kafka=kafka_handshake,
                 memory=host_free_memory):
        self.args, self.seed, self.start2 = args, seed, start2
        self.runner, self.clock, self.sleep, self.rmtree = runner, clock, sleep, rmtree
        self.utcnow = utcnow or (lambda: datetime.datetime.now(datetime.timezone.utc))
        self.env = load_env() if env is None else env
        self.pg = pg or PgAccess(self.env)
        self.http, self.kafka, self.memory = http, kafka, memory
        self.lake = _rel(args.lake) or args.lake
        self.checkpoints = _rel(args.checkpoints) or args.checkpoints
        self.state = {"env": {}}
        self.root = ROOT

    # -- commands
    def run(self, argv, timeout=None):
        env = dict(os.environ)
        if argv and argv[0] == "docker":
            env["POSTGRES_DB"] = SMOKE_DB  # compose builds the container's PG_DSN from it: the Spark jobs write to the smoke database
        res = self.runner(argv, check=False, capture_output=True, text=True, encoding="utf-8", errors="replace",
                          cwd=ROOT, env=env, timeout=timeout)
        for text, stream in ((getattr(res, "stdout", ""), sys.stdout), (getattr(res, "stderr", ""), sys.stderr)):
            if text:
                stream.write(text if text.endswith("\n") else text + "\n")
                stream.flush()
        return res

    def run_ok(self, argv, what, timeout=None):
        res = self.run(argv, timeout=timeout)
        if res.returncode != 0:
            raise StageFail("%s exited with code %d\n%s" % (what, res.returncode, tail((res.stdout or "") + "\n" + (res.stderr or ""))))
        return res.stdout or ""

    def spark(self, *cmd, dbx=False, pg=True):
        return spark_cmd(self.args, *cmd, dbx=dbx, pg=pg)

    def smoke_sql(self, sql, who="smoke"):
        return self.pg.query(who, sql)

    def scalar(self, sql, who="smoke"):
        rows = self.pg.query(who, sql)
        return rows[0][0] if rows else None


def spark_cmd(args, *cmd, dbx=False, pg=True):
    argv = ["docker", "compose", "run", "--rm"]
    if not pg:
        argv += ["-e", "PG_DSN="]
    if dbx:
        argv += ["-e", "DATABRICKS_CATALOG=" + args.catalog]
    return argv + ["spark", "python", *cmd]


# ---------------------------------------------------------------------------------------------- stage 1
def stage_preflight(ctx):
    notes, problems = [], []

    def check(label, fn):
        try:
            notes.append("%s: %s" % (label, fn()))
        except Exception as e:  # every check runs; a failure is a line of the evidence
            problems.append("%s: %s" % (label, " ".join(str(e).split())[:300]))

    def docker():
        res = ctx.run(["docker", "info", "--format", "{{.SystemTime}}|{{.MemTotal}}"], timeout=60)
        if res.returncode != 0:
            raise StageFail("docker info failed: " + tail(res.stderr or res.stdout, 3))
        stamp, mem = (res.stdout or "").strip().split("|")
        mem_gb = "%.1f GB" % (int(mem) / 1024 ** 3)
        when = stamp.strip()
        when = re.sub(r"(\.\d{6})\d*", r"\1", when).replace("Z", "+00:00")
        theirs = datetime.datetime.fromisoformat(when)
        if theirs.tzinfo is None:
            theirs = theirs.replace(tzinfo=datetime.timezone.utc)
        offset = abs((ctx.utcnow() - theirs).total_seconds())
        ctx.state["env"].update(docker_memory=mem_gb, clock_offset="%.2f s" % offset)
        if offset > CLOCK_TOLERANCE_SECONDS:
            raise StageFail("host and Docker clocks differ by %.2f s (limit %.0f s); the time windows would be wrong"
                            % (offset, CLOCK_TOLERANCE_SECONDS))
        return "engine answers, Docker memory %s, clock offset %.2f s" % (mem_gb, offset)

    def ntp():
        if sys.platform != "win32":
            ctx.state["env"]["ntp"] = "not queried"
            return "not queried (informational)"
        try:
            res = ctx.run(["w32tm", "/query", "/status"], timeout=15)
            lines = [l.strip() for l in (res.stdout or "").splitlines() if l.strip().lower().startswith(("source", "last successful"))]
            text = "; ".join(lines) if res.returncode == 0 and lines else "no status (informational)"
        except OSError:
            text = "w32tm not available (informational)"
        ctx.state["env"]["ntp"] = text
        return text

    def memory():
        text = ctx.memory()
        ctx.state["env"]["host_memory"] = text
        return text

    def demo():
        res = ctx.run(["docker", "ps", "--filter", "name=" + DEMO_PREFIX, "--format", "{{.Names}}"], timeout=60)
        if res.returncode != 0:
            raise StageFail("docker ps failed; cannot check for running demo containers")
        running = sorted(n for n in (res.stdout or "").split() if n.startswith(DEMO_PREFIX))
        if running:
            raise StageFail("demo container(s) running: %s (run .\\scripts\\stop_demo.ps1)" % ", ".join(running))
        return "no fs- container running"

    def grafana():
        base = ctx.env.get("GRAFANA_URL", "http://localhost:3000")
        code, _ = ctx.http(base + "/api/health")
        if code != 200:
            raise StageFail("GET /api/health answered %s" % code)
        password = ctx.env.get("GRAFANA_ADMIN_PASSWORD")
        if not password:
            raise StageFail("health 200, but GRAFANA_ADMIN_PASSWORD is not set: the data source cannot be checked")
        code, body = ctx.http(base + "/api/datasources/uid/fs-postgres/health", auth=("admin", password))
        status = json.loads(body).get("status") if code == 200 else None
        if status != "OK":
            raise StageFail("data source fs-postgres health: HTTP %s %s" % (code, tail(body, 2, 200)))
        return "health 200, data source fs-postgres OK"

    check("Docker", docker)
    check("NTP", ntp)
    check("Host memory", memory)
    check("Demo containers", demo)
    check("Kafka handshake", lambda: ctx.kafka("localhost:9092"))
    check("Grafana", grafana)
    check("Postgres", lambda: "answers" if ctx.pg.query("admin", "SELECT 1")[0][0] == 1 else (_ for _ in ()).throw(StageFail("SELECT 1 did not return 1")))
    if problems:
        raise StageFail("; ".join(problems) + (" | ok: " + "; ".join(notes) if notes else ""))
    return "; ".join(notes)


# ---------------------------------------------------------------------------------------------- stages 2 to 4
def stage_host_tests(ctx):
    res = ctx.run([sys.executable, "-m", "pytest", "tests", "-q", "-p", "no:cacheprovider"], timeout=3600)
    counts = parse_pytest_summary(res.stdout or "")
    if counts is None:
        raise StageFail("no pytest summary line (exit code %d)\n%s" % (res.returncode, tail(res.stdout or res.stderr)))
    text = "%d passed, %d failed, %d errors, %d skipped" % (counts["passed"], counts["failed"], counts["errors"], counts["skipped"])
    if res.returncode != 0 or counts["failed"] or counts["errors"]:
        raise StageFail(text + "\n" + tail(res.stdout))
    return text


def spark_test_files(root=ROOT):
    tests = os.path.join(root, "tests")
    return sorted(f for f in os.listdir(tests) if re.fullmatch(r"test_.*spark.*\.py", f))


def stage_spark_tests(ctx):
    results = []
    for name in spark_test_files(ctx.root):
        t0 = ctx.clock()
        argv = ["docker", "compose", "run", "--rm", "-e", "PG_DSN=", "-e", "RUN_SPARK_TESTS=1", "spark", "python", "-m",
                "pytest", "tests/" + name, "-q", "-p", "no:cacheprovider"]
        label = "spark tests: " + name
        try:
            res = ctx.run(argv, timeout=3600)
            counts = parse_pytest_summary(res.stdout or "")
            if counts is None:
                results.append(Result(label, FAIL, "no pytest summary (exit code %d)" % res.returncode, ctx.clock() - t0,
                                      tail((res.stdout or "") + (res.stderr or ""))))
                continue
            text = "%d passed, %d failed, %d errors, %d skipped" % (counts["passed"], counts["failed"], counts["errors"], counts["skipped"])
            bad = res.returncode != 0 or counts["failed"] or counts["errors"] or not counts["passed"]
            results.append(Result(label, FAIL if bad else PASS, text, ctx.clock() - t0, tail(res.stdout) if bad else ""))
        except Exception as e:
            results.append(Result(label, FAIL, type(e).__name__, ctx.clock() - t0, str(e)[:1500]))
    if not results:
        raise StageFail("no tests/test_*spark*.py file found")
    return results


def stage_static(ctx):
    import yaml
    from scripts.gen_dashboards import FILES
    problems, notes = [], []
    res = ctx.run([sys.executable, "scripts/gen_ddl.py", "--check"], timeout=300)
    if res.returncode != 0:
        problems.append("gen_ddl.py --check exited with code %d: %s" % (res.returncode, tail((res.stdout or "") + (res.stderr or ""), 3, 300)))
    else:
        notes.append("gen_ddl --check ok")
    files = sorted(f for f in os.listdir(GRAFANA_DASHBOARDS) if f.endswith(".json"))
    docs = {}
    for name in files:
        try:
            with open(os.path.join(GRAFANA_DASHBOARDS, name), encoding="utf-8") as fh:
                docs[name] = json.load(fh)
        except ValueError as e:
            problems.append("%s is not valid JSON: %s" % (name, e))
            continue
        ids = [i for i in panel_ids(docs[name])]
        dup = sorted({i for i in ids if ids.count(i) > 1}, key=str)
        if dup:
            problems.append("%s has duplicate panel ids %s" % (name, dup))
    notes.append("%d dashboards valid" % len(docs))
    for name, build in FILES.items():
        if name in docs and json.loads(json.dumps(build())) != docs[name]:
            problems.append("%s differs from its builder in monitoring/dashboards.py (run scripts/gen_dashboards.py)" % name)
    notes.append("%d builders equal their files" % len(FILES))
    with open(ALERTING, encoding="utf-8") as fh:
        rules = [r for g in yaml.safe_load(fh)["groups"] for r in g["rules"]]
    for r in rules:
        if r.get("condition") != "C":
            problems.append("alert rule %s: condition is %r, not C" % (r.get("uid"), r.get("condition")))
        if not (r.get("annotations") or {}).get("runbook"):
            problems.append("alert rule %s has no runbook" % r.get("uid"))
    notes.append("%d alert rules with condition C and a runbook" % len(rules))
    if problems:
        raise StageFail("\n".join(problems))
    return "; ".join(notes)


# ---------------------------------------------------------------------------------------------- stages 5 and 6
def stage_reset_generate(ctx):
    a = ctx.args
    if os.path.isdir(SMOKE_DIR):
        ctx.rmtree(SMOKE_DIR)
    os.makedirs(SMOKE_DIR, exist_ok=True)
    ctx.run_ok(["docker", "compose", "rm", "-sf", "kafka"], "docker compose rm kafka", timeout=300)
    ctx.run_ok(["docker", "compose", "up", "-d", "kafka"], "docker compose up kafka", timeout=300)
    start = ctx.clock()
    while ctx.run(KAFKA_LIST, timeout=60).returncode != 0:
        if ctx.clock() - start > KAFKA_WAIT_SECONDS:
            raise StageFail("Kafka did not answer within %d s" % KAFKA_WAIT_SECONDS)
        ctx.sleep(2)
    out = ctx.run_ok([sys.executable, "kafka/create_topics.py"], "create_topics.py", timeout=120)
    missing = [t for t in TOPICS if "created " + t not in out]
    if missing:
        raise StageFail("topics not created: %s\n%s" % (", ".join(missing), tail(out)))
    ctx.pg.recreate_database()
    ctx.run_ok(ctx.spark("scripts/apply_live_ddl.py", "--lake", ctx.lake), "apply_live_ddl.py", timeout=900)
    sim = ctx.run_ok(sim_command(a, ctx.seed), "run_simulator.py", timeout=3600)
    counts, total = parse_simulator_output(sim)
    ctx.state.update(sim_counts=counts, sim_total=total)
    if counts.get("inventory.events", 0) <= 0:
        raise StageFail("inventory.events has no message: %s" % counts)
    return "seed %d; %s; total %d" % (ctx.seed, ", ".join("%s=%d" % (t, counts.get(t, 0)) for t in TOPICS), total)


def sim_command(args, seed):
    return [sys.executable, "scripts/run_simulator.py", "--events", str(args.events), "--seed", str(seed),
            "--universe-seed", "7", "--bootstrap", "localhost:9092", "--manifest", SMOKE_REL + "/noise_manifest.json",
            "--inventory"]


def tiny_sim_command(args, seed, start):
    return [sys.executable, "scripts/run_simulator.py", "--events", str(TINY_EVENTS), "--seed", str(seed + 1),
            "--universe-seed", "7", "--bootstrap", "localhost:9092", "--manifest", SMOKE_REL + "/noise_manifest_tiny.json",
            "--start", start, "--noise-off", "--scenario", "hero-stockout", "--express-promise-minutes", str(TINY_PROMISE_MINUTES),
            "--late-express-every", "1"]


def bronze_command(ctx):
    return ctx.spark("-m", "streaming.jobs.bronze_ingest", "--once", "--lake", ctx.lake, "--checkpoints", ctx.checkpoints)


def silver_command(ctx):
    return ctx.spark("-m", "streaming.jobs.silver_clean", "--once", "--lake", ctx.lake, "--checkpoints", ctx.checkpoints)


def read_counts(ctx):
    return parse_json_line(ctx.run_ok(ctx.spark("scripts/smoke_spark_checks.py", "counts", "--lake", ctx.lake), "smoke_spark_checks counts", timeout=900))


def stage_bronze_silver(ctx):
    ctx.run_ok(bronze_command(ctx), "bronze_ingest --once", timeout=1800)
    ctx.run_ok(ctx.spark("scripts/seed_silver_reference.py", "--lake", ctx.lake), "seed_silver_reference.py", timeout=900)
    ctx.run_ok(silver_command(ctx), "silver_clean --once", timeout=1800)
    ctx.run_ok(ctx.spark("scripts/apply_live_ddl.py", "--lake", ctx.lake), "apply_live_ddl.py (product names)", timeout=900)
    counts = read_counts(ctx)
    ctx.state.update(counts=counts, data_day=counts.get("data_day"))
    problems = []
    bronze = sum(v or 0 for v in counts["bronze"].values())
    if bronze != ctx.state["sim_total"]:
        problems.append("Bronze rows %d differ from the simulator total %d (%s)" % (bronze, ctx.state["sim_total"], counts["bronze"]))
    empty = [t for t in SILVER_NON_EMPTY if not counts["silver"].get(t)]
    if empty:
        problems.append("Silver table(s) missing or empty: %s" % ", ".join(empty))
    if problems:
        raise StageFail("\n".join(problems))
    return "Bronze %d rows = simulator total; Silver: %s" % (bronze, ", ".join(
        "%s=%d" % (t.replace("silver_", ""), counts["silver"][t]) for t in SILVER_NON_EMPTY))


# ---------------------------------------------------------------------------------------------- stage 7
def backfill_express(ctx):
    """live_express_orders from all of Silver, in the smoke database. Why: the live sink writes only the orders of the last
    3 hours of the wall clock and a history run is days old, so without this the table is empty after the run."""
    return parse_json_line(ctx.run_ok(ctx.spark("scripts/smoke_spark_checks.py", "express-backfill", "--lake", ctx.lake),
                                      "smoke_spark_checks express-backfill", timeout=1800))


def stage_invariants(ctx):
    from warehouse import checks as ck
    problems, notes = [], []
    dup = ctx.run(ctx.spark("scripts/check_duplicates.py", "--lake", ctx.lake), timeout=900)
    if dup.returncode != 0:
        problems.append("check_duplicates.py exited with code %d\n%s" % (dup.returncode, tail(dup.stdout)))
    else:
        notes.append("no duplicate key in any Silver table")
    fill = backfill_express(ctx)
    if not fill.get("express_rows"):
        problems.append("silver_express_orders gave no express order to put into live_express_orders")
    else:
        notes.append("live_express_orders filled from Silver: %d orders" % fill["express_rows"])
    inv = parse_json_line(ctx.run_ok(ctx.spark("scripts/smoke_spark_checks.py", "invariants", "--lake", ctx.lake),
                                     "smoke_spark_checks invariants", timeout=1800))
    expected = ck.expected_quarantine(ck.load_manifest(os.path.join(SMOKE_DIR, "noise_manifest.json")))
    observed = inv["quarantine"]
    if observed is None or not ck.within_tolerance(observed, expected):
        problems.append("quarantine: expected %d from the manifest (2%% tolerance), observed %s" % (expected, observed))
    else:
        notes.append("quarantine %d (manifest %d)" % (observed, expected))
    if not inv["pairs"]:
        problems.append("stock: no store and product pair has a snapshot")
    if inv.get("negative_count"):
        problems.append("stock replay goes below zero for %d pair(s), e.g. %s" % (inv["negative_count"], ", ".join(inv["negative"])))
    if inv.get("mismatch_count"):
        problems.append("live rule differs from the ledger replay for %d pair(s), e.g. %s" % (inv["mismatch_count"], "; ".join(inv["mismatch"])))
    if inv["pairs"] and not inv.get("negative_count") and not inv.get("mismatch_count"):
        notes.append("stock: %d pairs, replay never below zero, live rule = replay" % inv["pairs"])
    negative_now = ctx.scalar("SELECT count(*) FROM live_stock_now WHERE available < 0")
    if negative_now:
        problems.append("live_stock_now has %d row(s) with available < 0" % negative_now)
    express = ctx.smoke_sql(
        "SELECT count(*), count(*) FILTER (WHERE promised_minutes IS NULL), "
        "count(*) FILTER (WHERE delivered_at IS NOT NULL AND delivered_at - created_at >= promised_minutes * interval '1 minute'), "
        "max(EXTRACT(EPOCH FROM (delivered_at - created_at)) / 60.0), max(promised_minutes) FROM live_express_orders")[0]
    n_express, no_promise, breaches, slowest, promise = express
    if not n_express:
        problems.append("live_express_orders is empty")
    else:
        if no_promise:
            problems.append("%d express order(s) have no promised_minutes" % no_promise)
        if breaches:
            problems.append("%d express order(s) were delivered at or after their promise in the clean run" % breaches)
        if not no_promise and not breaches:
            notes.append("express: %d orders, slowest %.1f min against a promise of %s" % (n_express, _number(slowest) or 0, promise))
    for table in LIVE_POPULATED:
        n = ctx.scalar("SELECT count(*) FROM %s" % table)
        if not n:
            problems.append("%s is empty" % table)
    beat = ctx.scalar("SELECT count(*) FROM pipeline_heartbeat WHERE component = 'silver_job'")
    if not beat:
        problems.append("pipeline_heartbeat has no silver_job row")
    if not problems:
        notes.append("live tables populated, heartbeat silver_job present")
    if problems:
        raise StageFail("\n".join(problems) + ("\nok: " + "; ".join(notes) if notes else ""))
    return "; ".join(notes)


# ---------------------------------------------------------------------------------------------- stage 8
def stage_daily_marts(ctx):
    day = ctx.state.get("data_day")
    if not day:
        raise StageFail("the Cairo day of the data is unknown")
    datetime.date.fromisoformat(day)  # the day goes into SQL text below: only a real date gets through
    ctx.run_ok(ctx.spark("scripts/build_inventory_daily.py", "--date", day, "--days", "2", "--lake", ctx.lake),
               "build_inventory_daily.py", timeout=1800)
    rows = {t: ctx.scalar("SELECT count(*) FROM %s" % t) for t in ("inventory_daily", "oos_demand_daily", "express_daily")}
    problems = ["%s has no row" % t for t, n in rows.items() if not n]
    closing = ctx.scalar("SELECT sum(closing_on_hand) FROM inventory_daily WHERE report_date = DATE '%s'" % day)
    live = ctx.scalar("SELECT sum(l.on_hand) FROM live_stock_now l JOIN inventory_daily i ON i.store_id = l.store_id "
                      "AND i.product_id = l.product_id AND i.report_date = DATE '%s'" % day)
    if closing is None or live is None or int(closing) != int(live):
        problems.append("closing stock of %s is %s, the sum of live_stock_now.on_hand for the same pairs is %s" % (day, closing, live))
    notes = ["%s: %s rows" % (t, n) for t, n in rows.items()] + ["closing stock of %s = live_stock_now (%s)" % (day, closing)]
    if problems:
        raise StageFail("\n".join(problems) + "\nok: " + "; ".join(notes))
    return "; ".join(notes)


# ---------------------------------------------------------------------------------------------- stage 9
def stage_dashboards(ctx):
    ran, failed = 0, []
    for name in sorted(os.listdir(GRAFANA_DASHBOARDS)):
        if not name.endswith(".json"):
            continue
        with open(os.path.join(GRAFANA_DASHBOARDS, name), encoding="utf-8") as fh:
            doc = json.load(fh)
        for title, sql in dashboard_queries(doc):
            try:
                ctx.pg.query("ro", expand_macros(sql, days=DASHBOARD_DAYS))
                ran += 1
            except StageFail:
                raise
            except Exception as e:
                failed.append("%s / %s: %s" % (name, title, " ".join(str(e).split())[:250]))
    if failed:
        raise StageFail("%d panel(s) ran, %d failed\n%s" % (ran, len(failed), "\n".join(failed)))
    return "%d panel queries ran without an error (read-only role)" % ran


# ---------------------------------------------------------------------------------------------- stage 10
def alert_rules():
    import yaml
    with open(ALERTING, encoding="utf-8") as fh:
        return [r for g in yaml.safe_load(fh)["groups"] for r in g["rules"]]


def rule_query(rule):
    return next(d["model"]["rawSql"] for d in rule["data"] if d["refId"] == "A")


def evaluate_rule(ctx, rule, instant=None):
    """(value, firing) of a rule. instant pins now(); no row means no data and the rule's noDataState decides."""
    sql = rule_query(rule)
    if instant:
        sql = pin_now(sql, instant)
    rows = ctx.pg.query("ro", sql)
    value = _number(rows[-1][-1]) if rows else None
    kind, limit = rule_threshold(rule)
    fires = apply_threshold(kind, limit, value)
    if fires is None:
        fires = rule.get("noDataState") == "Alerting"
    return value, bool(fires)


EXPRESS_RULE, OOS_RULE = "fs-express-sla", "fs-oos-demand"
EXPRESS_RULES = (EXPRESS_RULE, "fs-express-ontime")  # both read live_express_orders, so both are pinned to the end of the data
# The express rule looks at the last 60 minutes before now(); the data is days old, so it is evaluated as of the end of its
# data. Before the breach run that is the end of the clean orders; after it, the end of the breach run's orders (the only
# ones with a 2 minute promise), because the clean data may reach further than the small run.
AS_OF_CLEAN = "SELECT max(coalesce(delivered_at, created_at)) FROM live_express_orders"
AS_OF_BREACH = AS_OF_CLEAN + " WHERE promised_minutes = %d" % TINY_PROMISE_MINUTES


def evaluate_all(ctx, rules, express_as_of):
    """{uid: (value, firing, as_of)}; the express rule is evaluated as of the end of its data (express_as_of is the query
    that gives the moment), every other rule at the real now()."""
    out = {}
    for rule in rules:
        uid, instant = rule["uid"], None
        if uid in EXPRESS_RULES:
            instant = ctx.scalar(express_as_of, who="smoke")
            instant = instant.isoformat() if hasattr(instant, "isoformat") else instant
        value, firing = evaluate_rule(ctx, rule, instant)
        out[uid] = (value, firing, instant)
    return out


def describe(results):
    return "; ".join("%s=%s %s" % (uid, "-" if v is None else ("%g" % v), "firing" if f else "not firing")
                     for uid, (v, f, _) in sorted(results.items()))


def check_demand_rule(ctx, rules):
    """The out-of-stock demand rule on rows the stage writes itself (fs-oos-demand needs 5 add-to-carts of one pair in two
    minutes of a 3 minute window, all flagged oos_at_write).

    Why controlled: the flag is the stock at the moment the sink wrote the minute (not replay-safe). A history run is
    written in one or a few batches and the simulator's ledger restocks every pair before the run ends, so the real run
    cannot be made to leave a pair empty. The flag logic is tested in Spark (tests/test_live_stock_spark.py); this checks the
    rule's SQL and threshold on the real database and the read-only role. Returns (text, problems); the rows are removed."""
    rule = next(r for r in rules if r["uid"] == OOS_RULE)
    minute = ctx.utcnow().replace(second=0, microsecond=0) - datetime.timedelta(hours=1)
    first, second = minute - datetime.timedelta(minutes=2), minute - datetime.timedelta(minutes=1)
    instant = (minute + datetime.timedelta(minutes=1)).isoformat()
    limit = rule_threshold(rule)[1]
    problems, said = [], []
    try:
        values = ", ".join("(TIMESTAMPTZ '%s', '%s', '%s', 'direct', '', 5, 5, 5, true)"
                           % (t.isoformat(), CONTROL_STORE, CONTROL_PRODUCT) for t in (first, second))
        ctx.pg.query("smoke", "INSERT INTO live_oos_demand_minute (minute_start, store_id, product_id, campaign_id, utm_campaign, "
                              "views, add_to_cart, landing_clicks, oos_at_write) VALUES " + values)
        value, fires = evaluate_rule(ctx, rule, instant)
        if not fires:
            problems.append("%s does not fire on 5 add-to-carts of one pair in two minutes, flagged out of stock (value %s, "
                            "limit %s, as of %s)" % (OOS_RULE, value, limit, instant))
        else:
            said.append("fires with 5 add-to-carts in two minutes (value %g)" % value)
        ctx.pg.query("smoke", "UPDATE live_oos_demand_minute SET add_to_cart = 4 WHERE store_id = '%s'" % CONTROL_STORE)
        value, fires = evaluate_rule(ctx, rule, instant)
        if fires:
            problems.append("%s fires on 4 add-to-carts (value %s): below the threshold of 5" % (OOS_RULE, value))
        else:
            said.append("quiet with 4")
    finally:
        ctx.pg.query("smoke", "DELETE FROM live_oos_demand_minute WHERE store_id = '%s'" % CONTROL_STORE)
    return "; ".join(said), problems


def stage_alerts(ctx):
    rules = alert_rules()
    problems = []
    backfill_express(ctx)  # the clean run is history: the live sink wrote no express order (see backfill_express)
    before = evaluate_all(ctx, rules, AS_OF_CLEAN)
    if before[EXPRESS_RULE][2] is None:
        problems.append("%s: no express order in live_express_orders after the backfill, so the clean run proves nothing" % EXPRESS_RULE)
    elif before[EXPRESS_RULE][1]:
        problems.append("%s fires on the clean run (value %s, as of %s): no late express order exists there"
                        % (EXPRESS_RULE, before[EXPRESS_RULE][0], before[EXPRESS_RULE][2]))
    ctx.run_ok(tiny_sim_command(ctx.args, ctx.seed, ctx.start2), "run_simulator.py (breach and hero stock-out)", timeout=1800)
    ctx.run_ok(bronze_command(ctx), "bronze_ingest --once (breach run)", timeout=1800)
    ctx.run_ok(silver_command(ctx), "silver_clean --once (breach run)", timeout=1800)
    backfill_express(ctx)
    after = evaluate_all(ctx, rules, AS_OF_BREACH)
    if after[EXPRESS_RULE][2] is None:
        problems.append("%s: no express order with a %d minute promise after the breach run (no row to evaluate)"
                        % (EXPRESS_RULE, TINY_PROMISE_MINUTES))
    elif not after[EXPRESS_RULE][1]:
        problems.append("%s does not fire on the late express orders (promise %d minutes, every order late; value %s, as of %s)"
                        % (EXPRESS_RULE, TINY_PROMISE_MINUTES, after[EXPRESS_RULE][0], after[EXPRESS_RULE][2]))
    control, control_problems = check_demand_rule(ctx, rules)
    problems += control_problems
    ctx.state["alerts_after"] = after
    text = "clean: %s | breach run: %s | demand rule on controlled rows: %s" % (describe(before), describe(after), control)
    if problems:
        raise StageFail("\n".join(problems) + "\n" + text)
    return text


# ---------------------------------------------------------------------------------------------- stage 11
def stage_replay(ctx):
    before = read_counts(ctx)
    ctx.rmtree(os.path.join(ROOT, ctx.checkpoints, "silver_clean"))
    ctx.run_ok(silver_command(ctx), "silver_clean --once (full replay)", timeout=1800)
    after = read_counts(ctx)
    problems = []
    for group in ("bronze", "silver"):
        for t, n in before[group].items():
            if after[group].get(t) != n:
                problems.append("%s: %s rows before the replay, %s after" % (t, n, after[group].get(t)))
    dup = ctx.run(ctx.spark("scripts/check_duplicates.py", "--lake", ctx.lake), timeout=900)
    if dup.returncode != 0:
        problems.append("check_duplicates.py after the replay exited with code %d\n%s" % (dup.returncode, tail(dup.stdout)))
    if problems:
        raise StageFail("\n".join(problems))
    return "replay of Bronze: %d Silver tables unchanged, no duplicate key" % len(before["silver"])


# ---------------------------------------------------------------------------------------------- stage 12
def stage_databricks(ctx):
    a, lake = ctx.args, ctx.lake
    if not ctx.state.get("dbx_ddl_done"):
        ctx.run_ok(ctx.spark("scripts/apply_ddl.py", "--backend", "databricks", "--catalog", a.catalog, dbx=True, pg=False),
                   "apply_ddl.py", timeout=1800)
    ctx.run_ok(ctx.spark("scripts/publish_silver.py", "--lake", lake, "--export-dir", SMOKE_REL + "/export",
                         "--reset", "--yes", dbx=True, pg=False),
               "publish_silver.py", timeout=3600)
    out = ctx.run_ok(ctx.spark("scripts/run_batch.py", "--skip-publish", "--full", "--manifest", SMOKE_REL + "/noise_manifest.json",
                               dbx=True, pg=False), "run_batch.py", timeout=3600)
    if "BATCH OK" not in out:
        raise StageFail("no BATCH OK line\n" + tail(out))
    m = re.search(r"checks: (\d+) run, (\d+) failed", out)
    if m and int(m.group(2)):
        raise StageFail("%s data quality check(s) failed\n%s" % (m.group(2), tail(out)))
    return "BATCH OK in catalog %s%s" % (a.catalog, ", checks: %s run, %s failed" % m.groups() if m else "")


# ---------------------------------------------------------------------------------------------- stage 13
def stage_d1_marketing(ctx):
    """The marketing D-1 mart: demand from Postgres joined with the ad spend of agg_campaign_daily, so it needs the Gold
    tables of the Databricks batch (stage 12) and cannot run before them."""
    day = ctx.state.get("data_day")
    if not day:
        raise StageFail("the Cairo day of the data is unknown")
    datetime.date.fromisoformat(day)  # the day goes into SQL text below: only a real date gets through
    out = ctx.run_ok(ctx.spark("scripts/build_d1_marketing.py", "--date", day, dbx=True), "build_d1_marketing.py", timeout=1800)
    m = re.search(r"D-1 (\d{4}-\d{2}-\d{2}): (\d+) campaign rows, (\d+) product rows", out)
    if not m or m.group(1) != day:
        raise StageFail("no 'D-1 %s: <n> campaign rows, <n> product rows' line in the output\n%s" % (day, tail(out)))
    printed_campaigns, printed_products = int(m.group(2)), int(m.group(3))
    campaigns = ctx.scalar("SELECT count(*) FROM marketing_d1_campaign WHERE report_date = DATE '%s'" % day)
    products = ctx.scalar("SELECT count(*) FROM marketing_d1_product WHERE report_date = DATE '%s'" % day)
    problems = []
    if int(campaigns or 0) != printed_campaigns:
        problems.append("the build prints %d campaign rows but marketing_d1_campaign holds %s for %s" % (printed_campaigns, campaigns, day))
    if int(products or 0) != printed_products:
        problems.append("the build prints %d product rows but marketing_d1_product holds %s for %s" % (printed_products, products, day))
    if problems:
        raise StageFail("\n".join(problems))
    notes = ["marketing_d1_campaign: %d rows" % printed_campaigns, "marketing_d1_product: %d rows" % printed_products]
    if not printed_campaigns:
        # One campaign row is built per last-touch ad row of the day, so none means agg_campaign_daily has no ad row for it.
        notes.append("campaign mart empty by design: agg_campaign_daily has no last-touch ad row for %s (the ad API pull covers "
                     "its own dates, not necessarily the simulated day)" % day)
    if not printed_products:
        notes.append("no product rows: no demand was flagged out of stock at write time on %s (history runs flag against the "
                     "final stock)" % day)
    return "; ".join(notes)


def stage_power_bi(ctx):
    from warehouse.tables import GOLD_TABLES
    with open(MODEL_MD, encoding="utf-8") as fh:
        sources = parse_model_sources(fh.read())
    with open(MEASURES_DAX, encoding="utf-8") as fh:
        refs = parse_dax_refs(fh.read())
    gold = {t.name: set(t.column_names()) for t in GOLD_TABLES}
    pg = {}
    for table, column in ctx.pg.query("smoke", "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'public'"):
        pg.setdefault(table, set()).add(column)
    if not sources["gold"] or not sources["postgres"]:
        raise StageFail("MODEL.md lists no %s source table" % ("Gold" if not sources["gold"] else "Postgres"))
    problems = check_power_bi_sources(sources, refs, gold, pg)
    if problems:
        raise StageFail("\n".join(problems))
    measures = {m for m, _, _ in refs}
    return "%d Gold and %d Postgres source tables exist; %d column references of %d measures resolve" % (
        len(sources["gold"]), len(sources["postgres"]), len(set(refs)), len(measures))


# ---------------------------------------------------------------------------------------------- plan and runner
def plan(args, seed, start2, py=None):
    a, lake, ck = args, _rel(args.lake) or args.lake, _rel(args.checkpoints) or args.checkpoints
    dummy = Ctx.__new__(Ctx)
    dummy.args, dummy.lake, dummy.checkpoints = a, lake, ck
    sp = lambda *c, **k: spark_cmd(a, *c, **k)  # noqa: E731
    gold = "--skip-gold" if a.skip_gold else None
    return [
        Stage("preflight", "Preflight", stage_preflight, (),
              [["docker", "info"], ["docker", "ps", "--filter", "name=fs-"], ["(Kafka client handshake, Grafana /api/health, Postgres SELECT 1)"]],
              "Docker, clock, memory, Kafka, Grafana and its data source, Postgres, no demo container"),
        Stage("host_tests", "Host tests", stage_host_tests, (), [[sys.executable, "-m", "pytest", "tests", "-q", "-p", "no:cacheprovider"]],
              "zero failures and zero errors"),
        Stage("spark_tests", "Spark tests", stage_spark_tests, ("preflight",),
              [["docker", "compose", "run", "--rm", "-e", "PG_DSN=", "-e", "RUN_SPARK_TESTS=1", "spark", "python", "-m", "pytest", "tests/test_*spark*.py", "-q", "-p", "no:cacheprovider"]],
              "one container per file, one result row per file", "--skip-spark-tests" if a.skip_spark_tests else None),
        Stage("static", "Static checks", stage_static, (), [[sys.executable, "scripts/gen_ddl.py", "--check"], ["(dashboard JSON, builders against files, alert rules)"]],
              "generated DDL, dashboards and alert rules"),
        Stage("reset_generate", "Reset and generate", stage_reset_generate, ("preflight",),
              [["docker", "compose", "rm", "-sf", "kafka"], ["docker", "compose", "up", "-d", "kafka"], KAFKA_LIST, [sys.executable, "kafka/create_topics.py"],
               ["(drop and create database %s)" % SMOKE_DB], sp("scripts/apply_live_ddl.py", "--lake", lake), sim_command(a, seed)],
              "deletes %s, recreates Kafka (all messages lost), recreates database %s" % (SMOKE_REL, SMOKE_DB)),
        Stage("bronze_silver", "Bronze and Silver", stage_bronze_silver, ("reset_generate",),
              [sp("-m", "streaming.jobs.bronze_ingest", "--once", "--lake", lake, "--checkpoints", ck), sp("scripts/seed_silver_reference.py", "--lake", lake),
               sp("-m", "streaming.jobs.silver_clean", "--once", "--lake", lake, "--checkpoints", ck), sp("scripts/apply_live_ddl.py", "--lake", lake),
               sp("scripts/smoke_spark_checks.py", "counts", "--lake", lake)], "Bronze rows equal the simulator total, every Silver table is non-empty"),
        Stage("invariants", "Data invariants", stage_invariants, ("bronze_silver",),
              [sp("scripts/check_duplicates.py", "--lake", lake), sp("scripts/smoke_spark_checks.py", "express-backfill", "--lake", lake),
               sp("scripts/smoke_spark_checks.py", "invariants", "--lake", lake), ["(SQL on the live tables)"]],
              "duplicates, quarantine, stock replay, express promise (live_express_orders filled from Silver), live tables"),
        Stage("daily_marts", "Daily marts", stage_daily_marts, ("bronze_silver",),
              [sp("scripts/build_inventory_daily.py", "--date", "<Cairo day of the data>", "--days", "2", "--lake", lake)],
              "inventory_daily, oos_demand_daily, express_daily (Postgres only)", None),
        Stage("dashboards", "Dashboards run", stage_dashboards, ("bronze_silver",), [["(every panel query of every dashboard, as grafana_ro)"]],
              "Grafana macros expanded, every query must run"),
        Stage("alerts", "Alert logic", stage_alerts, ("bronze_silver",),
              [sp("scripts/smoke_spark_checks.py", "express-backfill", "--lake", lake), ["(query A of every rule, threshold applied)"],
               tiny_sim_command(a, seed, start2), bronze_command(dummy), silver_command(dummy),
               sp("scripts/smoke_spark_checks.py", "express-backfill", "--lake", lake), ["(rules again)"],
               ["(fs-oos-demand on controlled rows written to and removed from live_oos_demand_minute)"]],
              "second small run: late express orders; the demand rule is checked on controlled rows"),
        Stage("replay", "Replay safety", stage_replay, ("bronze_silver",),
              [["(delete %s/silver_clean)" % ck], silver_command(dummy), sp("scripts/check_duplicates.py", "--lake", lake)],
              "full replay of Bronze: counts and duplicates unchanged"),
        Stage("databricks", "Databricks batch", stage_databricks, ("bronze_silver",),
              [sp("scripts/apply_ddl.py", "--backend", "databricks", "--catalog", a.catalog, dbx=True, pg=False),
               sp("scripts/publish_silver.py", "--lake", lake, "--export-dir", SMOKE_REL + "/export", dbx=True, pg=False),
               sp("scripts/run_batch.py", "--skip-publish", "--full", "--manifest", SMOKE_REL + "/noise_manifest.json", dbx=True, pg=False)],
              "scratch catalog %s" % a.catalog, gold),
        Stage("d1_marketing", "Marketing D-1 mart", stage_d1_marketing, ("bronze_silver", "databricks"),
              [sp("scripts/build_d1_marketing.py", "--date", "<Cairo day of the data>", dbx=True)],
              "campaign rows = ad rows of the day (empty only with a stated reason), needs the Gold tables", gold),
        Stage("power_bi", "Power BI source check", stage_power_bi, ("reset_generate",), [["(MODEL.md tables, measures.dax columns, information_schema)"]],
              "every source table exists with the columns the measures use"),
    ]


def format_plan(stages, args, seed):
    lines = ["Smoke test plan: %d events, seed %d, catalog %s, lake %s" % (args.events, seed, args.catalog, args.lake)]
    n = 0
    for s in stages:
        n += 1
        lines.append("%2d. %s%s" % (n, s.title, "  (%s)" % s.note if s.note else ""))
        if s.skip_reason:
            lines.append("      skipped (%s)" % s.skip_reason)
            continue
        if s.depends:
            lines.append("      needs: " + ", ".join(s.depends))
        for c in s.commands:
            lines.append("      " + subprocess.list2cmdline(c))
    lines.append("%2d. Report  (writes docs/smoke-test.md, the verdict line and the exit code)" % (n + 1))
    return "\n".join(lines)


def run_stages(ctx, stages):
    """Run the stages in order; returns [Result]. A failing stage does not stop the independent ones."""
    results, status, blocked_by = [], {}, {}
    for st in stages:
        if st.skip_reason:
            results.append(Result(st.title, SKIPPED, "skipped: " + st.skip_reason))
            status[st.key] = SKIPPED
            continue
        root = None
        for d in st.depends:
            if status.get(d) == FAIL:
                root = d
                break
            if status.get(d) == SKIPPED and d in blocked_by:
                root = blocked_by[d]
                break
        if root is not None:
            results.append(Result(st.title, SKIPPED, "skipped: %s failed" % root))
            status[st.key], blocked_by[st.key] = SKIPPED, root
            continue
        print("--- stage %s ---" % st.title, flush=True)
        t0 = ctx.clock()
        try:
            out = st.func(ctx)
            seconds = ctx.clock() - t0
            if isinstance(out, list):
                for r in out:
                    r.seconds = r.seconds or seconds
                results += out
                status[st.key] = FAIL if any(r.status == FAIL for r in out) else PASS
            else:
                results.append(Result(st.title, PASS, out, seconds))
                status[st.key] = PASS
        except Exception as e:  # a stage that cannot run is a failed stage, with the reason in full
            seconds = ctx.clock() - t0
            text = str(e) if isinstance(e, StageFail) else "%s: %s" % (type(e).__name__, e)
            first = text.strip().splitlines()[0][:300] if text.strip() else type(e).__name__
            results.append(Result(st.title, FAIL, first, seconds, text))
            status[st.key] = FAIL
        if status[st.key] == FAIL:
            blocked_by[st.key] = st.key
    return results


# ---------------------------------------------------------------------------------------------- report
def verdict(results):
    failed = sum(1 for r in results if r.status == FAIL)
    return "SMOKE TEST PASSED" if not failed else "SMOKE TEST FAILED: %d stage%s failed" % (failed, "" if failed == 1 else "s")


def _cell(text):
    return " ".join((text or "").split()).replace("|", "/")


def render_report(results, meta):
    skipped = sum(1 for r in results if r.status == SKIPPED)
    lines = ["# Smoke test", "",
             "Date: %s" % meta["date"], "",
             "Command: `%s`" % meta["command"], "",
             "Git commit: %s" % meta["commit"], "",
             "Environment: %s; Python %s; Docker memory %s; host memory %s; clock offset to Docker %s; NTP %s" % (
                 meta["os"], meta["python"], meta["docker_memory"], meta["host_memory"], meta["clock_offset"], meta["ntp"]), "",
             "| # | stage | status | evidence | seconds |", "|---|---|---|---|---|"]
    for i, r in enumerate(results, 1):
        lines.append("| %d | %s | %s | %s | %.1f |" % (i, _cell(r.name), r.status, _cell(r.evidence), r.seconds))
    failures = [r for r in results if r.status == FAIL]
    if failures:
        lines += ["", "## Failures", ""]
        for r in failures:
            lines += ["### %s" % r.name, "", "```", (r.detail or r.evidence).strip(), "```", ""]
    if skipped:
        lines += ["", "%d stage%s skipped; a skipped stage proves nothing." % (skipped, "" if skipped == 1 else "s")]
    lines += ["", verdict(results)]
    return "\n".join(lines) + "\n"


def collect_meta(ctx, raw):
    def git():
        try:
            res = ctx.runner(["git", "rev-parse", "--short", "HEAD"], check=False, capture_output=True, text=True, cwd=ROOT)
            return res.stdout.strip() if res.returncode == 0 and res.stdout.strip() else "unknown"
        except Exception:
            return "unknown"
    env = ctx.state["env"]
    return {"date": ctx.utcnow().date().isoformat(), "command": "python scripts/smoke_test.py " + subprocess.list2cmdline(raw),
            "commit": git(), "os": platform.platform(), "python": platform.python_version(),
            "docker_memory": env.get("docker_memory", "unknown"), "host_memory": env.get("host_memory", "unknown"),
            "clock_offset": env.get("clock_offset", "unknown"), "ntp": env.get("ntp", "unknown")}


def _running_demo_containers(runner):
    res = runner(["docker", "ps", "--filter", "name=" + DEMO_PREFIX, "--format", "{{.Names}}"], check=False, capture_output=True,
                 text=True, encoding="utf-8", errors="replace", cwd=ROOT)
    if res.returncode != 0:
        raise RuntimeError("docker ps failed; cannot check for running demo containers")
    return sorted(n for n in (res.stdout or "").split() if n.startswith(DEMO_PREFIX))


def main(argv=None, ctx_factory=None, runner=subprocess.run, utcnow=None):
    raw = list(sys.argv[1:] if argv is None else argv)
    args = parse_args(raw)
    refusal = guard(args)
    if refusal:
        print("error: " + refusal, file=sys.stderr)
        return 3
    now = (utcnow or (lambda: datetime.datetime.now(datetime.timezone.utc)))()
    seed = args.seed if args.seed is not None else int(now.timestamp()) % 1_000_000
    start2 = (now - datetime.timedelta(days=TINY_DAYS_AGO)).replace(second=0, microsecond=0).isoformat()
    stages = plan(args, seed, start2)
    print(format_plan(stages, args, seed))
    if args.dry_run:
        print("DRY RUN: nothing was executed.")
        return 0
    if not args.yes:
        print("\nThis run recreates the Kafka container (all messages in Kafka are lost), deletes %s, recreates the database %s "
              "and writes to the catalog %s. The Bronze checkpoints of a real demo (data/checkpoints) then point at offsets that no "
              "longer exist: after a real run delete data/checkpoints (or use a fresh demo lake) before starting the demo again. "
              "Repeat with --yes to run it." % (SMOKE_REL, SMOKE_DB, args.catalog if not args.skip_gold else "(none)"))
        return 2
    try:
        running = _running_demo_containers(runner)
    except RuntimeError as e:
        print("error: %s" % e, file=sys.stderr)
        return 3
    if running:
        print("error: stop the demo first (.\\scripts\\stop_demo.ps1); it would consume the smoke test messages. Running: %s"
              % ", ".join(running), file=sys.stderr)
        return 3
    ctx = ctx_factory(args, seed, start2) if ctx_factory else Ctx(args, seed, start2, runner=runner, utcnow=utcnow)
    results = run_stages(ctx, stages)
    text = render_report(results, collect_meta(ctx, raw))
    os.makedirs(os.path.dirname(os.path.abspath(args.report)), exist_ok=True)
    with open(args.report, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
    print("%s; report written to %s" % (verdict(results), args.report))
    return 0 if all(r.status != FAIL for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
