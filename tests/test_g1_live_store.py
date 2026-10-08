"""Host tests for G1: live and health tables DDL, apply script, LiveSink, heartbeat helper, pipeline wiring.

No Spark and no Postgres: fakes only (the Spark and real-Postgres checks are in test_g1_live_spark.py)."""
import logging
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

import scripts.apply_live_ddl as apply_live_ddl
import warehouse.heartbeat as heartbeat_mod
import warehouse.live_store as live_store
from streaming.gold.live import minute_epochs
from warehouse.live_tables import HEALTH_COLUMNS, LIVE_METRIC_COLUMNS, LIVE_TABLE_NAMES, LIVE_TABLES
from warehouse.store import GoldStore, PostgresStore

ROOT = Path(__file__).resolve().parent.parent
DDL = (ROOT / "warehouse" / "live_ddl.sql").read_text(encoding="utf-8")
NOW = datetime(2026, 10, 8, 12, 30, 5, tzinfo=timezone.utc)


# ---------------------------------------------------------------- fakes
class MemStore(GoldStore):
    """Keeps upserted rows by primary key (what ON CONFLICT DO UPDATE ends up with) and records every call."""

    def __init__(self, fail=None):
        self.tables, self.calls, self.executed, self.closed = {}, [], [], False
        self.fail = fail  # exception raised by every upsert/query when set

    def upsert(self, table, rows, key_cols):
        if self.fail:
            raise self.fail
        self.calls.append((table, [dict(r) for r in rows], list(key_cols)))
        for r in rows:
            self.tables.setdefault(table, {}).setdefault(tuple(r[k] for k in key_cols), {}).update(r)
        return len(rows)

    def query(self, sql, params=None, retry=True):
        if self.fail:
            raise self.fail
        return []

    def execute(self, sql, retry=True):
        if self.fail:
            raise self.fail
        self.executed.append(sql)

    def close(self):
        self.closed = True


class RecCursor:
    def __init__(self, conn):
        self.conn = conn

    def executemany(self, sql, data):
        self.conn.many.append((sql, list(data)))

    def execute(self, sql, params=None):
        self.conn.statements.append((sql, params))

    def close(self):
        pass


class RecConn:
    def __init__(self):
        self.many, self.statements, self.commits, self.rollbacks = [], [], 0, 0

    def cursor(self):
        return RecCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        pass


# ---------------------------------------------------------------- 1. DDL and apply script
def test_ddl_has_the_four_contract_tables_and_keys():
    flat = re.sub(r"\s+", " ", re.sub(r"--[^\n]*", "", DDL))
    for name in LIVE_TABLE_NAMES:
        assert "CREATE TABLE IF NOT EXISTS %s (" % name in flat
    assert "PRIMARY KEY (minute_start, country)" in flat
    assert "minute_start timestamptz PRIMARY KEY" in flat
    assert "PRIMARY KEY (job_run_id, batch_id)" in flat
    assert "component text PRIMARY KEY" in flat
    for col in LIVE_METRIC_COLUMNS:
        assert re.search(r"\b%s\s+bigint\s+NOT NULL DEFAULT 0" % col, flat), col
    for col in HEALTH_COLUMNS:
        assert re.search(r"\b%s\s+bigint NOT NULL DEFAULT 0" % col, flat), col
    assert "finished_at timestamptz NOT NULL" in flat and "last_seen_at timestamptz NOT NULL" in flat
    assert "details text" in flat


def test_table_specs_match_ddl_columns():
    for t in LIVE_TABLES:
        body = re.search(r"CREATE TABLE IF NOT EXISTS %s \((.*?)\n\);" % t.name, DDL, re.S).group(1)
        names = [ln.split()[0] for ln in body.splitlines() if ln.strip() and not ln.strip().startswith(("PRIMARY", "--"))]
        assert names == t.column_names(), t.name


def test_ddl_grant_lists_only_the_four_tables_and_holds_no_password():
    grants = re.findall(r"GRANT\s+(.*?)\s+TO\s+grafana_ro", DDL, re.S)
    assert len(grants) == 1
    kind, _, tables = grants[0].partition(" ON ")
    assert kind == "SELECT"
    assert sorted(t.strip() for t in tables.split(",")) == sorted(LIVE_TABLE_NAMES)
    assert "CREATE ROLE grafana_ro LOGIN PASSWORD" in DDL
    assert re.search(r"IF NOT EXISTS \(SELECT 1 FROM pg_roles WHERE rolname = 'grafana_ro'\)", DDL)
    assert apply_live_ddl.PLACEHOLDER in DDL
    for pw in re.findall(r"PASSWORD\s+'([^']*)'", DDL):
        assert pw == apply_live_ddl.PLACEHOLDER
    assert "change_me" not in DDL


def test_render_fills_password_escapes_quotes_and_never_leaves_placeholder():
    sql = apply_live_ddl.render("pa'ss")
    assert "PASSWORD 'pa''ss'" in sql and apply_live_ddl.PLACEHOLDER not in sql


def test_render_without_password_drops_the_role_section():
    sql = apply_live_ddl.render("")
    assert "grafana_ro" not in sql and "CREATE TABLE IF NOT EXISTS pipeline_heartbeat" in sql


def test_render_rejects_dollar_quote_in_password():
    with pytest.raises(ValueError):
        apply_live_ddl.render("a$$b")


def test_apply_runs_script_once_prints_tables_never_the_password(capsys):
    store = MemStore()
    rc = apply_live_ddl.main([], env={"GRAFANA_DB_PASSWORD": "s3cret-pw"}, store=store)
    out = capsys.readouterr()
    assert rc == 0 and len(store.executed) == 1 and "s3cret-pw" in store.executed[0]
    assert "s3cret-pw" not in out.out + out.err
    for name in LIVE_TABLE_NAMES:
        assert "table %s ready" % name in out.out
    assert "role grafana_ro ready" in out.out
    assert store.closed is False  # a store handed in is the caller's to close


def test_apply_twice_sends_identical_sql():
    a, b = MemStore(), MemStore()
    apply_live_ddl.main([], env={"GRAFANA_DB_PASSWORD": "x"}, store=a)
    apply_live_ddl.main([], env={"GRAFANA_DB_PASSWORD": "x"}, store=a)
    assert a.executed[0] == a.executed[1]
    assert b.executed == []


def test_apply_without_grafana_password_creates_tables_then_exits_3_with_a_red_message(capsys):
    store = MemStore()
    assert apply_live_ddl.main([], env={}, store=store) == 3
    out = capsys.readouterr()
    assert "CREATE TABLE IF NOT EXISTS live_metrics_minute" in store.executed[0] and "grafana_ro" not in store.executed[0]
    assert "table pipeline_heartbeat ready" in out.out
    assert "\033[31m" in out.err and "NOT created" in out.err and "GRAFANA_DB_PASSWORD" in out.err


def test_role_name_is_a_parameter_and_defaults_to_grafana_ro():
    assert "CREATE ROLE grafana_ro LOGIN" in apply_live_ddl.render("pw")
    sql = apply_live_ddl.render("pw", role="grafana_ro_it_ab12")
    assert "grafana_ro_it_ab12" in sql and "TO grafana_ro_it_ab12" in sql
    assert not re.search(r"\bgrafana_ro\b", sql)
    with pytest.raises(ValueError):
        apply_live_ddl.render("pw", role="x; DROP ROLE y")
    store = MemStore()
    assert apply_live_ddl.main(["--role", "grafana_ro_it_ab12"], env={"GRAFANA_DB_PASSWORD": "pw"}, store=store) == 0
    assert "grafana_ro_it_ab12" in store.executed[0]


def test_apply_failure_returns_1_and_hides_the_password(capsys):
    store = MemStore(fail=RuntimeError("syntax error near s3cret-pw"))
    assert apply_live_ddl.main([], env={"GRAFANA_DB_PASSWORD": "s3cret-pw"}, store=store) == 1
    assert "s3cret-pw" not in capsys.readouterr().err


def test_apply_without_pg_dsn_is_a_config_error(capsys):
    assert apply_live_ddl.main([], env={}) == 2
    assert "PG_DSN" in capsys.readouterr().err


def test_env_example_lists_the_grafana_names_only():
    text = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert re.search(r"^GRAFANA_DB_PASSWORD=change_me_grafana$", text, re.M)
    assert re.search(r"^GRAFANA_ADMIN_PASSWORD=change_me_admin$", text, re.M)


# ---------------------------------------------------------------- 2. upsert statements (real PostgresStore, fake connection)
def test_live_table_upserts_set_every_measure_from_excluded_and_never_increment():
    conn = RecConn()
    store = PostgresStore("postgresql://x", connection=conn)
    minute = datetime(2026, 10, 8, 12, 30, tzinfo=timezone.utc)
    store.upsert("live_metrics_minute", [dict(minute_start=minute, country="EG", events=5, active_sessions=2,
                                              product_views=3, add_to_cart=1, orders_placed=1)],
                 live_store.LIVE_KEYS)
    store.upsert("pipeline_health_minute", [dict(minute_start=minute, events=5, late_events=1, quarantined=0,
                                                 silver_batches=2)], ["minute_start"])
    store.upsert("pipeline_batch_log", [dict(job_run_id="r", batch_id=3, finished_at=NOW)], ["job_run_id", "batch_id"])
    store.upsert("pipeline_heartbeat", [dict(component="silver_job", last_seen_at=NOW, details="d")], ["component"])
    sqls = [s for s, _ in conn.many]
    assert len(sqls) == 4 and conn.commits == 4
    wanted = {"live_metrics_minute": (["minute_start", "country"], LIVE_METRIC_COLUMNS),
              "pipeline_health_minute": (["minute_start"], HEALTH_COLUMNS),
              "pipeline_batch_log": (["job_run_id", "batch_id"], ["finished_at"]),
              "pipeline_heartbeat": (["component"], ["last_seen_at", "details"])}
    for sql, (table, (keys, measures)) in zip(sqls, wanted.items()):
        assert sql.startswith("INSERT INTO %s " % table)
        assert "ON CONFLICT (%s) DO UPDATE SET" % ", ".join(keys) in sql
        sets = sql.split("DO UPDATE SET")[1]
        for m in measures:
            assert "%s = EXCLUDED.%s" % (m, m) in sets
        assert sets.count("EXCLUDED") == len(measures)
        assert "+" not in sql and "DO NOTHING" not in sql


def test_no_increment_arithmetic_in_the_live_modules():
    for rel in ("warehouse/live_store.py", "warehouse/heartbeat.py", "warehouse/live_ddl.sql"):
        text = (ROOT / rel).read_text(encoding="utf-8")
        code = "\n".join(re.sub(r"(#|--).*", "", ln) for ln in text.splitlines())
        assert not re.search(r"=\s*\w+(\.\w+)?\s*\+\s*(EXCLUDED|1\b)", code), rel
        assert "EXCLUDED." not in code or "= EXCLUDED." in code


def test_upsert_failure_rolls_back_the_connection():
    class BadCursor(RecCursor):
        def executemany(self, sql, data):
            raise RuntimeError("boom")

    class BadConn(RecConn):
        def cursor(self):
            return BadCursor(self)

    conn = BadConn()
    with pytest.raises(RuntimeError):
        PostgresStore("x", connection=conn).upsert(
            "pipeline_heartbeat", [dict(component="c", last_seen_at=NOW)], ["component"])
    assert conn.rollbacks == 1 and conn.commits == 0


# ---------------------------------------------------------------- 3. log_batch, heartbeat, idempotence
def test_log_batch_and_heartbeat_rows():
    s = MemStore()
    live_store.log_batch(s, "run-1", 7, NOW)
    live_store.log_batch(s, "run-1", 7, NOW)  # the same batch twice
    assert s.tables["pipeline_batch_log"] == {("run-1", 7): dict(job_run_id="run-1", batch_id=7, finished_at=NOW)}
    assert live_store.heartbeat(s, "silver_job", "batch 7", NOW) is True
    assert s.tables["pipeline_heartbeat"][("silver_job",)]["details"] == "batch 7"


def test_live_store_heartbeat_swallows_store_errors(caplog):
    with caplog.at_level(logging.WARNING):
        assert live_store.heartbeat(MemStore(fail=RuntimeError("down")), "silver_job") is False
    assert "not written" in caplog.text


# ---------------------------------------------------------------- 4. LiveSink (refresh functions stubbed; Spark tests cover them)
@pytest.fixture
def stubbed(monkeypatch):
    calls = {"metrics": [], "health": []}

    def fake_metrics(spark, root, minutes, store, bot_lookback_hours=6):
        calls["metrics"].append(sorted(minutes))
        return store.upsert("live_metrics_minute", [dict(minute_start=NOW, country="EG", events=len(minutes))],
                            live_store.LIVE_KEYS)

    def fake_health(spark, root, minutes, store):
        calls["health"].append(sorted(minutes))
        return store.upsert("pipeline_health_minute", [dict(minute_start=NOW, events=1, late_events=0, quarantined=0,
                                                            silver_batches=1)], ["minute_start"])

    monkeypatch.setattr(live_store, "refresh_live_metrics", fake_metrics)
    monkeypatch.setattr(live_store, "refresh_health", fake_health)
    return calls


def test_sink_batch_writes_metrics_log_health_heartbeat_in_order(stubbed, caplog):
    s = MemStore()
    sink = live_store.LiveSink("lake", store=s, job_run_id="run-1")
    with caplog.at_level(logging.INFO, logger="funnelstream.live_store"):
        out = sink.on_batch(None, 4, [1_000_000_020, 1_000_000_030], [1_000_000_100], [], now=NOW)
    assert out == (1, 2)  # two event times in one minute; arrival minute plus the finish minute
    assert [c[0] for c in s.calls] == ["live_metrics_minute", "pipeline_batch_log", "pipeline_health_minute",
                                       "pipeline_heartbeat"]
    assert "live_store: 1 metric minutes, 2 health minutes" in caplog.text
    assert s.tables["pipeline_heartbeat"][("silver_job",)]["details"] == "batch 4"
    assert int(NOW.timestamp()) // 60 * 60 in stubbed["health"][0]  # the batch's finish minute is a health minute


def test_sink_same_batch_twice_gives_identical_rows(stubbed):
    s = MemStore()
    sink = live_store.LiveSink("lake", store=s, job_run_id="run-1")
    sink.on_batch(None, 4, [1_000_000_020], [1_000_000_100], [], now=NOW)
    first = {t: dict(v) for t, v in s.tables.items()}
    sink.on_batch(None, 4, [1_000_000_020], [1_000_000_100], [], now=NOW)
    assert s.tables == first


def test_sink_outage_never_raises_logs_and_keeps_minutes_pending(stubbed, caplog):
    now = [100.0]
    s = MemStore(fail=ConnectionError("db down"))
    sink = live_store.LiveSink("lake", store=s, clock=lambda: now[0], retry_after=30)
    with caplog.at_level(logging.WARNING, logger="funnelstream.live_store"):
        assert sink.on_batch(None, 1, [1_000_000_020], [], [], now=NOW) is None
        assert "not written to Postgres" in caplog.text and "ConnectionError" in caplog.text
        caplog.clear()
        now[0] = 110.0  # inside the back-off: no new attempt, minutes keep waiting
        assert sink.on_batch(None, 2, [1_000_000_200], [], [], now=NOW) is None
        assert "wait for the next batch" in caplog.text
    assert stubbed["metrics"] == [minute_epochs([1_000_000_020])]  # only the first attempt got as far as the refresh
    s.fail = None
    now[0] = 200.0  # back-off over, database back: all three batches' minutes are repaired together
    assert sink.on_batch(None, 3, [1_000_000_400], [], [], now=NOW) == (3, 1)
    assert stubbed["metrics"][-1] == minute_epochs([1_000_000_020, 1_000_000_200, 1_000_000_400])


def test_sink_builds_its_own_postgres_store_with_the_short_timeout(monkeypatch, stubbed):
    made = []

    class Fake(MemStore):
        @classmethod
        def bounded(cls, dsn):
            made.append(dsn)
            return cls()

    monkeypatch.setattr("warehouse.store.PostgresStore", Fake)
    sink = live_store.LiveSink("lake", dsn="postgresql://u:p@postgres:5432/db")
    assert sink.on_batch(None, 1, [60], [], [], now=NOW) == (1, 1)
    assert made == ["postgresql://u:p@postgres:5432/db"]
    assert live_store.CONNECT_TIMEOUT <= 3


def test_importing_live_store_does_not_import_psycopg():
    code = "import sys, warehouse.live_store, streaming.jobs.silver_clean as s; " \
           "print('psycopg' in sys.modules, s.live_store_enabled(False, {}))"
    out = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr[-500:]
    assert out.stdout.split() == ["False", "False"]


def test_silver_job_stays_unchanged_without_pg_dsn_or_flag():
    from streaming.jobs.silver_clean import live_store_enabled

    assert live_store_enabled(False, {}) is False
    assert live_store_enabled(False, {"PG_DSN": ""}) is False
    assert live_store_enabled(True, {}) is True
    assert live_store_enabled(False, {"PG_DSN": "postgresql://x"}) is True


# ---------------------------------------------------------------- 5. heartbeat helper
def test_beat_without_pg_dsn_does_nothing():
    assert heartbeat_mod.beat("publish", env={}) is False


def test_beat_swallows_connection_errors(caplog):
    def boom():
        raise OSError("connection refused")

    with caplog.at_level(logging.WARNING, logger="funnelstream.heartbeat"):
        assert heartbeat_mod.beat("publish", store_factory=boom) is False
    assert "not written" in caplog.text


def test_beat_writes_one_row_and_closes_the_store():
    s = MemStore()
    assert heartbeat_mod.beat("gold_build", "steps: a", store_factory=lambda: s) is True
    row = s.tables["pipeline_heartbeat"][("gold_build",)]
    assert row["details"] == "steps: a" and row["last_seen_at"].tzinfo is not None and s.closed


def test_bounded_store_passes_connect_statement_and_tcp_timeouts_and_plain_store_does_not(monkeypatch):
    import psycopg

    seen = []

    def fake_connect(dsn, **kw):
        seen.append(kw)
        return RecConn()

    monkeypatch.setattr(psycopg, "connect", fake_connect)
    PostgresStore.bounded("postgresql://x")._connection()
    PostgresStore("postgresql://x")._connection()
    bounded, plain = seen
    assert bounded["connect_timeout"] == 3 and bounded["options"] == "-c statement_timeout=5000"
    assert bounded["tcp_user_timeout"] == 5000 and bounded["keepalives"] == 1 and bounded["keepalives_idle"] <= 5
    assert plain == {}  # the Gold path is unchanged


@pytest.fixture
def blackhole():
    """A TCP server that accepts connections and never answers (a Postgres that stopped responding)."""
    import socket
    import threading

    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    held, stop = [], threading.Event()

    def accept():
        srv.settimeout(0.2)
        while not stop.is_set():
            try:
                held.append(srv.accept()[0])
            except OSError:
                pass

    threading.Thread(target=accept, daemon=True).start()
    yield "postgresql://u:p@127.0.0.1:%d/db" % srv.getsockname()[1]
    stop.set()
    for c in held:
        c.close()
    srv.close()


def test_a_database_that_never_answers_cannot_stall_the_heartbeat_or_the_batch(blackhole, stubbed):
    import time

    t = time.time()
    assert heartbeat_mod.beat("silver_job", env={"PG_DSN": blackhole}) is False
    assert time.time() - t < 8
    sink = live_store.LiveSink("lake", dsn=blackhole)
    t = time.time()
    assert sink.on_batch(None, 1, [60], [], [], now=NOW) is None  # logged, the batch itself is not affected
    assert time.time() - t < 8  # one 3 s connect timeout, then the 30 s back-off


def test_liveness_thread_writes_alive_while_the_query_is_active_and_stops_after(stubbed):
    import time

    class Query:
        isActive = True

    s, q = MemStore(), Query()
    sink = live_store.LiveSink("lake", store=s)
    thread = sink.start_liveness(q, interval=0.05)
    deadline = time.time() + 5
    while ("silver_job",) not in s.tables.get("pipeline_heartbeat", {}) and time.time() < deadline:
        time.sleep(0.02)
    assert s.tables["pipeline_heartbeat"][("silver_job",)]["details"] == "alive"
    q.isActive = False
    thread.join(5)
    assert not thread.is_alive() and thread.daemon
    assert live_store.LIVENESS_INTERVAL == 15.0


def test_liveness_survives_a_failing_store():
    import time

    class Query:
        isActive = True

    sink = live_store.LiveSink("lake", store=MemStore(fail=ConnectionError("down")))
    q = Query()
    thread = sink.start_liveness(q, interval=0.05)
    time.sleep(0.3)
    assert thread.is_alive()
    q.isActive = False
    thread.join(5)


def test_silver_run_starts_liveness_only_for_a_continuous_query():
    text = (ROOT / "streaming" / "jobs" / "silver_clean.py").read_text(encoding="utf-8")
    assert "sink.start_liveness(q)" in text and "not once" in text


def test_beat_uses_a_three_second_connect_timeout(monkeypatch):
    import psycopg

    seen = {}

    def fake_connect(dsn, **kw):
        seen.update(dsn=dsn, **kw)
        raise psycopg.OperationalError("timeout expired")

    monkeypatch.setattr(psycopg, "connect", fake_connect)
    assert heartbeat_mod.beat("publish", env={"PG_DSN": "postgresql://u:p@localhost:1/db"}) is False
    assert seen["connect_timeout"] == 3 == heartbeat_mod.CONNECT_TIMEOUT


def test_publish_and_gold_build_beat_after_success_not_on_dry_run(monkeypatch):
    import scripts.publish_silver as publish_silver
    import warehouse.gold_build as gold_build

    beats = []
    monkeypatch.setattr(heartbeat_mod, "beat", lambda component, details=None, **kw: beats.append(component))
    monkeypatch.setattr(gold_build, "get_store", lambda backend=None: MemStore())
    monkeypatch.setattr(gold_build, "run_build", lambda store=None, **kw: {
        "window": {"from": "2026-10-01", "to": "2026-10-02", "full": False}, "dry_run": kw.get("dry_run"), "steps": {"dim_date": {"status": "ok", "statements": 1,
                                                                            "rows": 1}}, "reported": {}})
    assert gold_build.main(["--dry-run"]) == 0 and beats == []
    assert gold_build.main(["--days", "1"]) == 0 and beats == ["gold_build"]

    monkeypatch.setattr(publish_silver, "publish", lambda *a, **kw: {"silver_events": {"rows": 1, "files": 1,
                                                                                         "merged": True}})
    monkeypatch.setattr(publish_silver, "_get_spark", lambda: type("S", (), {"stop": lambda self: None})())
    monkeypatch.setattr(publish_silver, "get_store", lambda backend=None: MemStore())
    monkeypatch.setattr(publish_silver, "make_uploader", lambda *a: None)
    monkeypatch.setenv("DATABRICKS_HOST", "h")
    monkeypatch.setenv("DATABRICKS_TOKEN", "t")
    beats.clear()
    assert publish_silver.main(["--dry-run"]) == 0 and beats == []
    assert publish_silver.main(["--export-only"]) == 0 and beats == []
    assert publish_silver.main([]) == 0 and beats == ["publish"]


# ---------------------------------------------------------------- 6. pipeline, compose, demo scripts
def test_pipeline_argument_parsing(monkeypatch):
    from streaming.jobs.pipeline import parse_args

    monkeypatch.delenv("KAFKA_BOOTSTRAP", raising=False)
    a = parse_args([])
    assert (a.lake, a.checkpoints, a.show, a.live_store, a.bootstrap) == ("data/lake", "data/checkpoints", 0, False,
                                                                        "kafka:19092")
    a = parse_args(["--lake", "L", "--checkpoints", "C", "--show", "--live-store"])
    assert (a.lake, a.checkpoints, a.show, a.live_store) == ("L", "C", 3, True)
    assert parse_args(["--show", "5"]).show == 5


def test_pipeline_starts_the_same_entry_points_as_the_two_jobs():
    text = (ROOT / "streaming" / "jobs" / "pipeline.py").read_text(encoding="utf-8")
    for needle in ("bronze_ingest.run(", "silver_clean.run(", "show_loop", "await_all(bronze_qs + [silver_q]"):
        assert needle in text
    assert text.count("get_spark(") == 1  # one SparkSession for the four queries


def test_compose_spark_has_pg_dsn_to_postgres_host_and_no_depends_on():
    svc = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))["services"]
    env = svc["spark"]["environment"]
    assert env["PG_DSN"] == "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}"
    assert "depends_on" not in svc["spark"]
    assert svc["spark"]["mem_limit"] == "4g" and svc["postgres"]["mem_limit"] == "512m"


def test_compose_grafana_waits_for_a_healthy_postgres():
    svc = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))["services"]
    assert svc["grafana"]["depends_on"] == {"postgres": {"condition": "service_healthy"}}


def test_compose_postgres_has_pg_isready_healthcheck_and_named_volume():
    doc = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    pg = doc["services"]["postgres"]
    assert "pg_isready" in " ".join(pg["healthcheck"]["test"])
    assert "pgdata:/var/lib/postgresql/data" in pg["volumes"] and "pgdata" in doc["volumes"]


def test_spark_image_installs_psycopg():
    text = (ROOT / "infrastructure" / "docker" / "spark" / "Dockerfile").read_text(encoding="utf-8")
    assert "psycopg[binary]" in text


def test_watch_demo_runs_one_pipeline_window_and_needs_postgres():
    text = (ROOT / "scripts" / "watch_demo.ps1").read_text(encoding="utf-8")
    assert "FS 2 Pipeline" in text and "--name fs-pipeline" in text and "streaming.jobs.pipeline" in text
    assert "FS 3 Silver" not in text and "FS 2 Bronze" not in text and "streaming.jobs.bronze_ingest" not in text
    assert "compose stop postgres" not in text
    assert "compose up -d postgres" in text and "pg_isready" in text and "scripts/apply_live_ddl.py" in text
    assert "FS 1 Kafka listener" in text and "Generator" in text
    assert "http://localhost:3000" in text
    assert "$LASTEXITCODE -eq 3" in text and "GRAFANA_DB_PASSWORD" in text
    assert text.index("apply_live_ddl.py") < text.index("Open-Window 'FS 1 Kafka listener'")
    stop = (ROOT / "scripts" / "stop_demo.ps1").read_text(encoding="utf-8")
    assert "fs-pipeline" in stop
