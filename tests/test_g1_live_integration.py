"""Integration: the live and health DDL and upserts against the real compose Postgres (docker compose up -d postgres).

Runs in a scratch database that is created and dropped here, so the real tables and Grafana are untouched.
Skipped when Postgres is not reachable. DSN: PG_DSN from the environment, else from .env."""
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

psycopg = pytest.importorskip("psycopg")

import scripts.apply_live_ddl as apply_live_ddl  # noqa: E402
import warehouse.heartbeat as heartbeat_mod  # noqa: E402
import warehouse.live_store as live_store  # noqa: E402
from warehouse.live_tables import HEALTH_COLUMNS, LIVE_METRIC_COLUMNS, LIVE_TABLE_NAMES  # noqa: E402
from warehouse.store import PostgresStore  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SCRATCH = {}  # name of the scratch role of this module's run


def base_dsn():
    dsn = os.environ.get("PG_DSN")
    if not dsn and (ROOT / ".env").exists():
        m = re.search(r"^PG_DSN=(.+)$", (ROOT / ".env").read_text(encoding="utf-8"), re.M)
        dsn = m.group(1).strip() if m else None
    return dsn


@pytest.fixture(scope="module")
def scratch_dsn():
    dsn = base_dsn()
    if not dsn:
        pytest.skip("no PG_DSN")
    suffix = uuid.uuid4().hex[:8]
    name = "fs_g1_" + suffix
    SCRATCH["role"] = "grafana_ro_it_" + suffix  # roles are cluster-wide: never touch the real grafana_ro
    try:
        admin = psycopg.connect(dsn, autocommit=True, connect_timeout=3)
        admin.execute("CREATE DATABASE %s" % name)
    except Exception as e:
        pytest.skip("Postgres not usable for the integration tests: %s" % type(e).__name__)
    try:
        yield re.sub(r"/[^/?]+(\?|$)", r"/%s\1" % name, dsn, count=1)
    finally:  # the database first (its grants go with it), then the scratch role
        admin.execute("DROP DATABASE IF EXISTS %s WITH (FORCE)" % name)
        admin.execute("DROP ROLE IF EXISTS %s" % SCRATCH["role"])
        admin.close()


def test_apply_twice_creates_tables_and_a_read_only_role(scratch_dsn):
    role = SCRATCH["role"]
    assert role != "grafana_ro"
    env = {"PG_DSN": scratch_dsn, "GRAFANA_DB_PASSWORD": "it-pw-1"}
    assert apply_live_ddl.main(["--role", role], env=env) == 0
    assert apply_live_ddl.main(["--role", role], env=dict(env, GRAFANA_DB_PASSWORD="it-pw-2")) == 0  # rotation works
    with psycopg.connect(scratch_dsn, autocommit=True) as c:
        c.execute("CREATE TABLE IF NOT EXISTS decoy (x int)")
        tables = {r[0] for r in c.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")}
        assert set(LIVE_TABLE_NAMES) <= tables
        for t in LIVE_TABLE_NAMES:
            assert c.execute("SELECT has_table_privilege(%s, %s, 'SELECT')", (role, t)).fetchone()[0]
            assert not c.execute("SELECT has_table_privilege(%s, %s, 'INSERT')", (role, t)).fetchone()[0]
        assert not c.execute("SELECT has_table_privilege(%s, 'decoy', 'SELECT')", (role,)).fetchone()[0]
    ro = re.sub(r"//[^@]*@", "//%s:it-pw-2@" % role, scratch_dsn, count=1)
    with psycopg.connect(ro) as c:
        assert c.execute("SELECT count(*) FROM live_metrics_minute").fetchone()[0] == 0
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            c.execute("INSERT INTO pipeline_heartbeat VALUES ('x', now(), NULL)")


def test_upserts_are_full_totals_not_increments(scratch_dsn):
    apply_live_ddl.main([], env={"PG_DSN": scratch_dsn})  # no password: tables only (exit 3), no role involved
    minute = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
    row = dict(minute_start=minute, country="EG", events=5, active_sessions=2, product_views=3, add_to_cart=1,
               orders_placed=1)
    with PostgresStore(scratch_dsn) as s:
        for _ in range(3):  # the same batch three times
            s.upsert("live_metrics_minute", [row], live_store.LIVE_KEYS)
        assert s.query("SELECT events, orders_placed FROM live_metrics_minute") == [(5, 1)]
        s.upsert("live_metrics_minute", [dict(row, events=9)], live_store.LIVE_KEYS)  # a recalculated total
        assert s.query("SELECT events, active_sessions FROM live_metrics_minute") == [(9, 2)]
        live_store.log_batch(s, "run", 1, minute)
        live_store.log_batch(s, "run", 1, minute)
        assert s.query("SELECT count(*) FROM pipeline_batch_log") == [(1,)]
        assert live_store.heartbeat(s, "silver_job", "x") is True


def test_heartbeat_helper_writes_and_a_dead_database_costs_under_five_seconds(scratch_dsn):
    import time

    apply_live_ddl.main([], env={"PG_DSN": scratch_dsn})
    assert heartbeat_mod.beat("publish", "it", env={"PG_DSN": scratch_dsn}) is True
    with PostgresStore(scratch_dsn) as s:
        assert s.query("SELECT component, details FROM pipeline_heartbeat WHERE component = 'publish'") == [
            ("publish", "it")]
    t = time.time()
    assert heartbeat_mod.beat("publish", env={"PG_DSN": "postgresql://u:p@10.255.255.1:5432/db"}) is False
    assert time.time() - t < 5
