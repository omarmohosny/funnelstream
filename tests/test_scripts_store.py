"""Host tests for apply_ddl, publish_silver and check_databricks (fakes, no network, env controlled)."""
from pathlib import Path

import pytest

import scripts.apply_ddl as apply_ddl
import scripts.check_databricks as check_databricks
import scripts.publish_silver as publish_silver
from warehouse.ddl_render import render_databricks, split_statements
from warehouse.store import GoldStore, env_catalog, get_store
from warehouse.tables import all_tables
from warehouse.unknown_members import seed_statements

ROOT = Path(__file__).resolve().parent.parent
TOKEN = "dapi-SECRET-TOKEN-0123456789"
HOST = "adb-secret-host.example.invalid"
ENV_NAMES = ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID", "DATABRICKS_HTTP_PATH",
             "DATABRICKS_CATALOG", "DATABRICKS_VOLUME", "GOLD_BACKEND", "PG_DSN"]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for n in ENV_NAMES:
        monkeypatch.delenv(n, raising=False)


def set_dbx_env(monkeypatch, **extra):
    monkeypatch.setenv("DATABRICKS_HOST", HOST)
    monkeypatch.setenv("DATABRICKS_TOKEN", TOKEN)
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh1")
    for k, v in extra.items():
        monkeypatch.setenv(k, v)


class FakeStore(GoldStore):
    def __init__(self, results=None, fail=None):
        self.executed, self.queries, self.closed = [], [], False
        self.results = results or {}
        self.fail = fail or {}

    def upsert(self, table, rows, key_cols):
        return len(rows)

    def query(self, sql, params=None, retry=True):
        self.queries.append(sql)
        for key, exc in self.fail.items():
            if key in sql:
                raise exc
        for key, rows in self.results.items():
            if key in sql:
                return rows
        return [(1,)]

    def execute(self, sql, retry=True):
        self.executed.append(sql)

    def close(self):
        self.closed = True


# 12. apply_ddl
def test_apply_ddl_databricks_runs_every_statement_through_store(monkeypatch):
    fake = FakeStore()
    monkeypatch.setattr(apply_ddl, "get_store", lambda backend, env=None: fake)
    rc = apply_ddl.main(["--backend", "databricks"])
    expected = split_statements(render_databricks(all_tables(), catalog="funnelstream", volume="silver_upload"))
    expected += seed_statements("databricks", "funnelstream")  # C3: unknown members run after the CREATEs
    assert rc == 0 and fake.executed == expected and fake.closed
    assert len(expected) == 27  # 23 from the committed file + CREATE VOLUME + 3 seed MERGEs
    assert len(split_statements((ROOT / "warehouse" / "databricks_ddl.sql").read_text(encoding="utf-8"))) == 23


def test_apply_ddl_order_is_schemas_volume_tables(monkeypatch):
    fake = FakeStore()
    monkeypatch.setattr(apply_ddl, "get_store", lambda backend, env=None: fake)
    monkeypatch.setenv("DATABRICKS_VOLUME", "myvol")
    apply_ddl.main(["--backend", "databricks", "--catalog", "workspace"])
    ex = fake.executed
    assert ex[0] == "CREATE SCHEMA IF NOT EXISTS workspace.gold;"
    assert ex[1] == "CREATE SCHEMA IF NOT EXISTS workspace.silver;"
    assert ex[2] == "CREATE VOLUME IF NOT EXISTS workspace.silver.myvol;"
    assert all(s.startswith("CREATE TABLE IF NOT EXISTS workspace.") for s in ex[3:-3])
    assert all(s.startswith("MERGE INTO workspace.gold.dim_") for s in ex[-3:])  # seeds last (C3)
    assert not any("funnelstream." in s for s in ex)


def test_apply_ddl_catalog_from_env_and_flag_overrides(monkeypatch, capsys):
    monkeypatch.setenv("DATABRICKS_CATALOG", "envcat")
    assert apply_ddl.main(["--backend", "databricks", "--dry-run"]) == 0
    assert "catalog envcat" in capsys.readouterr().out
    assert apply_ddl.main(["--backend", "databricks", "--dry-run", "--catalog", "flagcat"]) == 0
    assert "catalog flagcat" in capsys.readouterr().out
    assert apply_ddl.main(["--backend", "databricks", "--dry-run", "--catalog", "bad cat;"]) == 2
    monkeypatch.setenv("DATABRICKS_VOLUME", "bad vol")
    assert apply_ddl.main(["--backend", "databricks", "--dry-run"]) == 2


def test_apply_ddl_postgres_runs_whole_file_once(monkeypatch):
    fake = FakeStore()
    monkeypatch.setattr(apply_ddl, "get_store", lambda backend, env=None: fake)
    assert apply_ddl.main(["--backend", "postgres"]) == 0
    assert fake.executed == [(ROOT / "warehouse" / "ddl.sql").read_text(encoding="utf-8")]         + seed_statements("postgres", None)


def test_apply_ddl_dry_run_does_not_connect(monkeypatch, capsys):
    def boom(*a, **k):
        raise AssertionError("must not connect")

    monkeypatch.setattr(apply_ddl, "get_store", boom)
    assert apply_ddl.main(["--backend", "databricks", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "databricks: 27 statements" in out and "CREATE VOLUME IF NOT EXISTS" in out
    assert apply_ddl.main(["--backend", "postgres", "--dry-run"]) == 0
    assert "postgres: 1 script" in capsys.readouterr().out


def test_apply_ddl_missing_env_exit_2_no_traceback(capsys):
    assert apply_ddl.main(["--backend", "databricks"]) == 2
    err = capsys.readouterr().err
    assert "DATABRICKS_HOST" in err and "Traceback" not in err


# catalog-aware rendering
def test_render_with_custom_catalog_and_default_matches_committed():
    text = render_databricks(all_tables(), catalog="workspace")
    assert "workspace.gold." in text and "workspace.silver." in text and "funnelstream." not in text
    committed = (ROOT / "warehouse" / "databricks_ddl.sql").read_text(encoding="utf-8")
    assert render_databricks(all_tables()) == committed
    assert render_databricks(all_tables(), catalog="funnelstream") == committed
    with pytest.raises(ValueError):
        render_databricks(all_tables(), catalog="a b")
    with pytest.raises(ValueError):
        render_databricks(all_tables(), volume="a;b")


# one variable drives everything
def test_all_four_read_the_same_catalog_variable(monkeypatch, capsys, tmp_path):
    set_dbx_env(monkeypatch, DATABRICKS_CATALOG="workspace")
    assert env_catalog() == "workspace"
    assert get_store("databricks").catalog == "workspace"
    # apply_ddl
    assert apply_ddl.main(["--backend", "databricks", "--dry-run"]) == 0
    assert "catalog workspace" in capsys.readouterr().out
    # check_databricks
    fake = FakeStore(results={"SHOW SCHEMAS": [("gold",), ("silver",)]})
    monkeypatch.setattr(check_databricks, "get_store", lambda b: fake)
    assert check_databricks.main([]) == 0
    assert "SHOW SCHEMAS IN workspace" in fake.queries
    # publish_silver (upload-only dry run needs no Spark)
    d = tmp_path / "silver_events"
    d.mkdir()
    (d / "p.parquet").write_bytes(b"x")
    assert publish_silver.main(["--upload-only", "--dry-run", "--export-dir", str(tmp_path),
                                "--tables", "silver_events"]) == 0
    assert "workspace.silver.silver_events" in capsys.readouterr().out


# 13. check_databricks
def test_check_success_prints_ok_and_never_token(monkeypatch, capsys):
    set_dbx_env(monkeypatch)
    fake = FakeStore(results={"SHOW SCHEMAS": [("default",), ("gold",), ("silver",)]})
    monkeypatch.setattr(check_databricks, "get_store", lambda b: fake)
    assert check_databricks.main([]) == 0
    out = capsys.readouterr().out
    assert "OK connected" in out and "gold and silver exist" in out and fake.queries[0] == "SELECT 1"
    assert TOKEN not in out and HOST not in out and fake.closed


def test_check_missing_schemas_points_to_apply_ddl(monkeypatch, capsys):
    set_dbx_env(monkeypatch)
    monkeypatch.setattr(check_databricks, "get_store", lambda b: FakeStore(results={"SHOW SCHEMAS": [("default",)]}))
    assert check_databricks.main([]) == 0
    out = capsys.readouterr().out
    assert "gold, silver" in out and "apply_ddl.py" in out


def test_check_missing_env_lists_names_exit_1(capsys):
    assert check_databricks.main([]) == 1
    out = capsys.readouterr().out
    assert "DATABRICKS_HOST" in out and "DATABRICKS_TOKEN" in out and "DATABRICKS_WAREHOUSE_ID" in out
    assert len(out.strip().splitlines()) == 1


def test_check_unreachable_one_line_exit_1_no_token(monkeypatch, capsys):
    set_dbx_env(monkeypatch)
    fake = FakeStore(fail={"SELECT 1": OSError("connection refused for %s\nsecond line" % TOKEN)})
    monkeypatch.setattr(check_databricks, "get_store", lambda b: fake)
    assert check_databricks.main([]) == 1
    out = capsys.readouterr().out
    assert len(out.strip().splitlines()) == 1 and out.startswith("FAILED") and TOKEN not in out


def test_check_catalog_not_found_message(monkeypatch, capsys):
    set_dbx_env(monkeypatch, DATABRICKS_CATALOG="nocat")
    fake = FakeStore(fail={"SHOW SCHEMAS": RuntimeError("[CATALOG_NOT_FOUND] Catalog 'nocat' was not found.")})
    monkeypatch.setattr(check_databricks, "get_store", lambda b: fake)
    assert check_databricks.main([]) == 1
    out = capsys.readouterr().out.strip().splitlines()
    assert out[0] == "OK connected"
    assert out[1] == ("Catalog nocat not found. In the Databricks UI create it (Catalog Explorer, Create catalog) "
                      "or set DATABRICKS_CATALOG to an existing catalog such as workspace.")
    assert len(out) == 2


def test_check_upload_test_uses_volume_path(monkeypatch, capsys):
    set_dbx_env(monkeypatch, DATABRICKS_CATALOG="workspace", DATABRICKS_VOLUME="vol1")
    uploads = []

    class Up:
        def upload(self, path, data):
            uploads.append((path, data))

    monkeypatch.setattr(check_databricks, "get_store",
                        lambda b: FakeStore(results={"SHOW SCHEMAS": [("gold",), ("silver",)]}))
    monkeypatch.setattr(check_databricks, "make_uploader", lambda h, t: Up())
    assert check_databricks.main(["--upload-test"]) == 0
    assert uploads[0][0] == "/Volumes/workspace/silver/vol1/_connectivity_test/ping.txt"
    assert "OK upload" in capsys.readouterr().out

    class BadUp:
        def upload(self, path, data):
            raise RuntimeError("denied for %s" % TOKEN)

    monkeypatch.setattr(check_databricks, "make_uploader", lambda h, t: BadUp())
    assert check_databricks.main(["--upload-test"]) == 1
    out = capsys.readouterr().out
    assert "FAILED: upload test" in out and TOKEN not in out


# publish_silver
def test_publish_silver_missing_env_exit_2_no_traceback(capsys):
    assert publish_silver.main(["--upload-only"]) == 2
    err = capsys.readouterr().err
    assert "DATABRICKS_HOST" in err and "Traceback" not in err


def test_publish_silver_upload_only_wires_store_and_uploader(monkeypatch, tmp_path, capsys):
    set_dbx_env(monkeypatch)
    fake = FakeStore()
    ups = []

    class Up:
        def upload(self, path, data):
            ups.append(path)

    monkeypatch.setattr(publish_silver, "get_store", lambda b: fake)
    monkeypatch.setattr(publish_silver, "make_uploader", lambda h, t: Up())
    monkeypatch.setattr(publish_silver, "_get_spark", lambda: (_ for _ in ()).throw(AssertionError("no spark")))
    d = tmp_path / "silver_events"
    d.mkdir()
    (d / "p.parquet").write_bytes(b"x")
    assert publish_silver.main(["--upload-only", "--export-dir", str(tmp_path), "--tables", "silver_events"]) == 0
    assert fake.executed[0] == "CREATE VOLUME IF NOT EXISTS funnelstream.silver.silver_upload"
    assert fake.executed[1].startswith("MERGE INTO funnelstream.silver.silver_events")
    assert ups == ["/Volumes/funnelstream/silver/silver_upload/silver_events/p.parquet"]
    assert "silver_events" in capsys.readouterr().out
    assert publish_silver.main(["--tables", "nope"]) == 2
    assert publish_silver.main(["--export-only", "--upload-only"]) == 2


# C2: apply_ddl --recreate
def test_apply_ddl_recreate_without_yes_drops_nothing(monkeypatch, capsys):
    fake = FakeStore()
    monkeypatch.setattr(apply_ddl, "get_store", lambda backend, env=None: fake)
    rc = apply_ddl.main(["--backend", "databricks", "--catalog", "workspace",
                         "--recreate", "silver_events,silver_orders"])
    out = capsys.readouterr().out
    assert rc == 2 and fake.executed == [] and not fake.closed
    assert "WARNING" in out
    assert "DROP TABLE IF EXISTS workspace.silver.silver_events;" in out
    assert "DROP TABLE IF EXISTS workspace.silver.silver_orders;" in out


def test_apply_ddl_recreate_with_yes_drops_before_matching_create(monkeypatch):
    fake = FakeStore()
    monkeypatch.setattr(apply_ddl, "get_store", lambda backend, env=None: fake)
    rc = apply_ddl.main(["--backend", "databricks", "--catalog", "workspace", "--yes",
                         "--recreate", "silver_events,silver_campaign_snapshots"])
    ex = fake.executed
    assert rc == 0
    for t in ("silver_events", "silver_campaign_snapshots"):
        drop = "DROP TABLE IF EXISTS workspace.silver.%s;" % t
        assert ex.count(drop) == 1
        assert ex[ex.index(drop) + 1].startswith("CREATE TABLE IF NOT EXISTS workspace.silver.%s (" % t)
    assert sum(s.startswith("DROP") for s in ex) == 2
    plain = split_statements(render_databricks(all_tables(), catalog="workspace", volume="silver_upload"))
    assert [s for s in ex if not s.startswith("DROP")] == plain + seed_statements("databricks", "workspace")


def test_apply_ddl_recreate_unknown_table_rejected(monkeypatch, capsys):
    fake = FakeStore()
    monkeypatch.setattr(apply_ddl, "get_store", lambda backend, env=None: fake)
    rc = apply_ddl.main(["--backend", "databricks", "--yes", "--recreate", "silver_events,not_a_table"])
    assert rc == 2 and fake.executed == []
    assert "unknown table" in capsys.readouterr().err


def test_apply_ddl_recreate_rejected_on_postgres(monkeypatch, capsys):
    fake = FakeStore()
    monkeypatch.setattr(apply_ddl, "get_store", lambda backend, env=None: fake)
    rc = apply_ddl.main(["--backend", "postgres", "--yes", "--recreate", "silver_events"])
    assert rc == 2 and fake.executed == []
    assert "databricks backend only" in capsys.readouterr().err


def test_apply_ddl_recreate_dry_run_exits_zero_and_executes_nothing(monkeypatch, capsys):
    fake = FakeStore()
    monkeypatch.setattr(apply_ddl, "get_store", lambda backend, env=None: fake)
    rc = apply_ddl.main(["--backend", "databricks", "--dry-run", "--yes", "--recreate", "silver_orders"])
    assert rc == 0 and fake.executed == []
    assert "DROP TABLE IF EXISTS funnelstream.silver.silver_orders;" in capsys.readouterr().out
