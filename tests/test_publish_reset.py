"""Host tests for publish_silver --reset (fake store and uploader, no Spark, no network)."""
import pytest

import scripts.publish_silver as publish_silver
from warehouse.publish import (PUBLISH_ALLOWLIST, RESET_EXCLUDED, publish, reset_statement, resettable_tables)

ENV = ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID", "DATABRICKS_HTTP_PATH",
       "DATABRICKS_CATALOG", "DATABRICKS_VOLUME", "GOLD_BACKEND", "PG_DSN"]
OWNED = [t for t in PUBLISH_ALLOWLIST if t != "silver_campaign_snapshots"]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for n in ENV:
        monkeypatch.delenv(n, raising=False)
    monkeypatch.setenv("DATABRICKS_HOST", "adb-x.example.invalid")
    monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh1")


class Log:
    """One shared event log so the order of truncate, upload and merge can be asserted."""

    def __init__(self):
        self.events = []


class FakeStore:
    def __init__(self, log):
        self.log, self.closed = log, False

    def execute(self, sql, retry=True):
        kind = "TRUNCATE" if sql.startswith("TRUNCATE") else "MERGE" if sql.startswith("MERGE") else "OTHER"
        self.log.events.append((kind, sql))

    def close(self):
        self.closed = True


class FakeUploader:
    def __init__(self, log):
        self.log = log

    def upload(self, path, data):
        self.log.events.append(("UPLOAD", path))

    def clear(self, remote_dir):
        self.log.events.append(("CLEAR", remote_dir))


def export_dir(tmp_path, tables):
    for t in tables:
        d = tmp_path / t
        d.mkdir(parents=True)
        (d / "part-0.parquet").write_bytes(b"x")
    return str(tmp_path)


def wire(monkeypatch, log, no_store=False):
    def get_store(backend):
        if no_store:
            raise AssertionError("no store may be created")
        return FakeStore(log)
    monkeypatch.setattr(publish_silver, "get_store", get_store)
    monkeypatch.setattr(publish_silver, "make_uploader", lambda h, t: FakeUploader(log))
    monkeypatch.setattr(publish_silver, "_get_spark", lambda: (_ for _ in ()).throw(AssertionError("no spark")))


def test_reset_owns_every_mirror_table_except_the_ad_snapshots():
    assert RESET_EXCLUDED == ("silver_campaign_snapshots",)
    assert resettable_tables() == OWNED and len(OWNED) == 7
    assert resettable_tables(["silver_events", "silver_campaign_snapshots"]) == ["silver_events"]
    assert reset_statement("silver_events", "cat") == "TRUNCATE TABLE cat.silver.silver_events"
    for bad in ("silver_campaign_snapshots", "dim_customer", "nope"):
        with pytest.raises(ValueError):
            reset_statement(bad, "cat")
    with pytest.raises(ValueError):
        reset_statement("silver_events", "bad cat")


def test_without_yes_it_prints_the_plan_and_exits_2_touching_nothing(monkeypatch, capsys, tmp_path):
    log = Log()
    wire(monkeypatch, log, no_store=True)
    rc = publish_silver.main(["--reset", "--upload-only", "--export-dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert rc == 2 and log.events == []
    assert "WARNING" in out and "TRUNCATES" in out
    for t in OWNED:
        assert "  plan: TRUNCATE TABLE funnelstream.silver.%s" % t in out
    assert "silver_campaign_snapshots" in out and "plan: TRUNCATE TABLE funnelstream.silver.silver_campaign_snapshots" \
        not in out
    assert "DROP" not in out.replace("never drop", "")
    assert "Repeat with --yes" in out


def test_plan_follows_the_catalog_and_the_tables_filter(monkeypatch, capsys, tmp_path):
    wire(monkeypatch, Log(), no_store=True)
    monkeypatch.setenv("DATABRICKS_CATALOG", "workspace")
    rc = publish_silver.main(["--reset", "--tables", "silver_events,silver_campaign_snapshots", "--upload-only",
                              "--export-dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert rc == 2
    assert "plan: TRUNCATE TABLE workspace.silver.silver_events" in out
    assert "plan: TRUNCATE TABLE workspace.silver.silver_orders" not in out
    assert "silver_campaign_snapshots" not in out.split("WARNING: silver_campaign_snapshots")[0]


def test_with_yes_each_table_is_truncated_before_its_upload_and_merge(monkeypatch, tmp_path):
    log = Log()
    wire(monkeypatch, log)
    root = export_dir(tmp_path, ["silver_events", "silver_orders", "silver_campaign_snapshots"])
    rc = publish_silver.main(["--reset", "--yes", "--upload-only", "--export-dir", root,
                              "--tables", "silver_events,silver_orders,silver_campaign_snapshots"])
    assert rc == 0
    kinds = [(k, v.split()[-1] if k == "TRUNCATE" else v.split(" AS ")[0].split()[-1] if k == "MERGE" else v)
             for k, v in log.events if k != "OTHER"]
    assert kinds == [
        ("TRUNCATE", "funnelstream.silver.silver_events"),
        ("CLEAR", "/Volumes/funnelstream/silver/silver_upload/silver_events"),
        ("UPLOAD", "/Volumes/funnelstream/silver/silver_upload/silver_events/part-0.parquet"),
        ("MERGE", "funnelstream.silver.silver_events"),
        ("TRUNCATE", "funnelstream.silver.silver_orders"),
        ("CLEAR", "/Volumes/funnelstream/silver/silver_upload/silver_orders"),
        ("UPLOAD", "/Volumes/funnelstream/silver/silver_upload/silver_orders/part-0.parquet"),
        ("MERGE", "funnelstream.silver.silver_orders"),
        ("CLEAR", "/Volumes/funnelstream/silver/silver_upload/silver_campaign_snapshots"),
        ("UPLOAD", "/Volumes/funnelstream/silver/silver_upload/silver_campaign_snapshots/part-0.parquet"),
        ("MERGE", "funnelstream.silver.silver_campaign_snapshots")]  # the snapshots: merged, never truncated
    assert not any("DROP" in v for _, v in log.events)


def test_the_snapshot_table_alone_is_never_truncated_even_with_yes(monkeypatch, tmp_path):
    log = Log()
    wire(monkeypatch, log)
    root = export_dir(tmp_path, ["silver_campaign_snapshots"])
    assert publish_silver.main(["--reset", "--yes", "--upload-only", "--export-dir", root,
                                "--tables", "silver_campaign_snapshots"]) == 0
    assert not [e for e in log.events if e[0] == "TRUNCATE"]
    assert [e for e in log.events if e[0] == "MERGE"]


def test_dry_run_with_reset_lists_the_truncate_and_executes_nothing(monkeypatch, capsys, tmp_path):
    log = Log()
    wire(monkeypatch, log, no_store=True)
    root = export_dir(tmp_path, ["silver_events"])
    rc = publish_silver.main(["--reset", "--dry-run", "--upload-only", "--export-dir", root,
                              "--tables", "silver_events"])
    out = capsys.readouterr().out
    assert rc == 0 and log.events == []
    assert "would run: TRUNCATE TABLE funnelstream.silver.silver_events" in out
    assert out.index("would run: TRUNCATE") < out.index("would run: MERGE")


def test_reset_with_export_only_is_rejected(monkeypatch, capsys):
    log = Log()
    wire(monkeypatch, log, no_store=True)
    assert publish_silver.main(["--reset", "--yes", "--export-only"]) == 2
    assert "cannot be combined" in capsys.readouterr().err and log.events == []


def test_unknown_table_with_reset_exits_2_before_anything_runs(monkeypatch, capsys):
    log = Log()
    wire(monkeypatch, log, no_store=True)
    assert publish_silver.main(["--reset", "--yes", "--tables", "nope"]) == 2
    assert log.events == []


def test_publish_function_truncates_only_exported_tables_and_marks_the_entry(tmp_path):
    log = Log()
    root = export_dir(tmp_path, ["silver_events"])
    res = publish(None, "unused", root, FakeStore(log), FakeUploader(log), "c", "v",
                  tables=["silver_events", "silver_orders", "silver_campaign_snapshots"], upload_only=True, reset=True)
    assert res["silver_events"] == {"rows": 0, "files": 1, "merged": True, "reset": True}
    assert res["silver_orders"] == {"rows": 0, "files": 0, "merged": False}  # nothing exported: not truncated
    assert [e[0] for e in log.events] == ["TRUNCATE", "CLEAR", "UPLOAD", "MERGE"]
    assert log.events[0][1] == "TRUNCATE TABLE c.silver.silver_events"


def test_publish_without_reset_never_truncates(tmp_path):
    log = Log()
    root = export_dir(tmp_path, ["silver_events"])
    res = publish(None, "unused", root, FakeStore(log), FakeUploader(log), "c", "v", tables=["silver_events"],
                  upload_only=True)
    assert res["silver_events"] == {"rows": 0, "files": 1, "merged": True}
    assert "TRUNCATE" not in [e[0] for e in log.events]
    with pytest.raises(ValueError):
        publish(None, "unused", root, FakeStore(log), FakeUploader(log), "c", "v", export_only=True, reset=True)
