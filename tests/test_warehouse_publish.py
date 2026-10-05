"""Host tests for warehouse/publish.py (no Spark, fake store and uploader, no network)."""
import os

import pytest

from warehouse.publish import (PUBLISH_ALLOWLIST, PUBLISH_KEYS, build_publish_statements, export_table,
                               make_uploader, publish, upload_directory, volume_path)
from warehouse.tables import SILVER_MIRROR_TABLES

MIRROR = {t.name: t for t in SILVER_MIRROR_TABLES}


class FakeUploader:
    def __init__(self):
        self.calls = []
        self.cleared = []

    def upload(self, remote_path, data):
        self.calls.append((remote_path, data))

    def clear(self, remote_dir):
        self.cleared.append(remote_dir)


class FakeStore:
    def __init__(self):
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)


def make_export(tmp_path, tables=("silver_events", "silver_order_items")):
    for t in tables:
        d = tmp_path / t
        d.mkdir(parents=True)
        (d / "part-0000.parquet").write_bytes(b"PAR1-" + t.encode())
        (d / "part-0001.parquet").write_bytes(b"PAR1-b")
        (d / "_SUCCESS").write_bytes(b"")
        (d / ".part-0000.parquet.crc").write_bytes(b"crc")
    return tmp_path


# 8. allow-list
def test_allowlist_columns_exist_in_mirror_and_cover_tables():
    assert set(PUBLISH_ALLOWLIST) == set(MIRROR)  # all eight mirror tables
    assert len(PUBLISH_ALLOWLIST) == 8
    for table, cols in PUBLISH_ALLOWLIST.items():
        names = MIRROR[table].column_names()
        assert len(cols) == len(set(cols)), table
        for c in cols:
            assert c in names, (table, c)


def test_allowlist_has_no_raw_payload_or_personal_columns():
    for table, cols in PUBLISH_ALLOWLIST.items():
        for c in cols:
            assert c != "raw_payload"
            assert c not in ("name", "phone", "email", "address"), (table, c)
    assert "raw_payload" in MIRROR["silver_quarantine"].column_names()  # exists in the mirror, never filled


def test_allowlist_keys_exist_in_allowlist():
    assert set(PUBLISH_KEYS) == set(PUBLISH_ALLOWLIST)
    for table, keys in PUBLISH_KEYS.items():
        for k in keys:
            assert k in PUBLISH_ALLOWLIST[table], (table, k)
    assert PUBLISH_KEYS["silver_order_items"] == ["order_id", "line_no"]
    assert PUBLISH_KEYS["silver_campaign_snapshots"] == ["campaign_id", "report_date", "pulled_at"]
    for table, spec in MIRROR.items():  # keys agree with the mirror spec where the spec has them
        if spec.keys:
            assert list(spec.keys) == PUBLISH_KEYS[table]


# 9. build_publish_statements
def test_statement_silver_events_exact():
    (stmt,) = build_publish_statements("silver_events", "funnelstream", "silver_upload")
    cols = ", ".join(PUBLISH_ALLOWLIST["silver_events"])
    vals = ", ".join("s." + c for c in PUBLISH_ALLOWLIST["silver_events"])
    assert stmt == ("MERGE INTO funnelstream.silver.silver_events AS t USING (SELECT %s FROM "
                    "parquet.`/Volumes/funnelstream/silver/silver_upload/silver_events`) AS s "
                    "ON t.event_id = s.event_id WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)" % (cols, cols, vals))


def test_statement_composite_key_insert_only_no_star():
    (stmt,) = build_publish_statements("silver_order_items", "cat", "vol")
    assert "ON t.order_id = s.order_id AND t.line_no = s.line_no WHEN NOT MATCHED" in stmt
    assert "UPDATE" not in stmt and "WHEN MATCHED" not in stmt and "*" not in stmt
    assert "parquet.`/Volumes/cat/silver/vol/silver_order_items`" in stmt
    (q,) = build_publish_statements("silver_quarantine", "cat", "vol")
    assert "raw_payload" not in q and "ON t.quarantine_id = s.quarantine_id" in q


def test_statement_validates_identifiers_and_table():
    for catalog, volume in (("c", "bad vol"), ("c;x", "v"), ("c", "v`; DROP"), ("", "v")):
        with pytest.raises(ValueError):
            build_publish_statements("silver_events", catalog, volume)
    with pytest.raises(ValueError):
        build_publish_statements("silver_nope", "c", "v")
    with pytest.raises(ValueError):
        volume_path("c", "v", "t t")
    assert volume_path("funnelstream", "silver_upload", "silver_events") == \
        "/Volumes/funnelstream/silver/silver_upload/silver_events"


# 10. upload_directory
def test_upload_directory_only_parquet_and_paths(tmp_path):
    make_export(tmp_path, ("silver_events",))
    up = FakeUploader()
    n = upload_directory(up, str(tmp_path / "silver_events"), "/Volumes/c/silver/v/silver_events/")
    assert n == 2
    assert [p for p, _ in up.calls] == ["/Volumes/c/silver/v/silver_events/part-0000.parquet",
                                        "/Volumes/c/silver/v/silver_events/part-0001.parquet"]
    assert up.calls[0][1] == b"PAR1-silver_events"
    assert up.cleared == ["/Volumes/c/silver/v/silver_events/"]  # stale parts of earlier exports go first
    assert upload_directory(up, str(tmp_path / "missing"), "/r") == 0


def test_make_uploader_normalises_host(monkeypatch):
    import databricks.sdk as sdk
    seen = {}

    class FakeClient:
        def __init__(self, host, token):
            seen.update(host=host, token=token)
            self.files = self

        def upload(self, path, fileobj, overwrite):
            seen.update(path=path, data=fileobj.read(), overwrite=overwrite)

    monkeypatch.setattr(sdk, "WorkspaceClient", FakeClient)
    u = make_uploader("adb-1.example.invalid/", "tok")
    assert seen["host"] == "https://adb-1.example.invalid"
    u.upload("/Volumes/x", b"abc")
    assert seen["path"] == "/Volumes/x" and seen["data"] == b"abc" and seen["overwrite"] is True
    assert "tok" not in repr(u)
    make_uploader("https://adb-2.example.invalid", "tok")
    assert seen["host"] == "https://adb-2.example.invalid"


# 11. publish orchestration
def test_publish_upload_only_runs_in_table_order(tmp_path):
    export = make_export(tmp_path)
    store, up = FakeStore(), FakeUploader()
    res = publish(None, "unused", str(export), store, up, "funnelstream", "silver_upload",
                  tables=["silver_order_items", "silver_events", "silver_orders"], upload_only=True)
    assert list(res) == ["silver_order_items", "silver_events", "silver_orders"]
    assert res["silver_order_items"]["files"] == 2 and res["silver_order_items"]["merged"] is True
    assert res["silver_events"]["merged"] is True
    assert res["silver_orders"] == {"rows": 0, "files": 0, "merged": False}  # nothing exported for it
    assert [s.split("silver.")[1].split(" ")[0] for s in store.executed] == ["silver_order_items", "silver_events"]
    assert len(up.calls) == 4
    assert up.calls[0][0].startswith("/Volumes/funnelstream/silver/silver_upload/silver_order_items/")


def test_publish_dry_run_executes_and_uploads_nothing(tmp_path):
    export = make_export(tmp_path)
    store, up = FakeStore(), FakeUploader()
    res = publish(None, "unused", str(export), store, up, "funnelstream", "silver_upload",
                  tables=["silver_events", "silver_order_items"], upload_only=True, dry_run=True)
    assert store.executed == [] and up.calls == []
    assert res["silver_events"]["merged"] is False and res["silver_events"]["files"] == 2
    assert res["silver_events"]["statements"] == build_publish_statements("silver_events", "funnelstream",
                                                                          "silver_upload")


def test_publish_export_only_uploads_nothing(tmp_path, monkeypatch):
    import warehouse.publish as pub
    calls = []

    def fake_export(spark, lake, table, out):
        calls.append(table)
        d = tmp_path / table
        d.mkdir(exist_ok=True)
        (d / "p.parquet").write_bytes(b"x")
        return str(d), 5

    monkeypatch.setattr(pub, "export_table", fake_export)
    store, up = FakeStore(), FakeUploader()
    res = publish(object(), "lake", str(tmp_path), store, up, "c", "v", tables=["silver_events"], export_only=True)
    assert calls == ["silver_events"] and up.calls == [] and store.executed == []
    assert res["silver_events"] == {"rows": 5, "files": 1, "merged": False}


def test_publish_full_path_with_fake_export_and_repeat_is_stable(tmp_path, monkeypatch):
    import warehouse.publish as pub

    def fake_export(spark, lake, table, out):
        d = tmp_path / table
        d.mkdir(exist_ok=True)
        (d / "p.parquet").write_bytes(b"x")
        return str(d), 7

    monkeypatch.setattr(pub, "export_table", fake_export)
    runs = []
    for _ in range(2):
        store, up = FakeStore(), FakeUploader()
        runs.append((publish(object(), "l", str(tmp_path), store, up, "c", "v", tables=["silver_orders"]),
                     store.executed, up.calls))
    assert runs[0] == runs[1]
    assert runs[0][0] == {"silver_orders": {"rows": 7, "files": 1, "merged": True}}


def test_publish_rejects_bad_arguments(tmp_path):
    with pytest.raises(ValueError):
        publish(None, "l", str(tmp_path), FakeStore(), FakeUploader(), "c", "v", tables=["nope"])
    with pytest.raises(ValueError):
        publish(None, "l", str(tmp_path), FakeStore(), FakeUploader(), "c", "v", export_only=True, upload_only=True)


def test_export_table_missing_local_folder_is_skipped(tmp_path, caplog):
    class NoSpark:
        def __getattr__(self, name):
            raise AssertionError("spark must not be touched")

    with caplog.at_level("WARNING"):
        assert export_table(NoSpark(), str(tmp_path), "silver_campaign_snapshots", str(tmp_path / "x")) == ("", 0)
    assert "skipping silver_campaign_snapshots" in caplog.text
    assert not os.path.exists(tmp_path / "x")
    with pytest.raises(ValueError):
        export_table(NoSpark(), str(tmp_path), "bronze_orders", str(tmp_path))


def test_sdk_uploader_clear_deletes_only_stale_parquet_files():
    class E:
        def __init__(self, name, is_directory=False):
            self.name, self.is_directory = name, is_directory

    class NotFound(Exception):
        pass

    class Files:
        deleted = []

        def list_directory_contents(self, d):
            if d.endswith("gone"):
                raise NotFound("x")
            return [E("part-1.parquet"), E("part-2.parquet"), E("_SUCCESS"), E("sub.parquet", True)]

        def delete(self, path):
            self.deleted.append(path)

    class Client:
        files = Files()

    from warehouse.publish import _SdkUploader
    u = _SdkUploader(Client())
    u.clear("/Volumes/c/silver/v/t/")
    assert Client.files.deleted == ["/Volumes/c/silver/v/t/part-1.parquet", "/Volumes/c/silver/v/t/part-2.parquet"]
    u.clear("/Volumes/c/silver/v/gone")  # missing folder is fine
    assert len(Client.files.deleted) == 2
