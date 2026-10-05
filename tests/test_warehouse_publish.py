"""Host tests for warehouse/publish.py (no Spark, fake store and uploader, no network)."""
import os

import pytest

from warehouse.publish import (BACKFILL_COLUMNS, BANNED_COLUMNS, NAME_EXCEPTIONS, PUBLISH_ALLOWLIST, PUBLISH_KEYS, build_publish_statements, export_table,
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
            if c in BANNED_COLUMNS:
                assert (table, c) in NAME_EXCEPTIONS, (table, c)
    assert NAME_EXCEPTIONS == {("silver_products", "name")}
    assert BANNED_COLUMNS == ("name", "phone", "email", "address")
    assert "name" in PUBLISH_ALLOWLIST["silver_products"]
    offenders = [(t, c) for t, cols in PUBLISH_ALLOWLIST.items() for c in cols if c in BANNED_COLUMNS]
    assert offenders == [("silver_products", "name")]
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
                    "parquet.`/Volumes/funnelstream/silver/silver_upload/silver_events` "
                    "QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_id) = 1) AS s "
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


# I-2: de-duplicated MERGE source
def test_every_table_statement_has_qualify_on_its_keys_and_is_insert_only_except_backfill():
    for table in PUBLISH_ALLOWLIST:
        (stmt,) = build_publish_statements(table, "cat", "vol")
        keys = ", ".join(PUBLISH_KEYS[table])
        assert ("QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 1) AS s" % (keys, keys)) in stmt, table
        assert "INSERT (%s) VALUES (" % ", ".join(PUBLISH_ALLOWLIST[table]) in stmt
        assert stmt.count("WHEN NOT MATCHED") == 1 and "DELETE" not in stmt
        if table in BACKFILL_COLUMNS:
            continue
        assert "UPDATE" not in stmt and "WHEN MATCHED" not in stmt, table


def test_products_backfill_is_narrow_name_only():
    assert BACKFILL_COLUMNS == {"silver_products": ["name"]}
    (stmt,) = build_publish_statements("silver_products", "cat", "vol")
    assert ("ON t.product_id = s.product_id AND t.updated_at = s.updated_at "
            "WHEN MATCHED AND t.name IS NULL AND s.name IS NOT NULL THEN UPDATE SET t.name = s.name "
            "WHEN NOT MATCHED THEN INSERT (") in stmt
    assert stmt.count("UPDATE SET") == 1 and "t.price" not in stmt


def test_same_file_uploaded_twice_still_merges_through_key_dedup(tmp_path):
    make_export(tmp_path, ("silver_events",))
    volume = {}  # a fake volume that, unlike the real clear(), keeps what was uploaded before

    class KeepingUploader:
        def upload(self, path, data):
            volume.setdefault(path, []).append(data)

    store = FakeStore()
    for _ in range(2):
        publish(None, "l", str(tmp_path), store, KeepingUploader(), "c", "v", tables=["silver_events"],
                upload_only=True)
    assert len(volume) == 2 and all(len(v) == 2 for v in volume.values())  # duplicates did land in the volume
    assert len(store.executed) == 2 and store.executed[0] == store.executed[1]
    assert "PARTITION BY event_id" in store.executed[0]


# I-1: quarantine can not echo payload values
def _canary_cases():
    import json
    import random
    from datetime import datetime, timezone

    from simulator.catalog import make_catalog, make_customers
    from simulator.orders import make_order

    C = "CANARY_7f3a9"
    catalog, cust = make_catalog(3), make_customers(3, 5)
    order = make_order(random.Random(5), cust[0], catalog, None, datetime(2026, 9, 1, tzinfo=timezone.utc), "ord_c")
    base = order[0][1]
    for it in base["payload"]["items"]:
        it["product_id"] = C + "_prod"

    def variant(**top):
        e = json.loads(json.dumps(base))
        e["event_id"], e["session_id"], e["order_id"] = C + "_evt", C + "_sess", C + "_ord"
        e["payload"]["channel"] = C + "_chan"
        for k, v in top.items():
            e[k] = v
        return e

    cases = {}
    cases["BAD_JSON"] = '{"event_id": "%s_evt", "payload": {"items": [{"product_id": "%s_prod"' % (C, C)
    cases["MISSING_FIELD"] = variant(order_id="")
    cases["UNKNOWN_EVENT_TYPE"] = variant(event_type=C + "_type")
    cases["BAD_VERSION"] = variant(event_version=C)
    neg = variant()
    neg["payload"]["items"][0]["unit_price"] = -5
    neg["payload"]["order_total"] = -5
    cases["NEGATIVE_PRICE"] = neg
    cases["FUTURE_TIMESTAMP"] = variant(event_time="2999-01-01T00:00:00+00:00")
    bad = variant()
    bad["payload"]["items"][0]["quantity"] = C + "_qty"
    cases["BAD_VALUE"] = bad
    from scripts.run_simulator import iter_batch
    app = next(e for _, e in iter_batch(random.Random(3), cust, catalog, datetime(2026, 9, 27, tzinfo=timezone.utc),
                                        max_events=400) if e.get("payload", {}).get("product_id"))
    app["event_id"], app["session_id"] = C + "_evt", C + "_sess"
    app["payload"]["product_id"] = C + "_prod"
    app["payload"]["price"] = 12.5
    cases["UNKNOWN_PRODUCT"] = app
    return C, {k: (v if isinstance(v, str) else json.dumps(v)) for k, v in cases.items()}


def test_quarantine_error_detail_never_echoes_payload_values():
    from datetime import datetime, timezone

    from streaming.silver.transform import transform_message

    canary, cases = _canary_cases()
    now = datetime(2026, 10, 20, tzinfo=timezone.utc)
    seen = set()
    for expected, raw in cases.items():
        rows = transform_message(raw, "orders", now, now, known_products={"prod_0001"}, now=now)
        assert [t for t, _ in rows] == ["silver_quarantine"], expected
        row = rows[0][1]
        seen.add(row["error_code"])
        assert canary in row["raw_payload"], expected  # the canary really is in the raw row
        for col, value in row.items():
            if col == "raw_payload":
                continue
            assert canary not in str(value), (expected, col)
        published = {c: row[c] for c in PUBLISH_ALLOWLIST["silver_quarantine"]}
        assert canary not in repr(published) and "raw_payload" not in published
    assert seen == {"BAD_JSON", "MISSING_FIELD", "UNKNOWN_EVENT_TYPE", "BAD_VERSION", "NEGATIVE_PRICE",
                    "FUTURE_TIMESTAMP", "BAD_VALUE", "UNKNOWN_PRODUCT"}, seen
    assert any(canary in c for c in cases.values())


def test_cancel_after_shipment_detail_is_constant():
    import inspect

    import streaming.silver.orders as orders
    src = inspect.getsource(orders)
    assert "order_cancelled arrived after order_shipped for the same order" in src
    assert "%" not in src.split("error_detail")[1].split("\n")[0]
