"""Spark tests for warehouse/publish.py (run in Docker: RUN_SPARK_TESTS=1). Fake store and uploader only."""
import os

import pytest

pytestmark = pytest.mark.spark

from tests.test_silver_clean_spark import NOW, bronze_df, silver, silver_counts, simulated  # noqa: E402
from streaming.jobs.silver_clean import SILVER_TABLES, process_batch  # noqa: E402
from warehouse.publish import (PUBLISH_ALLOWLIST, build_publish_statements, export_table,  # noqa: E402
                               publish, upload_directory)


class FakeStore:
    def __init__(self):
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)


class FakeUploader:
    def __init__(self):
        self.files = {}

    def upload(self, remote_path, data):
        self.files[remote_path] = data


def _build_lake(spark, tmp_path, events=6_000):
    msgs, _ = simulated(events)
    lake = str(tmp_path / "lake")
    process_batch(spark, bronze_df(spark, msgs), lake, None, now=NOW)
    return lake


def test_packages_are_installed_in_the_image():
    import databricks.sdk  # noqa: F401
    import databricks.sql  # noqa: F401
    import pyarrow  # noqa: F401


def test_export_table_writes_allowlist_columns_only(spark, tmp_path):
    lake = _build_lake(spark, tmp_path)
    export = str(tmp_path / "export")
    counts = silver_counts(spark, lake)
    for table in ("silver_events", "silver_orders", "silver_order_items", "silver_quarantine"):
        folder, rows = export_table(spark, lake, table, export)
        assert rows == counts[table] > 0, table
        back = spark.read.parquet(folder)
        assert back.columns == PUBLISH_ALLOWLIST[table], table
        assert back.count() == rows
        n_files = len([f for f in os.listdir(folder) if f.endswith(".parquet")])
        assert 1 <= n_files <= 8
    assert "raw_payload" in silver(spark, lake, "silver_quarantine").columns  # exists locally
    print("\nP1 rows per table %s" % {t: counts[t] for t in SILVER_TABLES})


def test_export_table_skips_missing_table(spark, tmp_path):
    assert export_table(spark, str(tmp_path / "lake"), "silver_campaign_snapshots", str(tmp_path / "e")) == ("", 0)
    assert not os.path.exists(tmp_path / "e")


def test_export_table_fails_loudly_on_missing_column(spark, tmp_path, monkeypatch):
    lake = _build_lake(spark, tmp_path, events=2_000)
    import warehouse.publish as pub
    monkeypatch.setitem(pub.PUBLISH_ALLOWLIST, "silver_orders",
                        PUBLISH_ALLOWLIST["silver_orders"] + ["no_such_column"])
    with pytest.raises(Exception, match="no_such_column"):
        export_table(spark, lake, "silver_orders", str(tmp_path / "e"))


def test_publish_end_to_end_with_fakes_and_repeat(spark, tmp_path):
    lake = _build_lake(spark, tmp_path, events=8_000)
    counts = silver_counts(spark, lake)
    runs = []
    for i in range(2):
        store, up = FakeStore(), FakeUploader()
        res = publish(spark, lake, str(tmp_path / "export"), store, up, "funnelstream", "silver_upload")
        runs.append((res, list(store.executed)))
        for table in SILVER_TABLES:
            assert res[table]["rows"] == counts[table], table
            assert res[table]["merged"] is True and res[table]["files"] >= 1
        # uploaded files' row total equals the Silver rows per table
        import io
        import pyarrow.parquet as pq
        for table in SILVER_TABLES:
            prefix = "/Volumes/funnelstream/silver/silver_upload/%s/" % table
            total = sum(pq.read_table(io.BytesIO(data)).num_rows for p, data in up.files.items()
                        if p.startswith(prefix))
            assert total == counts[table], table
        # one MERGE per published table, campaign snapshots has no local Delta so it is skipped
        assert res["silver_campaign_snapshots"] == {"rows": 0, "files": 0, "merged": False}
        assert store.executed == [build_publish_statements(t, "funnelstream", "silver_upload")[0]
                                  for t in PUBLISH_ALLOWLIST if t in SILVER_TABLES]
    assert runs[0] == runs[1]  # second publish: same counts, same statements
    print("\nP2 rows %s statements=%d" % ({t: runs[0][0][t]["rows"] for t in SILVER_TABLES}, len(runs[0][1])))


def test_upload_directory_over_real_export_skips_spark_metadata(spark, tmp_path):
    lake = _build_lake(spark, tmp_path, events=2_000)
    folder, _ = export_table(spark, lake, "silver_orders", str(tmp_path / "e"))
    names = os.listdir(folder)
    assert "_SUCCESS" in names and any(n.endswith(".crc") for n in names)
    up = FakeUploader()
    n = upload_directory(up, folder, "/Volumes/c/silver/v/silver_orders")
    assert n == len([x for x in names if x.endswith(".parquet")]) == len(up.files)
    assert all(p.endswith(".parquet") for p in up.files)
