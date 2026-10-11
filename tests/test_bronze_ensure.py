"""C1: silver_clean and bronze_ingest create missing Bronze tables themselves (host test, Spark is stubbed)."""
from unittest.mock import MagicMock

from streaming.jobs import bronze_ingest, pipeline, silver_clean


def test_helper_and_schema_live_in_bronze_ingest_and_pipeline_reuses_them():
    assert pipeline.ensure_bronze_tables is bronze_ingest.ensure_bronze_tables
    assert pipeline.BRONZE_SCHEMA is bronze_ingest.BRONZE_SCHEMA
    assert [f.name for f in bronze_ingest.BRONZE_SCHEMA.fields][:2] == ["raw_value", "kafka_topic"]


def test_silver_clean_run_ensures_all_bronze_tables_before_reading(monkeypatch, tmp_path):
    calls = []
    spark = MagicMock()
    monkeypatch.setattr(silver_clean, "get_spark", lambda name: spark)
    monkeypatch.setattr(silver_clean, "ensure_bronze_tables", lambda s, lake: calls.append((s, lake)) or [])
    spark.readStream.format.side_effect = lambda *a: (_ for _ in ()).throw(AssertionError("read before ensure")) \
        if not calls else MagicMock()
    monkeypatch.delenv("PG_DSN", raising=False)
    silver_clean.run(str(tmp_path / "lake"), str(tmp_path / "ckpt"), once=True)
    assert len(calls) == 1 and calls[0][0] is spark and calls[0][1].endswith("lake")


def test_bronze_ingest_run_ensures_the_tables_of_the_topics_it_reads(monkeypatch, tmp_path):
    import streaming.common.spark as common

    calls = []
    spark = MagicMock()
    monkeypatch.setattr(common, "get_spark", lambda name, with_kafka=False: spark)
    monkeypatch.setattr(bronze_ingest, "bronze_frame", lambda raw: MagicMock())
    monkeypatch.setattr(bronze_ingest, "ensure_bronze_tables",
                        lambda s, lake, topics=None: calls.append((lake, list(topics))) or [])
    bronze_ingest.run("kafka:1", str(tmp_path / "lake"), str(tmp_path / "ckpt"), topics=["orders"], once=True)
    assert len(calls) == 1 and calls[0][1] == ["orders"]
    calls.clear()
    bronze_ingest.run("kafka:1", str(tmp_path / "lake"), str(tmp_path / "ckpt"), once=True)
    assert calls[0][1] == list(bronze_ingest.BRONZE_TABLES)


def test_ensure_creates_only_missing_tables(monkeypatch, tmp_path):
    spark = MagicMock()
    existing = {bronze_ingest.bronze_path(str(tmp_path), "orders")}
    monkeypatch.setattr(bronze_ingest.DeltaTable, "isDeltaTable", staticmethod(lambda s, p: p in existing))
    made = bronze_ingest.ensure_bronze_tables(spark, str(tmp_path))
    assert sorted(made) == ["bronze_app_events", "bronze_delivery_events", "bronze_inventory_events"]
