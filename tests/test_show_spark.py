"""Spark tests for the terminal views of the Silver job and the Bronze progress loop (run in Docker)."""
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.spark

from streaming.jobs.silver_clean import show_batch  # noqa: E402
from streaming.silver.writer import silver_path  # noqa: E402


def _write(spark, lake, table, rows, schema):
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").save(silver_path(lake, table))


def test_show_batch_prints_counts_and_newest_rows_of_tables_that_grew(spark, tmp_path):
    lake = str(tmp_path)
    t = lambda m: datetime(2026, 10, 6, 12, m, tzinfo=timezone.utc)
    _write(spark, lake, "silver_events",
           [(t(1), "page_view", "Maadi", "app", "meta", "shop.example.eg/?x"), (t(2), "product_view", "Dokki", "web", None, None)],
           "event_time timestamp, event_type string, district string, platform string, utm_source string, page_url string")
    _write(spark, lake, "silver_quarantine", [(t(3), "BAD_JSON", "orders", "x")],
           "received_at timestamp, error_code string, source_topic string, error_detail string")
    out = []
    show_batch(spark, lake, 7, {"silver_events": 2, "silver_orders": 0, "silver_quarantine": 1},
               {"district_violations": 0}, sample=1, out=out.append)
    text = "\n".join(out)
    assert "[silver] batch 7" in text and "events=2" in text and "district_violations=0" in text
    assert "product_view" in text and "Dokki" in text      # newest event only (sample=1)
    assert "page_view" not in text
    assert "BAD_JSON" in text
    assert not any(line.strip().startswith("orders ") for line in out)  # silver_orders did not grow: no sample lines


def test_show_batch_survives_a_missing_table(spark, tmp_path):
    out = []
    show_batch(spark, str(tmp_path), 1, {"silver_events": 5}, {}, sample=2, out=out.append)
    assert any("sample unavailable" in line for line in out)
