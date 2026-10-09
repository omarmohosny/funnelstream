"""Spark tests for scripts/check_duplicates.py (run in Docker)."""
import shutil
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.spark

from scripts.check_duplicates import run  # noqa: E402
from streaming.silver.writer import silver_path  # noqa: E402

T = datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc)


def _write(spark, lake, table, rows, schema, mode="overwrite"):
    spark.createDataFrame(rows, schema).write.format("delta").mode(mode).save(silver_path(lake, table))


def _lake(spark, lake, extra_event=None):
    events = [("e1", T), ("e2", T)] + ([extra_event] if extra_event else [])
    _write(spark, lake, "silver_events", events, "event_id string, event_time timestamp")
    _write(spark, lake, "silver_orders", [("o1", T)], "order_id string, event_time timestamp")
    _write(spark, lake, "silver_order_status", [("s1", T), ("s2", T)], "event_id string, event_time timestamp")
    _write(spark, lake, "silver_order_items", [("o1", 1, "p1"), ("o1", 2, "p2")],
           "order_id string, line_no int, product_id string")


def test_clean_tables_report_no_duplicates(spark, tmp_path):
    lake = str(tmp_path)
    _lake(spark, lake)
    out = []
    assert run(lake, spark, out.append) == 0
    assert out[-1] == "NO DUPLICATES"
    assert len(out) == 5 and all("duplicate_keys=0" in l for l in out[:4])
    assert any(l.startswith("silver_events") and "rows=2" in l for l in out)


def test_injected_duplicate_event_id_is_found(spark, tmp_path):
    lake = str(tmp_path)
    _lake(spark, lake, extra_event=("e1", T))
    out = []
    assert run(lake, spark, out.append) == 1
    assert out[-1] == "DUPLICATES FOUND"
    ev = [l for l in out if l.startswith("silver_events")][0]
    assert "rows=3" in ev and "duplicate_keys=1" in ev


def test_composite_key_duplicate_in_order_items(spark, tmp_path):
    lake = str(tmp_path)
    _lake(spark, lake)
    _write(spark, lake, "silver_order_items", [("o1", 2, "p2")],
           "order_id string, line_no int, product_id string", mode="append")
    out = []
    assert run(lake, spark, out.append) == 1
    assert any(l.startswith("silver_order_items") and "duplicate_keys=1" in l for l in out)


def test_missing_table_exits_2(spark, tmp_path):
    lake = str(tmp_path)
    _lake(spark, lake)
    shutil.rmtree(silver_path(lake, "silver_orders"))
    out = []
    assert run(lake, spark, out.append) == 2
    assert any("Missing Silver table" in l and "silver_orders" in l for l in out)
