"""C1 (Spark, not run in the host suite): the standalone path works on a lake that has no inventory Bronze table."""
import pytest

from streaming.jobs import silver_clean
from streaming.jobs.bronze_ingest import bronze_path, ensure_bronze_tables

pytestmark = pytest.mark.spark


def test_ensure_creates_all_four_empty_tables_once(spark, tmp_path):
    lake = str(tmp_path / "lake")
    assert sorted(ensure_bronze_tables(spark, lake)) == ["bronze_app_events", "bronze_delivery_events",
                                                         "bronze_inventory_events", "bronze_orders"]
    assert ensure_bronze_tables(spark, lake) == []


def test_silver_clean_once_on_a_lake_with_three_bronze_tables(spark, tmp_path):
    lake = str(tmp_path / "lake")
    for t in ("app.events", "orders", "delivery.events"):
        ensure_bronze_tables(spark, lake, [t])  # the pre-branch lake: three tables, no inventory
    assert not (tmp_path / "lake" / "bronze" / "bronze_inventory_events").exists()
    q = silver_clean.run(lake, str(tmp_path / "ckpt"), once=True, live_store=False)
    q.awaitTermination(300)
    assert q.exception() is None
    assert spark.read.format("delta").load(bronze_path(lake, "inventory.events")).count() == 0
