"""Helpers shared by the live Postgres computes (kept apart so live_store and live_stock do not import each other)."""
from datetime import datetime, timezone

from delta.tables import DeltaTable

from streaming.silver.writer import silver_path


def _utc(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def _read(spark, silver_root, table):
    """The Silver Delta table, or None when it does not exist yet (Silver only creates a table on its first row)."""
    path = silver_path(silver_root, table)
    return spark.read.format("delta").load(path) if DeltaTable.isDeltaTable(spark, path) else None
