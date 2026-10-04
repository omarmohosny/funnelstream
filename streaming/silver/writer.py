"""Idempotent Silver writes: Delta MERGE that only inserts rows whose key is not yet present.

silver_events, silver_orders and silver_order_status are partitioned by event_date and merged with a
partition predicate (the dates present in the batch), so a MERGE touches only those partitions.
silver_order_items and silver_quarantine stay unpartitioned (small tables). Caveat: a key is matched only
inside the partitions of the batch, so the same key arriving with a different event_date would be inserted
again; event_date is derived from event_time, which is fixed per event.
"""
import os
from datetime import date, datetime

from delta.tables import DeltaTable

SILVER_KEYS = {
    "silver_events": ["event_id"],
    "silver_orders": ["order_id"],
    "silver_order_status": ["event_id"],
    "silver_order_items": ["order_id", "line_no"],
    "silver_quarantine": ["quarantine_id"],
    "silver_products": ["product_id", "updated_at"],
    "silver_customers": ["customer_id", "updated_at"],
}
PARTITION_COLS = {
    "silver_events": "event_date",
    "silver_orders": "event_date",
    "silver_order_status": "event_date",
}


def silver_path(lake_root: str, table: str) -> str:
    return os.path.join(lake_root, "silver", table)


def _literal(value):
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return "DATE'%s'" % value.isoformat()
    return "'%s'" % str(value).replace("\\", "\\\\").replace("'", "''")


def partition_condition(col, values):
    """SQL predicate on the target alias t limiting a MERGE to the given partition values; '' if none."""
    vals = sorted({v for v in values if v is not None}, key=str)
    parts = []
    if vals:
        parts.append("t.`%s` IN (%s)" % (col, ", ".join(_literal(v) for v in vals)))
    if any(v is None for v in values):
        parts.append("t.`%s` IS NULL" % col)
    if not parts:
        return ""
    return parts[0] if len(parts) == 1 else "(" + " OR ".join(parts) + ")"


def merge_insert(spark, path, df, key_cols, partition_col=None):
    """MERGE df into the Delta table at path, inserting only unmatched keys (replays change nothing).

    The table is created from df.schema on the first write (partitioned by partition_col if given).
    df is de-duplicated on key_cols first so a single MERGE never sees two source rows with one key.
    With partition_col, the batch's distinct values of that column are added to the MERGE condition so
    Delta prunes partitions.
    """
    df = df.dropDuplicates(list(key_cols))
    if not DeltaTable.isDeltaTable(spark, path):
        b = DeltaTable.createIfNotExists(spark).location(path).addColumns(df.schema)
        if partition_col:
            b = b.partitionedBy(partition_col)
        b.execute()
    cond = " AND ".join("t.`%s` = s.`%s`" % (c, c) for c in key_cols)
    if partition_col:
        values = [r[0] for r in df.select(partition_col).distinct().collect()]
        extra = partition_condition(partition_col, values)
        if extra:
            cond += " AND " + extra
    (DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), cond)
     .whenNotMatchedInsertAll().execute())
