"""Idempotent Silver writes: Delta MERGE that only inserts rows whose key is not yet present."""
import os

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


def silver_path(lake_root: str, table: str) -> str:
    return os.path.join(lake_root, "silver", table)


def merge_insert(spark, path, df, key_cols):
    """MERGE df into the Delta table at path, inserting only unmatched keys (replays change nothing).

    The table is created from df.schema on the first write. df is de-duplicated on key_cols first so
    a single MERGE never sees two source rows with one key.
    """
    df = df.dropDuplicates(list(key_cols))
    if not DeltaTable.isDeltaTable(spark, path):
        (DeltaTable.createIfNotExists(spark).location(path).addColumns(df.schema).execute())
    cond = " AND ".join("t.`%s` = s.`%s`" % (c, c) for c in key_cols)
    (DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), cond)
     .whenNotMatchedInsertAll().execute())
