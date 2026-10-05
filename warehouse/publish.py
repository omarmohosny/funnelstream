"""Publish local Silver Delta tables to the Databricks Silver mirror (spec section 3, item 13).

Flow per table: export an explicit allow-list of columns to Parquet (Spark, local) -> upload the Parquet
files to a Unity Catalog volume -> MERGE them into funnelstream.silver.<table> (insert-only, idempotent).
Names, phones and raw payloads never leave the machine: silver_quarantine.raw_payload and every
free-form personal column are simply not in PUBLISH_ALLOWLIST (the mirror column stays NULL).

Product titles (silver_products.name) are published: the only exception to the ban on columns called
name, phone, email or address is NAME_EXCEPTIONS (a product title is not a person's name).

This module imports no Spark at module level (the host tests run without it).
"""
import io
import logging
import math
import os
import re

from warehouse.store import _ident

log = logging.getLogger("funnelstream.publish")

# Column names that must never be published, except the explicit exceptions below.
BANNED_COLUMNS = ("name", "phone", "email", "address")
NAME_EXCEPTIONS = {("silver_products", "name")}

# PUBLISH_ALLOWLIST: explicit per-table column lists, the only columns that leave the machine.
# NOTE: search_term (silver_events) and reason (silver_order_status) are FREE-TEXT columns. They are
# published because the data is synthetic; review them (and possibly drop or hash them) before this
# pipeline is used with real customer data.
PUBLISH_ALLOWLIST = {
    "silver_events": [
        "event_id", "event_type", "event_time", "kafka_timestamp", "is_late", "is_bot", "event_version",
        "session_id", "customer_id", "device", "channel", "platform", "country", "city", "campaign_id",
        "utm_source", "utm_campaign", "click_id", "product_id", "element", "dwell_seconds", "cart_id", "order_id",
        "search_term", "event_date",
    ],
    "silver_orders": [
        "order_id", "customer_id", "session_id", "channel", "platform", "utm_source", "utm_campaign", "click_id",
        "payment_method", "city", "country", "order_total", "order_created_at", "event_date",
    ],
    "silver_order_status": [
        "event_id", "order_id", "customer_id", "status", "status_rank", "status_time", "payment_method",
        "carrier", "reason", "event_date",
    ],
    "silver_order_items": [
        "order_id", "line_no", "product_id", "quantity", "unit_price", "line_total", "currency", "order_time",
    ],
    "silver_quarantine": [  # raw_payload deliberately absent: raw rows never leave the machine
        "quarantine_id", "source_topic", "error_code", "error_detail", "received_at",
    ],
    "silver_products": ["product_id", "updated_at", "name", "category", "price", "is_active"],
    "silver_customers": [
        "customer_id", "updated_at", "country", "city", "signup_date", "primary_device", "marketing_consent",
    ],
    "silver_campaign_snapshots": [
        "campaign_id", "report_date", "pulled_at", "utm_campaign", "platform", "impressions", "clicks",
        "platform_conversions", "spend", "currency",
    ],
}

PUBLISH_KEYS = {
    "silver_events": ["event_id"],
    "silver_orders": ["order_id"],
    "silver_order_status": ["event_id"],
    "silver_order_items": ["order_id", "line_no"],
    "silver_quarantine": ["quarantine_id"],
    "silver_products": ["product_id", "updated_at"],
    "silver_customers": ["customer_id", "updated_at"],
    "silver_campaign_snapshots": ["campaign_id", "report_date", "pulled_at"],
}

# Columns refreshed on already-published keys when the mirror holds NULL (narrow repair path: rows published
# before the column was on the allow-list). Everything else stays insert-only.
BACKFILL_COLUMNS = {"silver_products": ["name"]}

MAX_FILES = 8
TARGET_FILE_BYTES = 128 * 1024 * 1024


def volume_path(catalog, volume, table) -> str:
    return "/Volumes/%s/silver/%s/%s" % (_ident(catalog), _ident(volume), _ident(table))


def build_publish_statements(table, catalog, volume) -> list:
    if table not in PUBLISH_ALLOWLIST:
        raise ValueError("table is not publishable: %s" % table)
    cols = PUBLISH_ALLOWLIST[table]
    for c in cols:
        _ident(c)
    path = volume_path(catalog, volume, table)
    on = " AND ".join("t.%s = s.%s" % (k, k) for k in PUBLISH_KEYS[table])
    collist = ", ".join(cols)
    keys = ", ".join(PUBLISH_KEYS[table])
    # QUALIFY: defence in depth, the source holds each key once even if the volume folder holds duplicates.
    source = ("SELECT %s FROM parquet.`%s` QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 1"
              % (collist, path, keys, keys))
    backfill = ""
    for c in BACKFILL_COLUMNS.get(table, []):
        _ident(c)
        backfill += " WHEN MATCHED AND t.%s IS NULL AND s.%s IS NOT NULL THEN UPDATE SET t.%s = s.%s" % (c, c, c, c)
    return ["MERGE INTO %s.silver.%s AS t USING (%s) AS s ON %s%s "
            "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)"
            % (_ident(catalog), table, source, on, backfill, collist, ", ".join("s." + c for c in cols))]


def _folder_bytes(path):
    return sum(os.path.getsize(os.path.join(d, f)) for d, _, files in os.walk(path) for f in files)


def _parquet_files(folder):
    if not os.path.isdir(folder):
        return []
    return sorted(f for f in os.listdir(folder)
                  if f.endswith(".parquet") and os.path.isfile(os.path.join(folder, f)))


def export_table(spark, lake_root, table, export_dir):
    """Write the allow-list columns of one local Silver table as Parquet. Returns (folder, row count)."""
    if table not in PUBLISH_ALLOWLIST:
        raise ValueError("table is not publishable: %s" % table)
    src = os.path.join(os.path.abspath(lake_root), "silver", table)
    if not os.path.isdir(src):
        log.warning("skipping %s: no local Delta table at %s", table, src)
        return "", 0
    out = os.path.join(os.path.abspath(export_dir), table)
    df = spark.read.format("delta").load(src).select(*PUBLISH_ALLOWLIST[table])  # a missing column fails here
    rows = df.count()
    files = max(1, min(MAX_FILES, math.ceil(_folder_bytes(src) / TARGET_FILE_BYTES)))
    df.coalesce(files).write.mode("overwrite").parquet(out)
    return out, rows


class Uploader:
    """Protocol: upload(remote_path, data: bytes) -> None (overwrites)."""

    def upload(self, remote_path, data):  # pragma: no cover - interface
        raise NotImplementedError

    def clear(self, remote_dir):  # pragma: no cover - interface
        """Delete the *.parquet files already in remote_dir (a missing folder is fine)."""
        raise NotImplementedError


class _SdkUploader(Uploader):
    def __init__(self, client):
        self._client = client

    def upload(self, remote_path, data):
        self._client.files.upload(remote_path, io.BytesIO(data), overwrite=True)

    def clear(self, remote_dir):
        # Spark names part files with a fresh UUID on every export; without this the volume folder keeps the
        # parts of earlier exports and the MERGE source would hold the same rows several times.
        try:
            entries = list(self._client.files.list_directory_contents(remote_dir))
        except Exception as e:
            if type(e).__name__ == "NotFound":
                return
            raise
        for entry in entries:
            if not entry.is_directory and entry.name.endswith(".parquet"):
                self._client.files.delete(remote_dir.rstrip("/") + "/" + entry.name)

    def __repr__(self):
        return "_SdkUploader()"


def make_uploader(host, token) -> Uploader:
    from databricks.sdk import WorkspaceClient
    host = host.strip()
    if not re.match(r"^https?://", host):
        host = "https://" + host
    return _SdkUploader(WorkspaceClient(host=host.rstrip("/"), token=token))


def upload_directory(client, local_dir, remote_dir) -> int:
    names = _parquet_files(local_dir)
    if names and hasattr(client, "clear"):
        client.clear(remote_dir)
    for name in names:
        with open(os.path.join(local_dir, name), "rb") as fh:
            client.upload(remote_dir.rstrip("/") + "/" + name, fh.read())
    return len(names)


def _parquet_rows(spark, folder):
    """Row count of exported Parquet: Spark when given, else pyarrow footers; 0 if unreadable."""
    try:
        if spark is not None:
            return spark.read.parquet(folder).count()
        import pyarrow.parquet as pq
        return sum(pq.ParquetFile(os.path.join(folder, n)).metadata.num_rows for n in _parquet_files(folder))
    except Exception as e:  # counting is informational
        log.warning("could not count rows in %s: %s", folder, type(e).__name__)
        return 0


def publish(spark, lake_root, export_dir, store, uploader, catalog, volume, tables=None, dry_run=False,
            export_only=False, upload_only=False) -> dict:
    tables = list(tables) if tables else list(PUBLISH_ALLOWLIST)
    unknown = [t for t in tables if t not in PUBLISH_ALLOWLIST]
    if unknown:
        raise ValueError("not publishable: %s" % ", ".join(unknown))
    if export_only and upload_only:
        raise ValueError("export_only and upload_only cannot be combined")
    result = {}
    for table in tables:
        statements = build_publish_statements(table, catalog, volume)
        if upload_only:
            folder = os.path.join(os.path.abspath(export_dir), table)
            folder = folder if os.path.isdir(folder) else ""
            rows = _parquet_rows(spark, folder) if folder and dry_run else 0
        else:
            folder, rows = export_table(spark, lake_root, table, export_dir)
        entry = {"rows": rows, "files": len(_parquet_files(folder)) if folder else 0, "merged": False}
        if dry_run:
            entry["statements"] = statements if entry["files"] else []
        elif folder and not export_only:
            entry["files"] = upload_directory(uploader, folder, volume_path(catalog, volume, table))
            if upload_only:
                entry["rows"] = _parquet_rows(spark, folder)
            if entry["files"]:
                for stmt in statements:
                    store.execute(stmt)
                entry["merged"] = True
        result[table] = entry
    return result
