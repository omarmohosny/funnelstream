"""Table specs of the Postgres live and health tables (spec 17.3, 17.17; shared contract g-shared-contract.md).

Kept apart from warehouse/tables.py on purpose: these are local serving tables, not Gold, so they are not part of
the generated Databricks/Postgres Gold DDL. The store only needs the names, columns and keys for its upserts;
the DDL itself is warehouse/live_ddl.sql.
"""
from warehouse.tables import Table, _parse

_LIVE_METRICS = ("minute_start:timestamp country:string events:bigint active_sessions:bigint "
                 "product_views:bigint add_to_cart:bigint orders_placed:bigint")
_HEALTH = "minute_start:timestamp events:bigint late_events:bigint quarantined:bigint silver_batches:bigint"
_BATCH_LOG = "job_run_id:string batch_id:bigint finished_at:timestamp"
_HEARTBEAT = "component:string last_seen_at:timestamp details:string?"

LIVE_METRIC_COLUMNS = ["events", "active_sessions", "product_views", "add_to_cart", "orders_placed"]
HEALTH_COLUMNS = ["events", "late_events", "quarantined", "silver_batches"]

LIVE_TABLES = [
    Table("live_metrics_minute", "gold", _parse(_LIVE_METRICS, ["minute_start", "country"]),
          ("minute_start", "country")),
    Table("pipeline_health_minute", "gold", _parse(_HEALTH, ["minute_start"]), ("minute_start",)),
    Table("pipeline_batch_log", "gold", _parse(_BATCH_LOG, ["job_run_id", "batch_id"]),
          ("job_run_id", "batch_id")),
    Table("pipeline_heartbeat", "gold", _parse(_HEARTBEAT, ["component"]), ("component",)),
]
LIVE_TABLE_NAMES = [t.name for t in LIVE_TABLES]
