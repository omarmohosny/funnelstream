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
_DIM_STORE = "store_id:string store_name:string city:string"
_DIM_STORE_DISTRICT = "city:string district:string store_id:string"
_STOCK_NOW = ("store_id:string product_id:string on_hand:int reserved_qty:int available:int reorder_point:int "
              "updated_at:timestamp")
_DIM_PRODUCT_CURRENT = "product_id:string name:string? category:string? price:decimal(10,2)?"
_OOS_DEMAND = ("minute_start:timestamp store_id:string product_id:string campaign_id:string utm_campaign:string "
               "views:bigint add_to_cart:bigint landing_clicks:bigint oos_at_write:boolean")

_EXPRESS_ORDERS = ("order_id:string store_id:string promised_minutes:int created_at:timestamp delivered_at:timestamp? "
                   "closed_without_delivery:boolean")
_INV_DAILY = ("report_date:date store_id:string product_id:string closing_on_hand:int units_sold:int "
              "units_received:int units_shrunk:int units_adjusted:int oversold_units:int ended_out_of_stock:boolean")
_OOS_DAILY = ("report_date:date store_id:string product_id:string campaign_id:string utm_campaign:string "
              "served_views:bigint served_add_to_cart:bigint served_clicks:bigint oos_views:bigint "
              "oos_add_to_cart:bigint oos_clicks:bigint")
_EXPRESS_DAILY = ("report_date:date store_id:string orders:int delivered:int on_time:int breached:int "
                  "avg_delivery_minutes:decimal(8,2)? max_delivery_minutes:decimal(8,2)?")
_D1_CAMPAIGN = ("report_date:date campaign_id:string utm_campaign:string spend:decimal(12,2)? ad_clicks:bigint? "
                "orders_placed:bigint? revenue_placed:decimal(12,2)? revenue_delivered:decimal(12,2)? cpa:decimal(12,2)? "
                "roas_placed:decimal(12,2)? roas_delivered:decimal(12,2)? served_clicks:bigint oos_clicks:bigint "
                "oos_click_share:decimal(8,4)? exposed_spend:decimal(12,2)? provisional:boolean")
_D1_PRODUCT = ("report_date:date store_id:string product_id:string oos_views:bigint oos_add_to_carts:bigint "
               "revenue_at_risk:decimal(12,2)?")

LIVE_METRIC_COLUMNS = ["events", "active_sessions", "product_views", "add_to_cart", "orders_placed"]
HEALTH_COLUMNS = ["events", "late_events", "quarantined", "silver_batches"]

LIVE_TABLES = [
    Table("live_metrics_minute", "gold", _parse(_LIVE_METRICS, ["minute_start", "country"]),
          ("minute_start", "country")),
    Table("pipeline_health_minute", "gold", _parse(_HEALTH, ["minute_start"]), ("minute_start",)),
    Table("pipeline_batch_log", "gold", _parse(_BATCH_LOG, ["job_run_id", "batch_id"]),
          ("job_run_id", "batch_id")),
    Table("pipeline_heartbeat", "gold", _parse(_HEARTBEAT, ["component"]), ("component",)),
    Table("dim_store", "gold", _parse(_DIM_STORE, ["store_id"]), ("store_id",)),
    Table("dim_store_district", "gold", _parse(_DIM_STORE_DISTRICT, ["city", "district"]), ("city", "district")),
    Table("dim_product_current", "gold", _parse(_DIM_PRODUCT_CURRENT, ["product_id"]), ("product_id",)),
    Table("live_stock_now", "gold", _parse(_STOCK_NOW, ["store_id", "product_id"]), ("store_id", "product_id")),
    Table("live_oos_demand_minute", "gold",
          _parse(_OOS_DEMAND, ["minute_start", "store_id", "product_id", "campaign_id"]),
          ("minute_start", "store_id", "product_id", "campaign_id")),
    Table("live_express_orders", "gold", _parse(_EXPRESS_ORDERS, ["order_id"]), ("order_id",)),
    Table("inventory_daily", "gold", _parse(_INV_DAILY, ["report_date", "store_id", "product_id"]),
          ("report_date", "store_id", "product_id")),
    Table("oos_demand_daily", "gold",
          _parse(_OOS_DAILY, ["report_date", "store_id", "product_id", "campaign_id"]),
          ("report_date", "store_id", "product_id", "campaign_id")),
    Table("express_daily", "gold", _parse(_EXPRESS_DAILY, ["report_date", "store_id"]), ("report_date", "store_id")),
    Table("marketing_d1_campaign", "gold", _parse(_D1_CAMPAIGN, ["report_date", "campaign_id"]),
          ("report_date", "campaign_id")),
    Table("marketing_d1_product", "gold", _parse(_D1_PRODUCT, ["report_date", "store_id", "product_id"]),
          ("report_date", "store_id", "product_id")),
]
LIVE_TABLE_NAMES = [t.name for t in LIVE_TABLES]
