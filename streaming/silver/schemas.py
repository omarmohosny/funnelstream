"""Silver table column lists (spec section 7 and section 16.4)."""

SILVER_EVENTS_COLUMNS = [
    "event_id", "event_type", "event_time", "kafka_timestamp", "is_late", "is_bot", "event_version",
    "session_id", "customer_id", "device", "channel", "platform", "country", "city", "district",
    "campaign_id", "utm_source", "utm_campaign", "utm_medium", "click_id", "page_url", "product_id", "element", "dwell_seconds", "cart_id", "order_id",
    "search_term", "event_date",
]
SILVER_ORDERS_COLUMNS = [
    "order_id", "customer_id", "session_id", "channel", "platform", "utm_source", "utm_campaign", "utm_medium",
    "click_id", "payment_method", "city", "district", "country", "order_total", "order_created_at", "event_date",
]
SILVER_ORDER_STATUS_COLUMNS = [
    "event_id", "order_id", "customer_id", "status", "status_rank", "status_time", "payment_method",
    "carrier", "reason", "event_date",
]
SILVER_ORDER_ITEMS_COLUMNS = [
    "order_id", "line_no", "product_id", "quantity", "unit_price", "line_total", "currency", "order_time",
]
SILVER_QUARANTINE_COLUMNS = [
    "quarantine_id", "raw_payload", "source_topic", "error_code", "error_detail", "received_at",
]
SILVER_STOCK_MOVEMENTS_COLUMNS = [
    "event_id", "store_id", "product_id", "qty_delta", "reason", "order_id", "requested_qty", "event_time",
    "kafka_timestamp", "event_date",
]
SILVER_STOCK_SNAPSHOTS_COLUMNS = [
    "event_id", "store_id", "product_id", "on_hand", "reserved_qty", "reorder_point", "event_time",
    "kafka_timestamp", "event_date",
]
SILVER_EXPRESS_ORDERS_COLUMNS = ["order_id", "store_id", "promised_minutes", "created_at", "event_date"]
TABLE_COLUMNS = {
    "silver_events": SILVER_EVENTS_COLUMNS,
    "silver_orders": SILVER_ORDERS_COLUMNS,
    "silver_order_status": SILVER_ORDER_STATUS_COLUMNS,
    "silver_order_items": SILVER_ORDER_ITEMS_COLUMNS,
    "silver_quarantine": SILVER_QUARANTINE_COLUMNS,
    "silver_stock_movements": SILVER_STOCK_MOVEMENTS_COLUMNS,
    "silver_stock_snapshots": SILVER_STOCK_SNAPSHOTS_COLUMNS,
    "silver_express_orders": SILVER_EXPRESS_ORDERS_COLUMNS,
}
ERROR_CODES = [
    "BAD_JSON", "MISSING_FIELD", "UNKNOWN_EVENT_TYPE", "BAD_VERSION", "NEGATIVE_PRICE", "FUTURE_TIMESTAMP",
    "BAD_VALUE", "UNKNOWN_PRODUCT", "CANCEL_AFTER_SHIPMENT", "NEGATIVE_SPEND", "CLICKS_GT_IMPRESSIONS",
]
LATE_AFTER_SECONDS = 600
CURRENCY = "EGP"
