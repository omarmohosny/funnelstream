"""Writes modeling/metadata/silver_catalog.json: the metadata JSON the modeler (baseline or LLM) reads.

Column lists come from streaming/silver/schemas.py, so the catalog cannot drift from the real Silver tables.
Only table and column names, types, keys and one-line meanings are exported: never data rows.

    python -m modeling.export_catalog
"""
import json
from pathlib import Path

from streaming.silver.schemas import TABLE_COLUMNS

OUT = Path(__file__).parent / "metadata" / "silver_catalog.json"

TS, DATE, BOOL, INT, MONEY = "timestamp", "date", "boolean", "int", "decimal(12,2)"
TYPES = {
    "silver_events": {"event_time": TS, "kafka_timestamp": TS, "is_late": BOOL, "is_bot": BOOL, "dwell_seconds": INT,
                      "event_date": DATE},
    "silver_orders": {"order_total": MONEY, "order_created_at": TS, "event_date": DATE},
    "silver_order_status": {"status_rank": INT, "status_time": TS, "event_date": DATE},
    "silver_order_items": {"line_no": INT, "quantity": INT, "unit_price": MONEY, "line_total": MONEY, "order_time": TS},
    "silver_quarantine": {"received_at": TS},
    "silver_products": {"updated_at": TS, "price": "decimal(10,2)", "is_active": BOOL},
    "silver_customers": {"updated_at": TS, "signup_date": DATE, "marketing_consent": BOOL},
    "silver_campaign_snapshots": {"report_date": DATE, "pulled_at": TS, "impressions": "bigint", "clicks": "bigint",
                                  "platform_conversions": "bigint", "spend": "decimal(12,4)"},
}
COLUMNS = dict(TABLE_COLUMNS)
COLUMNS["silver_products"] = ["product_id", "updated_at", "name", "category", "price", "is_active"]
COLUMNS["silver_customers"] = ["customer_id", "updated_at", "country", "city", "district", "signup_date",
                               "primary_device", "marketing_consent"]
COLUMNS["silver_campaign_snapshots"] = ["campaign_id", "report_date", "pulled_at", "utm_campaign", "ad_platform",
                                        "impressions", "clicks", "platform_conversions", "spend", "currency"]
KEYS = {
    "silver_events": ["event_id"], "silver_orders": ["order_id"], "silver_order_status": ["event_id"],
    "silver_order_items": ["order_id", "line_no"], "silver_quarantine": ["quarantine_id"],
    "silver_products": ["product_id", "updated_at"], "silver_customers": ["customer_id", "updated_at"],
    "silver_campaign_snapshots": ["campaign_id", "report_date", "pulled_at"],
}
MEANING = {
    "silver_events": "clickstream events, deduplicated on event_id; is_late and is_bot flags; event_date is the Africa/Cairo date",
    "silver_orders": "one row per order from the order_created event; order_total in EGP",
    "silver_order_status": "order and delivery status changes; current status is the highest status_rank (tie: latest status_time)",
    "silver_order_items": "one row per order line",
    "silver_quarantine": "rejected rows with error_code; never joined into facts",
    "silver_products": "product versions (updated_at) for the SCD2 product dimension",
    "silver_customers": "customer versions (updated_at) for the SCD2 customer dimension",
    "silver_campaign_snapshots": "ad API pulls; the latest pull per campaign and report_date wins",
}
BUSINESS_QUESTIONS = [
    "Spend, CPA and ROAS per campaign per day (delivered ROAS is the headline)",
    "Conversion and cart/checkout abandonment per channel and platform, split by payment method (COD vs prepaid)",
    "Funnel from product view to cart, checkout, order and delivery, with drop-off per step",
    "Cancellation, failed-delivery rate and delivery lead time per carrier and city",
    "Products browsed and bought per city; next-product recommendations (labelled co-view)",
]


def build() -> dict:
    tables = []
    for name, cols in COLUMNS.items():
        types = TYPES.get(name, {})
        tables.append({"name": name, "description": MEANING[name], "primary_key": KEYS[name],
                       "columns": [{"name": c, "type": types.get(c, "string")} for c in cols]})
    return {"layer": "silver", "tables": tables, "business_questions": BUSINESS_QUESTIONS,
            "notes": ["Gold holds no names or phone numbers.",
                      "SCD2 dimensions: valid_from = updated_at, valid_to = next updated_at, is_current on the last row.",
                      "Dimension joins are written as @gold.<table>."]}


if __name__ == "__main__":
    OUT.write_text(json.dumps(build(), indent=2) + "\n", encoding="utf-8")
    print("wrote", OUT)
