import json
import random
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from pyspark.sql.types import BooleanType, DateType, DecimalType, IntegerType, StringType, TimestampType

from scripts.run_simulator import iter_batch
from simulator.catalog import make_catalog, make_customers
from streaming.silver.schemas import TABLE_COLUMNS
from streaming.silver.spark_schemas import ALL_COLUMNS, SCHEMAS, coerce_row
from streaming.silver.transform import transform_message

START = datetime(2026, 9, 27, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 20, tzinfo=timezone.utc)
CATALOG = make_catalog(3)


def test_schema_names_equal_columns():
    for table, cols in TABLE_COLUMNS.items():
        assert SCHEMAS[table].names == cols
    assert SCHEMAS["silver_products"].names == ["product_id", "updated_at", "name", "category", "price", "is_active"]
    assert SCHEMAS["silver_customers"].names == ["customer_id", "updated_at", "country", "city", "signup_date",
                                                 "primary_device", "marketing_consent"]
    assert set(SCHEMAS) == set(ALL_COLUMNS)


def test_schema_types():
    t = {n: {f.name: f.dataType for f in s.fields} for n, s in SCHEMAS.items()}
    assert isinstance(t["silver_events"]["event_date"], DateType)
    assert isinstance(t["silver_events"]["event_time"], TimestampType)
    assert isinstance(t["silver_events"]["dwell_seconds"], IntegerType)
    assert isinstance(t["silver_events"]["is_late"], BooleanType)
    assert isinstance(t["silver_events"]["city"], StringType)
    assert t["silver_orders"]["order_total"] == DecimalType(12, 2)
    assert isinstance(t["silver_order_status"]["status_time"], TimestampType)
    assert isinstance(t["silver_order_items"]["line_no"], IntegerType)
    assert isinstance(t["silver_order_items"]["quantity"], IntegerType)
    assert t["silver_order_items"]["line_total"] == DecimalType(12, 2)
    assert isinstance(t["silver_order_items"]["order_time"], TimestampType)
    assert t["silver_products"]["price"] == DecimalType(10, 2)


def test_coerce_row_converts_transform_output():
    rng = random.Random(4)
    seen = set()
    for hint, e in iter_batch(rng, make_customers(3, 200), CATALOG, START, max_events=1500):
        kts = datetime.fromisoformat(e["event_time"]) + timedelta(seconds=1)
        for table, row in transform_message(json.dumps(e), "x", kts, NOW, known_products=None, now=NOW):
            out = coerce_row(table, row)
            assert list(out) == SCHEMAS[table].names
            seen.add(table)
            for f in SCHEMAS[table].fields:
                v = out[f.name]
                if v is None:
                    continue
                if isinstance(f.dataType, TimestampType):
                    assert isinstance(v, datetime) and v.tzinfo is None
                elif isinstance(f.dataType, DateType):
                    assert type(v) is date
                elif isinstance(f.dataType, DecimalType):
                    assert isinstance(v, Decimal)
                elif isinstance(f.dataType, IntegerType):
                    assert type(v) is int
                elif isinstance(f.dataType, BooleanType):
                    assert type(v) is bool
                else:
                    assert isinstance(v, str)
    assert seen == set(TABLE_COLUMNS) - {"silver_quarantine"}


def test_coerce_row_values():
    row = {"event_time": "2026-10-01T22:30:00+02:00", "event_date": "2026-10-02", "is_late": 1,
           "dwell_seconds": "7", "kafka_timestamp": datetime(2026, 10, 1, 12, tzinfo=timezone(timedelta(hours=3)))}
    out = coerce_row("silver_events", row)
    assert out["event_time"] == datetime(2026, 10, 1, 20, 30)
    assert out["kafka_timestamp"] == datetime(2026, 10, 1, 9)
    assert out["event_date"] == date(2026, 10, 2)
    assert out["is_late"] is True and out["dwell_seconds"] == 7
    assert out["event_id"] is None
    assert coerce_row("silver_orders", {"order_total": 12.5})["order_total"] == Decimal("12.50")
    assert coerce_row("silver_customers", {"signup_date": "2025-03-04"})["signup_date"] == date(2025, 3, 4)


def test_catalog_to_silver_rows_has_no_pii_and_normalized_city():
    from datetime import datetime, timezone
    from scripts.seed_silver_reference import catalog_to_silver_rows
    from streaming.silver.spark_schemas import SILVER_CUSTOMERS_COLUMNS, SILVER_PRODUCTS_COLUMNS

    customers = make_customers(3, 50)
    customers[0]["city"] = " CAIRO "
    now = datetime(2026, 10, 1, tzinfo=timezone.utc)
    products, custs = catalog_to_silver_rows(CATALOG, customers, now)
    assert len(products) == len(CATALOG) and len(custs) == 50
    assert list(products[0]) == SILVER_PRODUCTS_COLUMNS and list(custs[0]) == SILVER_CUSTOMERS_COLUMNS
    assert all("name" not in c and "phone" not in c for c in custs)
    assert custs[0]["city"] == "Cairo"
    assert all(isinstance(p["price"], Decimal) for p in products)
    assert custs[1]["primary_device"] in ("mobile", "desktop", "tablet")
    for table, rows in (("silver_products", products), ("silver_customers", custs)):
        assert list(coerce_row(table, rows[0])) == SCHEMAS[table].names
