"""Spark schemas for the Silver Delta tables and the Python-to-Spark row coercion.

Timestamps are TimestampType. Python values handed to Spark are NAIVE datetimes holding UTC wall
time; the Spark container runs with TZ=UTC (and spark.sql.session.timeZone=UTC), so they round-trip
unchanged. event_date and signup_date are DateType, money is Decimal(12,2) (product price Decimal(10,2)).
"""
from datetime import date, datetime, timezone
from decimal import Decimal

from pyspark.sql.types import (BooleanType, DateType, DecimalType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

from streaming.silver.schemas import TABLE_COLUMNS

SILVER_PRODUCTS_COLUMNS = ["product_id", "updated_at", "name", "category", "price", "is_active"]
SILVER_CUSTOMERS_COLUMNS = ["customer_id", "updated_at", "country", "city", "district", "signup_date", "primary_device",
                            "marketing_consent"]

_TS = TimestampType()
_DATE = DateType()
_MONEY = DecimalType(12, 2)
_INT = IntegerType()
_BOOL = BooleanType()

_TYPES = {
    "silver_events": {"event_time": _TS, "kafka_timestamp": _TS, "is_late": _BOOL, "is_bot": _BOOL,
                      "dwell_seconds": _INT, "event_date": _DATE},
    "silver_orders": {"order_total": _MONEY, "order_created_at": _TS, "event_date": _DATE},
    "silver_order_status": {"status_rank": _INT, "status_time": _TS, "event_date": _DATE},
    "silver_order_items": {"line_no": _INT, "quantity": _INT, "unit_price": _MONEY, "line_total": _MONEY,
                           "order_time": _TS},
    "silver_quarantine": {"received_at": _TS},
    "silver_products": {"updated_at": _TS, "price": DecimalType(10, 2), "is_active": _BOOL},
    "silver_customers": {"updated_at": _TS, "signup_date": _DATE, "marketing_consent": _BOOL},
}
ALL_COLUMNS = dict(TABLE_COLUMNS)
ALL_COLUMNS["silver_products"] = SILVER_PRODUCTS_COLUMNS
ALL_COLUMNS["silver_customers"] = SILVER_CUSTOMERS_COLUMNS

# Every field is nullable: bad rows are quarantined upstream.
SCHEMAS = {
    table: StructType([StructField(c, _TYPES[table].get(c, StringType()), True) for c in cols])
    for table, cols in ALL_COLUMNS.items()
}


def silver_schema(table: str) -> StructType:
    return SCHEMAS[table]


def _to_datetime(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.strip())
    if not isinstance(value, datetime):
        raise TypeError("not a timestamp: %r" % (value,))
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _to_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip()[:10])


def _coerce(value, dtype):
    if value is None:
        return None
    if isinstance(dtype, TimestampType):
        return _to_datetime(value)
    if isinstance(dtype, DateType):
        return _to_date(value)
    if isinstance(dtype, DecimalType):
        q = Decimal(1).scaleb(-dtype.scale)
        return (value if isinstance(value, Decimal) else Decimal(str(value))).quantize(q)
    if isinstance(dtype, IntegerType):
        return int(value)
    if isinstance(dtype, BooleanType):
        return bool(value)
    return value if isinstance(value, str) else str(value)


def coerce_row(table: str, row: dict) -> dict:
    """Row dict with values converted to the Python types of the table's Spark schema, in schema order."""
    return {f.name: _coerce(row.get(f.name), f.dataType) for f in SCHEMAS[table].fields}
