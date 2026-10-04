"""Seed silver_products and silver_customers from the simulator catalog (reference data for Silver)."""
import argparse
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
from datetime import datetime, timezone
from decimal import Decimal

from contracts.normalize import normalize_city, normalize_device
from simulator.catalog import make_catalog, make_customers

_CENT = Decimal("0.01")


def catalog_to_silver_rows(catalog, customers, updated_at):
    """(product_rows, customer_rows) as dicts in silver column order. No names or phones."""
    products = [{
        "product_id": p["product_id"], "updated_at": updated_at, "name": p["name"], "category": p["category"],
        "price": Decimal(str(p["price"])).quantize(_CENT), "is_active": bool(p["is_active"]),
    } for p in catalog]
    custs = [{
        "customer_id": c["customer_id"], "updated_at": updated_at, "country": c["country"],
        "city": normalize_city(c["city"]), "signup_date": c["signup_date"],
        "primary_device": normalize_device(c["device_primary"]),
        "marketing_consent": bool(c["marketing_consent"]),
    } for c in customers]
    return products, custs


DEFAULT_UPDATED_AT = "2025-01-01T00:00:00+00:00"
_ATTRS = {
    "silver_products": ("product_id", ["name", "category", "price", "is_active"]),
    "silver_customers": ("customer_id", ["country", "city", "signup_date", "primary_device", "marketing_consent"]),
}


def _only_changed(spark, path, df, table):
    """Keep new rows whose attributes differ from the latest stored version of the same id."""
    from delta.tables import DeltaTable
    from pyspark.sql import Window, functions as F

    if not DeltaTable.isDeltaTable(spark, path):
        return df
    id_col, attrs = _ATTRS[table]
    w = Window.partitionBy(id_col).orderBy(F.col("updated_at").desc())
    latest = (spark.read.format("delta").load(path).withColumn("_rn", F.row_number().over(w))
              .where("_rn = 1").select(id_col, *attrs))
    cond = (df[id_col] == latest[id_col])
    for a in attrs:
        cond = cond & df[a].eqNullSafe(latest[a])
    return df.join(latest, cond, "left_anti")


def seed(spark, lake, updated_at, catalog, customers):
    """Write reference rows; idempotent. A new version (new updated_at) is added only for changed rows."""
    from streaming.silver.spark_schemas import SCHEMAS, coerce_row
    from streaming.silver.writer import SILVER_KEYS, merge_insert, silver_path

    lake = os.path.abspath(lake)
    products, custs = catalog_to_silver_rows(catalog, customers, updated_at)
    for table, rows in (("silver_products", products), ("silver_customers", custs)):
        path = silver_path(lake, table)
        df = spark.createDataFrame([tuple(coerce_row(table, r).values()) for r in rows], SCHEMAS[table])
        merge_insert(spark, path, _only_changed(spark, path, df, table), SILVER_KEYS[table])
    return len(products), len(custs)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, default=5000)
    ap.add_argument("--lake", default="data/lake")
    ap.add_argument("--updated-at", default=os.environ.get("SEED_UPDATED_AT", DEFAULT_UPDATED_AT))
    args = ap.parse_args(argv)
    seed_value = int(os.environ.get("SIM_SEED", "42"))
    updated_at = datetime.fromisoformat(args.updated_at)
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)

    from streaming.common.spark import get_spark

    spark = get_spark("seed_silver_reference")
    n_p, n_c = seed(spark, args.lake, updated_at, make_catalog(seed_value),
                    make_customers(seed_value, args.customers))
    print("silver_products", n_p, "rows offered; silver_customers", n_c, "rows offered")
    spark.stop()


if __name__ == "__main__":
    main()
