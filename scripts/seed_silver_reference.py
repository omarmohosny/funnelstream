"""Seed silver_products and silver_customers from the simulator catalog (reference data for Silver)."""
import argparse
import os
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


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, default=5000)
    ap.add_argument("--lake", default="data/lake")
    args = ap.parse_args(argv)
    seed = int(os.environ.get("SIM_SEED", "42"))

    from streaming.common.spark import get_spark
    from streaming.silver.spark_schemas import SCHEMAS, coerce_row
    from streaming.silver.writer import SILVER_KEYS, merge_insert, silver_path

    spark = get_spark("seed_silver_reference")
    now = datetime.now(timezone.utc)
    products, custs = catalog_to_silver_rows(make_catalog(seed), make_customers(seed, args.customers), now)
    for table, rows in (("silver_products", products), ("silver_customers", custs)):
        data = [tuple(coerce_row(table, r).values()) for r in rows]
        merge_insert(spark, silver_path(args.lake, table), spark.createDataFrame(data, SCHEMAS[table]),
                     SILVER_KEYS[table])
        print(table, len(rows), "rows offered")
    spark.stop()


if __name__ == "__main__":
    main()
