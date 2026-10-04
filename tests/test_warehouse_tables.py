"""Task 10: Gold and Silver-mirror DDL generated from one table spec (warehouse/tables.py).

Intentional differences between the design spec (section 7) and tables.py:
  * The spec's Columns cell omits the key columns for facts and dimensions; tables.py lists the keys as columns
    too, so the comparison is: spec Columns + spec Key columns == table columns (set equality).
  * live_metrics_minute is in the spec table but is local serving, not Gold (spec 16.1/brief): excluded.
"""
import os
import re
import uuid
from pathlib import Path

import pytest

from warehouse import ddl_render
from warehouse.tables import (GOLD_TABLES, SILVER_MIRROR_TABLES, Column, all_tables, gold_table)

ROOT = Path(__file__).resolve().parent.parent
SPEC = ROOT / "docs" / "superpowers" / "specs" / "2026-10-04-funnelstream-design.md"
EXPECTED_ORDER = ["dim_customer", "dim_product", "dim_campaign", "dim_date", "fact_orders", "fact_order_lines",
                  "fact_funnel_daily", "fact_attribution", "fact_campaign_daily", "fact_product_recs",
                  "fact_city_product_daily", "fact_customer_360"]


def _cells(cell):
    cell = re.sub(r"\([^)]*\)", "", cell)
    out = [c.strip().strip("`").strip() for c in cell.split(",")]
    return [c for c in out if c]


def _spec_gold_rows():
    text = SPEC.read_text(encoding="utf-8")
    section = text.split("### Gold (business-ready)")[1].split("**Stretch")[0]
    rows = {}
    for line in section.splitlines():
        m = re.match(r"\|\s*`(\w+)`", line)
        if not m:
            continue
        parts = [p.strip() for p in line.strip().strip("|").split("|")]
        rows[m.group(1)] = {"key": _cells(parts[3]), "columns": _cells(parts[4])}
    return rows


# group 1
def test_gold_table_list_and_order():
    assert [t.name for t in GOLD_TABLES] == EXPECTED_ORDER


def test_gold_columns_match_spec_section_7():
    spec = _spec_gold_rows()
    spec.pop("live_metrics_minute")
    assert list(spec) == EXPECTED_ORDER
    compared = 0
    for name, row in spec.items():
        t = gold_table(name)
        want = set(row["key"]) | set(row["columns"])
        assert set(t.column_names()) == want, name
        assert len(t.column_names()) == len(set(t.column_names())), name
        compared += len(want)
    assert compared == 142  # spec columns + keys across the 12 tables
    for name in ("dim_customer", "dim_product"):
        assert {"valid_from", "valid_to", "is_current"} <= set(gold_table(name).column_names())


# group 2
MONEY = {"order_total", "line_total", "price", "spend", "revenue", "revenue_placed", "revenue_delivered", "aov",
         "cpa", "roas_placed", "roas_delivered", "total_spend", "avg_order_value", "unit_price"}
RATES = {"cancel_rate", "failed_delivery_rate", "ctr", "conversion_rate", "direct_share", "cart_abandon_rate",
         "checkout_abandon_rate"}


def test_no_float_money_decimal_no_live_metrics_identity_only_on_dims():
    for t in all_tables():
        for c in t.columns:
            assert "float" not in c.type.lower() and "double" not in c.type.lower(), (t.name, c.name)
    for t in GOLD_TABLES:
        for c in t.columns:
            if c.name in MONEY or c.name in RATES:
                assert c.type.startswith("decimal("), (t.name, c.name)
    assert gold_table("fact_campaign_daily").column("spend").type == "decimal(12,4)"
    assert gold_table("fact_orders").column("order_total").type == "decimal(12,2)"
    assert gold_table("fact_campaign_daily").column("ctr").type == "decimal(8,4)"
    assert "live_metrics_minute" not in [t.name for t in all_tables()]
    identity = {(t.name, c.name) for t in all_tables() for c in t.columns if c.identity}
    assert identity == {("dim_customer", "customer_key"), ("dim_product", "product_key"),
                        ("dim_campaign", "campaign_key")}
    for t in GOLD_TABLES:
        for c in t.columns:
            if c.identity:
                assert c.type == "bigint"
    assert gold_table("dim_date").column("date_key").type == "int"
    with pytest.raises(ValueError):
        Column("x", "float")


# group 3
def test_keys_exist_and_match_spec():
    spec = _spec_gold_rows()
    for t in all_tables():
        assert set(t.keys) <= set(t.column_names()), t.name
    for name in EXPECTED_ORDER:
        assert list(gold_table(name).keys) == spec[name]["key"], name
    for dim, sk in (("dim_customer", "customer_key"), ("dim_product", "product_key"),
                    ("dim_campaign", "campaign_key")):
        assert gold_table(dim).keys == (sk,)
    assert gold_table("dim_date").keys == ("date_key",)


# group 4
def test_silver_mirror_matches_spark_schemas():
    from streaming.silver.spark_schemas import SCHEMAS
    names = [t.name for t in SILVER_MIRROR_TABLES]
    assert names == ["silver_events", "silver_orders", "silver_order_status", "silver_order_items",
                     "silver_quarantine", "silver_products", "silver_customers", "silver_campaign_snapshots"]
    assert len(SCHEMAS) == 7
    for name, schema in SCHEMAS.items():
        t = next(x for x in SILVER_MIRROR_TABLES if x.name == name)
        assert t.column_names() == [f.name for f in schema.fields], name
    snap = next(x for x in SILVER_MIRROR_TABLES if x.name == "silver_campaign_snapshots")
    assert snap.column_names() == ["campaign_id", "report_date", "pulled_at", "utm_campaign", "platform",
                                   "impressions", "clicks", "spend", "currency"]
    assert snap.column("spend").type == "decimal(12,4)"
    ev = next(x for x in SILVER_MIRROR_TABLES if x.name == "silver_events")
    assert ev.column("event_time").type == "timestamp" and ev.column("event_date").type == "date"
    assert ev.column("dwell_seconds").type == "int" and ev.column("is_late").type == "boolean"
    assert {t.name for t in SILVER_MIRROR_TABLES if t.partition == "event_date"} == {
        "silver_events", "silver_orders", "silver_order_status"}


# group 5
def test_postgres_render():
    sql = ddl_render.render_postgres(all_tables())
    for t in GOLD_TABLES:
        assert "CREATE TABLE IF NOT EXISTS %s (" % t.name in sql
    assert "live_metrics_minute" not in sql
    assert "silver_events" not in sql
    assert "analyst" in sql and "engineer" in sql and "NOLOGIN" in sql
    assert sql.count("DO $$") == 2 and "IF NOT EXISTS" in sql
    assert "GENERATED ALWAYS AS IDENTITY" in sql and "timestamptz" in sql and "numeric(12,2)" in sql
    assert "UNIQUE (customer_id, valid_from)" in sql and "UNIQUE (product_id, valid_from)" in sql
    assert "GENERATED FILE, edit warehouse/tables.py and run scripts/gen_ddl.py" in sql
    for bad in (r"USING DELTA", r"PARTITIONED BY", r"\bSTRING\b", r"\bDECIMAL\b", r"funnelstream\.gold"):
        assert not re.search(bad, sql, re.I), bad


# group 6
FORBIDDEN = [r"\bSERIAL\b", r"\bBIGSERIAL\b", r"\bUNIQUE\b", r"ON\s+CONFLICT", r"\bTIMESTAMPTZ\b", r"\bTEXT\b",
             r"::", r"PRIMARY\s+KEY", r"\bINFINITY\b"]


def _statements(sql):
    body = "\n".join(l for l in sql.splitlines() if not l.startswith("--"))
    return [b.strip() for b in re.split(r"\n\s*\n", body) if b.strip()]


def test_databricks_render():
    tables = all_tables()
    sql = ddl_render.render_databricks(tables)
    for pat in FORBIDDEN:
        assert not re.search(pat, sql, re.I), pat
    for tok in ("USING DELTA", "DECIMAL(12,2)", "GENERATED ALWAYS AS IDENTITY",
                "CREATE SCHEMA IF NOT EXISTS funnelstream.gold;", "CREATE SCHEMA IF NOT EXISTS funnelstream.silver;"):
        assert tok in sql, tok
    assert ddl_render.CATALOG == "funnelstream"
    assert "live_metrics_minute" not in sql
    for t in tables:
        assert "CREATE TABLE IF NOT EXISTS funnelstream.%s.%s (" % (t.schema_group, t.name) in sql
    for name in ("silver_events", "silver_orders", "silver_order_status"):
        blk = next(s for s in _statements(sql) if ".%s (" % name in s)
        assert "PARTITIONED BY (event_date)" in blk
    assert sql.count("PARTITIONED BY (event_date)") == 3
    for s in _statements(sql):
        assert s.endswith(";"), s[:60]
    assert "GENERATED FILE, edit warehouse/tables.py and run scripts/gen_ddl.py" in sql
    blk = next(s for s in _statements(sql) if ".dim_customer (" in s)
    assert "customer_key BIGINT GENERATED ALWAYS AS IDENTITY" in blk and "COMMENT 'keys: customer_key" in blk
    assert "event_id STRING," in next(s for s in _statements(sql) if ".silver_events (" in s)


# group 7
def test_committed_ddl_files_match_generator(tmp_path):
    from scripts import gen_ddl
    assert gen_ddl.main(["--check"]) == 0
    assert gen_ddl.main(["--out-dir", str(tmp_path)]) == 0
    assert gen_ddl.main(["--check", "--out-dir", str(tmp_path)]) == 0
    for name in ("ddl.sql", "databricks_ddl.sql"):
        data = (tmp_path / name).read_bytes()
        assert b"\r" not in data
        assert data == (ROOT / "warehouse" / name).read_bytes()
    p = tmp_path / "databricks_ddl.sql"
    p.write_bytes(p.read_bytes() + b"-- drift\n")
    assert gen_ddl.main(["--check", "--out-dir", str(tmp_path)]) != 0


# group 8
@pytest.mark.integration
def test_postgres_ddl_applies_twice_and_roles():
    dsn = os.environ.get("PG_DSN")
    if not dsn:
        pytest.skip("PG_DSN not set")
    psycopg = pytest.importorskip("psycopg")
    try:
        conn = psycopg.connect(dsn, autocommit=True, connect_timeout=3)
    except Exception as exc:
        pytest.skip("no Postgres at PG_DSN: %s" % exc)
    schema = "t10_" + uuid.uuid4().hex[:8]
    ddl = (ROOT / "warehouse" / "ddl.sql").read_text(encoding="utf-8")
    try:
        conn.execute("CREATE SCHEMA %s" % schema)
        conn.execute("SET search_path TO %s" % schema)
        conn.execute(ddl)
        conn.execute(ddl)  # idempotent
        rows = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=%s", (schema,))
        have = {r[0] for r in rows}
        assert len(have & set(EXPECTED_ORDER)) == 12
        for t in GOLD_TABLES:
            pk = [r[0] for r in conn.execute(
                "SELECT kcu.column_name FROM information_schema.table_constraints tc "
                "JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name "
                "AND tc.table_schema=kcu.table_schema WHERE tc.table_schema=%s AND tc.table_name=%s "
                "AND tc.constraint_type='PRIMARY KEY' ORDER BY kcu.ordinal_position", (schema, t.name))]
            assert pk == list(t.keys), t.name
        ins = ("INSERT INTO dim_customer (customer_id, valid_from, valid_to, is_current) "
               "VALUES (%s, now(), '9999-12-31 00:00:00+00', true) RETURNING customer_key")
        k1 = conn.execute(ins, ("c1",)).fetchone()[0]
        k2 = conn.execute(ins, ("c2",)).fetchone()[0]
        assert k2 == k1 + 1
        conn.execute("GRANT USAGE ON SCHEMA %s TO analyst" % schema)
        conn.execute("SET ROLE analyst")
        try:
            assert conn.execute("SELECT count(*) FROM dim_customer").fetchone()[0] == 2
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute("INSERT INTO dim_customer (customer_id, valid_from, valid_to, is_current) "
                             "VALUES ('x', now(), now(), true)")
        finally:
            conn.execute("RESET ROLE")
    finally:
        conn.execute("RESET ROLE")
        conn.execute("DROP SCHEMA IF EXISTS %s CASCADE" % schema)
        conn.close()
