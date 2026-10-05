"""Apply the DDL through a GoldStore.

Databricks: rendered at run time from warehouse/tables.py for the catalog in DATABRICKS_CATALOG (or
--catalog), plus the upload volume DATABRICKS_VOLUME; run statement by statement.
Postgres: warehouse/ddl.sql run as one script (it holds DO $$ blocks).
"""
import argparse
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from warehouse.ddl_render import render_databricks, split_statements
from warehouse.store import StoreConfigError, _ident, env_catalog, env_volume, get_store
from warehouse.tables import all_tables

POSTGRES_FILE = os.path.join("warehouse", "ddl.sql")


def databricks_sql(catalog, volume):
    return render_databricks(all_tables(), catalog=catalog, volume=volume)


def postgres_sql():
    with open(os.path.join(_ROOT, POSTGRES_FILE), encoding="utf-8") as fh:
        return fh.read()


def main(argv=None):
    ap = argparse.ArgumentParser(description="Create the Gold and Silver-mirror tables (idempotent).")
    ap.add_argument("--backend", choices=["databricks", "postgres"],
                    help="default: GOLD_BACKEND from the environment, else postgres")
    ap.add_argument("--catalog", help="Databricks catalog (default: DATABRICKS_CATALOG, else funnelstream)")
    ap.add_argument("--dry-run", action="store_true", help="print what would run, do not connect")
    args = ap.parse_args(argv)
    backend = args.backend or os.environ.get("GOLD_BACKEND") or "postgres"
    if backend not in ("databricks", "postgres"):
        print("error: unknown backend %r (use databricks or postgres)" % backend, file=sys.stderr)
        return 2
    try:
        if backend == "databricks":
            catalog = _ident(args.catalog) if args.catalog else env_catalog()
            volume = env_volume()
            sql_text = databricks_sql(catalog, volume)
            label = "catalog %s, volume %s" % (catalog, volume)
        else:
            sql_text, label = postgres_sql(), POSTGRES_FILE
    except ValueError as e:
        print("error: %s" % e, file=sys.stderr)
        return 2
    if args.dry_run:
        if backend == "postgres":
            print("postgres: 1 script (%d lines) from %s, run as a whole" % (sql_text.count("\n"), label))
        else:
            statements = split_statements(sql_text)
            print("databricks: %d statements (%s)" % (len(statements), label))
            for i, stmt in enumerate(statements, 1):
                print("  %2d. %s" % (i, " ".join(stmt.split()[:6])))
        return 0
    try:
        with get_store(backend, env=dict(os.environ, DATABRICKS_CATALOG=catalog) if backend == "databricks"
                       else None) as store:
            if backend == "postgres":
                store.execute(sql_text)
                n = 1
            else:
                n = store.run_script(sql_text)
    except StoreConfigError as e:
        print("error: %s" % e, file=sys.stderr)
        return 2
    except Exception as e:
        print("error: DDL failed (%s): %s" % (type(e).__name__, " ".join(str(e).split())[:300]), file=sys.stderr)
        return 1
    print("applied %d %s (%s)" % (n, "script" if backend == "postgres" else "statements", label))
    return 0


if __name__ == "__main__":
    sys.exit(main())
