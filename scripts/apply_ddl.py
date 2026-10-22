"""Apply the DDL through a GoldStore.

Databricks: rendered at run time from warehouse/tables.py for the catalog in DATABRICKS_CATALOG (or
--catalog), plus the upload volume DATABRICKS_VOLUME; run statement by statement.
Postgres: warehouse/ddl.sql run as one script (it holds DO $$ blocks).
After the CREATE statements, both backends run the idempotent unknown-member seeds (key -1 in dim_customer,
dim_product, dim_campaign; spec section 17, rule 2).
"""
import argparse
import os
import re
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from warehouse.ddl_render import add_columns_statement, render_databricks, split_statements
from warehouse.store import StoreConfigError, _ident, env_catalog, env_volume, get_store
from warehouse.tables import all_tables
from warehouse.unknown_members import seed_statements

POSTGRES_FILE = os.path.join("warehouse", "ddl.sql")


def databricks_sql(catalog, volume):
    return render_databricks(all_tables(), catalog=catalog, volume=volume)


def postgres_sql():
    with open(os.path.join(_ROOT, POSTGRES_FILE), encoding="utf-8") as fh:
        return fh.read()


_CREATE_TABLE_RE = re.compile(r"^CREATE TABLE IF NOT EXISTS (\w+)\.(\w+)\.(\w+) \(")


def parse_recreate(value):
    """'a,b' to a list of table names, each checked against all_tables(). Raises ValueError."""
    known = {t.name for t in all_tables()}
    names = [n.strip() for n in value.split(",") if n.strip()]
    if not names:
        raise ValueError("--recreate needs at least one table name")
    for n in names:
        if n not in known:
            raise ValueError("unknown table %r for --recreate (known: %s)" % (n, ", ".join(sorted(known))))
    return list(dict.fromkeys(names))


def with_drops(statements, tables):
    """Insert DROP TABLE IF EXISTS before the CREATE TABLE of every table in `tables`."""
    out = []
    for stmt in statements:
        m = _CREATE_TABLE_RE.match(stmt)
        if m and m.group(3) in tables:
            out.append("DROP TABLE IF EXISTS %s.%s.%s;" % m.groups())
        out.append(stmt)
    return out


def existing_columns(store, catalog, table):
    """Column names of an existing Databricks table (DESCRIBE TABLE rows up to the first blank or # row)."""
    names = []
    for row in store.query("DESCRIBE TABLE %s.%s.%s" % (catalog, table.schema_group, table.name)):
        if not isinstance(row[0], str):  # not a DESCRIBE answer: nothing is known about the table
            return []
        name = row[0].strip()
        if not name or name.startswith("#"):
            break
        names.append(name)
    return names


def migrate_columns(store, catalog, tables=None):
    """Add the columns a table lacks (CREATE TABLE IF NOT EXISTS never changes an existing table). Only adds: a column is
    never dropped or retyped here. Returns the ALTER statements that ran."""
    ran = []
    for t in (all_tables() if tables is None else tables):
        have = {c.lower() for c in existing_columns(store, catalog, t)}
        missing = [c for c in t.columns if c.name.lower() not in have and not c.identity]
        if have and missing:
            stmt = add_columns_statement(t, missing, catalog)
            store.execute(stmt)
            ran.append(stmt)
    return ran


def main(argv=None):
    ap = argparse.ArgumentParser(description="Create the Gold and Silver-mirror tables (idempotent).")
    ap.add_argument("--backend", choices=["databricks", "postgres"],
                    help="default: GOLD_BACKEND from the environment, else postgres")
    ap.add_argument("--catalog", help="Databricks catalog (default: DATABRICKS_CATALOG, else funnelstream)")
    ap.add_argument("--dry-run", action="store_true", help="print what would run, do not connect")
    ap.add_argument("--recreate", metavar="TABLE[,TABLE...]",
                    help="Databricks only: DROP these tables (data is lost) before their CREATE, for a schema "
                         "change that CREATE IF NOT EXISTS cannot make. Without --yes only prints the plan.")
    ap.add_argument("--yes", action="store_true", help="really execute the --recreate drops")
    args = ap.parse_args(argv)
    backend = args.backend or os.environ.get("GOLD_BACKEND") or "postgres"
    if backend not in ("databricks", "postgres"):
        print("error: unknown backend %r (use databricks or postgres)" % backend, file=sys.stderr)
        return 2
    recreate = []
    if args.recreate is not None:
        if backend != "databricks":
            print("error: --recreate works with the databricks backend only", file=sys.stderr)
            return 2
        try:
            recreate = parse_recreate(args.recreate)
        except ValueError as e:
            print("error: %s" % e, file=sys.stderr)
            return 2
    try:
        if backend == "databricks":
            catalog = _ident(args.catalog) if args.catalog else env_catalog()
            volume = env_volume()
            sql_text = databricks_sql(catalog, volume)
            seeds = seed_statements("databricks", catalog)
            label = "catalog %s, volume %s" % (catalog, volume)
            if recreate:
                statements = with_drops(split_statements(sql_text), recreate) + seeds
                drops = [x for x in statements if x.startswith("DROP TABLE")]
                print("WARNING: --recreate DROPS these tables and ALL their data before creating them again: %s"
                      % ", ".join(recreate))
                print("WARNING: republish Silver afterwards (publish_silver) and rebuild Gold.")
                for d in drops:
                    print("  plan: %s" % d)
                if args.dry_run or not args.yes:
                    print("DRY RUN: nothing was dropped or created. Repeat with --yes to execute.")
                    return 0 if args.dry_run else 2
        else:
            sql_text, label = postgres_sql(), POSTGRES_FILE
            seeds = seed_statements("postgres", None)
    except ValueError as e:
        print("error: %s" % e, file=sys.stderr)
        return 2
    if args.dry_run:
        if backend == "postgres":
            print("postgres: 1 script (%d lines) from %s, run as a whole, then %d seed statements"
                  % (sql_text.count("\n"), label, len(seeds)))
            for stmt in seeds:
                print("      seed: %s" % " ".join(stmt.split()[:6]))
        else:
            statements = split_statements(sql_text) + seeds
            print("databricks: %d statements (%s)" % (len(statements), label))
            for i, stmt in enumerate(statements, 1):
                print("  %2d. %s" % (i, " ".join(stmt.split()[:6])))
        return 0
    try:
        with get_store(backend, env=dict(os.environ, DATABRICKS_CATALOG=catalog) if backend == "databricks"
                       else None) as store:
            if backend == "postgres":
                store.execute(sql_text)
                for stmt in seeds:
                    store.execute(stmt)
                n = 1
            else:
                statements = statements if recreate else split_statements(sql_text) + seeds
                for stmt in statements:
                    store.execute(stmt)
                n = len(statements)
                for stmt in migrate_columns(store, catalog):
                    print("added columns: %s" % stmt)
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
