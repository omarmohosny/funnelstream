"""Apply warehouse/live_ddl.sql (the Postgres live and health tables) through the Postgres store.

Idempotent. The read-only role grafana_ro gets its password from GRAFANA_DB_PASSWORD at apply time; without it the
role section is skipped, a red error is printed and the exit code is 3 (the tables are still created).
`--role NAME` renames the role (tests use a scratch role). Passwords are never printed.
"""
import os
import re
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from contracts.stores import district_rows, store_rows
from warehouse.live_tables import LIVE_TABLE_NAMES
from warehouse.store import StoreConfigError, get_store

DDL_FILE = os.path.join("warehouse", "live_ddl.sql")
PLACEHOLDER = "__GRAFANA_DB_PASSWORD__"
_ROLE_BLOCK = re.compile(r"-- BEGIN grafana_ro.*?-- END grafana_ro\n?", re.DOTALL)


def read_ddl():
    with open(os.path.join(_ROOT, DDL_FILE), encoding="utf-8") as fh:
        return fh.read()


DEFAULT_ROLE = "grafana_ro"
_ROLE_RE = re.compile(r"[a-z_][a-z0-9_]{0,62}")


def render(password=None, text=None, role=DEFAULT_ROLE):
    """DDL text ready to run. With a password the role section is filled in, without one it is removed.
    `role` renames the read-only role (tests use a scratch role so the real one is never touched)."""
    if not _ROLE_RE.fullmatch(role or ""):
        raise ValueError("invalid role name: %r" % (role,))
    text = read_ddl() if text is None else text
    if not password:
        return _ROLE_BLOCK.sub("", text)
    if "$$" in password or "\0" in password:
        raise ValueError("GRAFANA_DB_PASSWORD must not contain '$$' or NUL")
    return text.replace(PLACEHOLDER, password.replace("'", "''")).replace(DEFAULT_ROLE, role)


RED, RESET = "\033[31m", "\033[0m"
EXIT_NO_PASSWORD = 3


def seed_store_dims(store):
    """The store map is data: written from the one Python source every time, so the SQL joins and the simulator agree."""
    store.upsert("dim_store", store_rows(), ["store_id"])
    store.upsert("dim_store_district", district_rows(), ["city", "district"])


def seed_product_dim(store, rows):
    """dim_product_current: the latest silver_products version of each product, filled by the same job as dim_store so
    Grafana tables show names. Run the script again after the catalog changes (seed_silver_reference.py)."""
    if rows:
        store.upsert("dim_product_current", rows, ["product_id"])
    return len(rows)


def read_product_rows(lake):
    """Rows from local Silver through Spark; [] without a Spark session when the silver_products folder is missing,
    [] and a warning on stderr when Spark is not available."""
    from streaming.silver.writer import silver_path
    if not os.path.isdir(silver_path(lake, "silver_products")):
        return []  # nothing to read: no Spark session (a JVM start) just to find that out
    try:
        from streaming.common.spark import get_spark
        from warehouse.live_stock import compute_product_rows
        return compute_product_rows(get_spark("apply_live_ddl"), lake)
    except Exception as e:  # the DDL must still be applied; the dimension is filled on the next run
        print("warning: dim_product_current not filled (%s)" % type(e).__name__, file=sys.stderr)
        return []


def main(argv=None, env=None, store=None, product_rows=None):
    import argparse

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--role", default=DEFAULT_ROLE, help="name of the read-only role (default grafana_ro)")
    ap.add_argument("--lake", default="data/lake", help="local lake root, read for dim_product_current")
    args = ap.parse_args(argv)
    env = os.environ if env is None else env
    password = env.get("GRAFANA_DB_PASSWORD") or ""
    own = store is None
    try:
        sql = render(password, role=args.role)
        if own:
            store = get_store("postgres", env=env)
        store.execute(sql)
        seed_store_dims(store)
        rows = product_rows if product_rows is not None else (read_product_rows(os.path.abspath(args.lake)) if own else [])
        n_products = seed_product_dim(store, rows)
    except (StoreConfigError, ValueError) as e:
        print("error: %s" % e, file=sys.stderr)
        return 2
    except Exception as e:
        text = " ".join(str(e).split())[:300]
        print("error: applying the live DDL failed (%s): %s" % (
            type(e).__name__, text.replace(password, "***") if password else text), file=sys.stderr)
        return 1
    finally:
        if own and store is not None:
            store.close()
    for name in LIVE_TABLE_NAMES:
        print("table %s ready" % name)
    print("dim_product_current: %d products" % n_products)
    if password:
        print("role %s ready (SELECT on %s)" % (args.role, ", ".join(LIVE_TABLE_NAMES)))
        return 0
    print("%sERROR: role %s NOT created: GRAFANA_DB_PASSWORD is not set (put it in .env, run this again). "
          "Grafana cannot read the tables without it.%s" % (RED, args.role, RESET), file=sys.stderr)
    return EXIT_NO_PASSWORD


if __name__ == "__main__":
    sys.exit(main())
