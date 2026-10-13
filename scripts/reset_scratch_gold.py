"""Empty the Gold tables of a SCRATCH Databricks catalog (TRUNCATE, never DROP), so a full Gold build starts clean.

The Gold build is a MERGE and never removes rows, so rows of earlier test runs stay in a scratch catalog and break the
reconciliation checks. The real catalog (funnelstream) is refused. Run in the Spark container so DATABRICKS_* reach it:
  docker compose run --rm -e PG_DSN= spark python scripts/reset_scratch_gold.py --catalog workspace          (plan only)
  docker compose run --rm -e PG_DSN= spark python scripts/reset_scratch_gold.py --catalog workspace --yes    (executes)
Afterwards run scripts/apply_ddl.py for the same catalog: it re-seeds the unknown members (key -1).
"""
import argparse
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from warehouse.store import _ident, get_store
from warehouse.tables import GOLD_TABLES

REAL_CATALOG = "funnelstream"


def reset_statements(catalog):
    """One TRUNCATE per Gold table of `catalog`. Raises ValueError for the real catalog or an unsafe name."""
    name = _ident(catalog.strip())
    if name.lower() == REAL_CATALOG:
        raise ValueError("refusing to reset the real catalog %s" % REAL_CATALOG)
    return ["TRUNCATE TABLE %s.gold.%s" % (name, t.name) for t in GOLD_TABLES]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--catalog", required=True, help="scratch catalog, for example workspace")
    ap.add_argument("--yes", action="store_true", help="really execute the truncates (default: print the plan)")
    args = ap.parse_args(argv)
    try:
        statements = reset_statements(args.catalog)
    except ValueError as e:
        print("error: %s" % e, file=sys.stderr)
        return 2
    for s in statements:
        print(("run:  " if args.yes else "plan: ") + s)
    if args.yes:
        store = get_store("databricks")
        for s in statements:
            store.execute(s)
        print("%d Gold tables of catalog %s emptied" % (len(statements), args.catalog))
    return 0


if __name__ == "__main__":
    sys.exit(main())
