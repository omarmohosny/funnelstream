"""Apply warehouse/live_ddl.sql (the Postgres live and health tables) through the Postgres store.

Idempotent. The read-only role grafana_ro gets its password from GRAFANA_DB_PASSWORD at apply time; without it the
role section is skipped, a red error is printed and the exit code is 3 (the four tables are still created).
`--role NAME` renames the role (tests use a scratch role). Passwords are never printed.
"""
import os
import re
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

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


def main(argv=None, env=None, store=None):
    import argparse

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--role", default=DEFAULT_ROLE, help="name of the read-only role (default grafana_ro)")
    args = ap.parse_args(argv)
    env = os.environ if env is None else env
    password = env.get("GRAFANA_DB_PASSWORD") or ""
    own = store is None
    try:
        sql = render(password, role=args.role)
        if own:
            store = get_store("postgres", env=env)
        store.execute(sql)
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
    if password:
        print("role %s ready (SELECT on %s)" % (args.role, ", ".join(LIVE_TABLE_NAMES)))
        return 0
    print("%sERROR: role %s NOT created: GRAFANA_DB_PASSWORD is not set (put it in .env, run this again). "
          "Grafana cannot read the tables without it.%s" % (RED, args.role, RESET), file=sys.stderr)
    return EXIT_NO_PASSWORD


if __name__ == "__main__":
    sys.exit(main())
