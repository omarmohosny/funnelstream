"""Check the Databricks connection and settings (never prints the token).

  docker compose run --rm spark python scripts/check_databricks.py [--upload-test]
"""
import argparse
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from warehouse.publish import make_uploader, volume_path
from warehouse.store import StoreConfigError, env_catalog, env_volume, get_store


def _line(exc, token):
    text = " ".join(str(exc).split())[:240] or type(exc).__name__
    return text.replace(token, "***") if token else text


def _catalog_missing(exc):
    t = str(exc).lower()
    return "catalog" in t and ("not found" in t or "not exist" in t or "cannot be found" in t
                               or "no such" in t or "does not exist" in t)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Check the Databricks connection, schemas and (optionally) upload.")
    ap.add_argument("--upload-test", action="store_true", help="also upload a tiny file to the volume")
    args = ap.parse_args(argv)
    token = os.environ.get("DATABRICKS_TOKEN") or ""
    try:
        catalog, volume = env_catalog(), env_volume()
        store = get_store("databricks")
    except StoreConfigError as e:
        print("FAILED: %s. Set them in .env (see .env.example)." % e)
        return 1
    except ValueError as e:
        print("FAILED: invalid DATABRICKS_CATALOG or DATABRICKS_VOLUME (%s)" % e)
        return 1
    try:
        with store:
            store.query("SELECT 1")
            print("OK connected")
            try:
                names = {str(r[0]).lower() for r in store.query("SHOW SCHEMAS IN %s" % catalog)}
            except Exception as e:
                if _catalog_missing(e):
                    print("Catalog %s not found. In the Databricks UI create it (Catalog Explorer, Create "
                          "catalog) or set DATABRICKS_CATALOG to an existing catalog such as workspace." % catalog)
                    return 1
                raise
        missing = [s for s in ("gold", "silver") if s not in names]
        if missing:
            print("MISSING schemas in %s: %s. Run: python scripts/apply_ddl.py --backend databricks"
                  % (catalog, ", ".join(missing)))
        else:
            print("OK schemas gold and silver exist in %s" % catalog)
    except Exception as e:
        print("FAILED: cannot reach Databricks or run SQL (%s): %s" % (type(e).__name__, _line(e, token)))
        return 1
    if args.upload_test:
        try:
            uploader = make_uploader(os.environ["DATABRICKS_HOST"], token)
            remote = volume_path(catalog, volume, "_connectivity_test") + "/ping.txt"
            uploader.upload(remote, b"funnelstream connectivity test\n")
            print("OK upload to %s" % remote)
        except Exception as e:
            print("FAILED: upload test (%s): %s. Does the volume %s.silver.%s exist? apply_ddl.py creates it."
                  % (type(e).__name__, _line(e, token), catalog, volume))
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
