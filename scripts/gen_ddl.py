"""Generate warehouse/ddl.sql and warehouse/databricks_ddl.sql from warehouse/tables.py.

Usage: python scripts/gen_ddl.py [--check] [--out-dir DIR]
--check exits non-zero when the files in the directory differ from the generated output (CRLF tolerated).
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from warehouse.ddl_render import render_databricks, render_postgres  # noqa: E402
from warehouse.tables import all_tables  # noqa: E402


def generate():
    tables = all_tables()
    return {"ddl.sql": render_postgres(tables), "databricks_ddl.sql": render_databricks(tables)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--out-dir", default=str(ROOT / "warehouse"))
    args = ap.parse_args(argv)
    out = Path(args.out_dir)
    files = generate()
    if args.check:
        bad = [n for n, text in files.items()
               if not (out / n).exists() or (out / n).read_bytes().replace(b"\r\n", b"\n") !=text.encode("utf-8")]
        for n in bad:
            print("DRIFT: %s differs from generated output" % (out / n), file=sys.stderr)
        return 1 if bad else 0
    out.mkdir(parents=True, exist_ok=True)
    for n, text in files.items():
        with open(out / n, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(text)
        print("wrote %s" % (out / n))
    return 0


if __name__ == "__main__":
    sys.exit(main())
