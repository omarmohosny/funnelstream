"""Validate the JSON files of the generated Power BI project against the schemas Microsoft publishes.

Power BI Desktop checks every file of a .pbip project against the schema named in its "$schema" line and refuses to
open the project on the first violation. Nobody can open Power BI Desktop in a test, so this script fetches the
schemas (internet needed) and reports every violation at once:

  .venv\\Scripts\\python scripts/validate_pbip.py [dashboards/powerbi]

Exit code 0 when every file is valid, 1 otherwise. It checks the JSON files only; the semantic model itself is TMDL
text, which has no schema.
"""
import json
import sys
import urllib.request
from pathlib import Path

import jsonschema
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT7

ROOT = Path(__file__).resolve().parent.parent
EXTENSIONS = (".json", ".pbir", ".pbip", ".pbism")


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    root = Path(argv[0]) if argv else ROOT / "dashboards" / "powerbi"
    cache = {}

    def fetch(uri):
        uri = uri.split("#")[0]
        if uri not in cache:
            with urllib.request.urlopen(uri, timeout=30) as response:
                cache[uri] = json.load(response)
        return cache[uri]

    registry = Registry(retrieve=lambda uri: Resource.from_contents(fetch(uri), default_specification=DRAFT7))
    problems = 0
    files = sorted(p for p in root.rglob("*") if p.is_file() and p.suffix in EXTENSIONS)
    for f in files:
        doc = json.loads(f.read_text(encoding="utf-8"))
        uri = doc.get("$schema")
        name = f.relative_to(root)
        if not uri:
            print("no $schema (not checked):", name)
            continue
        try:
            errors = sorted(jsonschema.Draft7Validator(fetch(uri), registry=registry).iter_errors(doc),
                            key=lambda e: list(e.path))
        except Exception as exc:  # no internet, or a schema that moved
            print("CANNOT CHECK", name, "->", type(exc).__name__, str(exc)[:120])
            problems += 1
            continue
        for e in errors[:6]:
            print("INVALID", name, "|", "/" + "/".join(map(str, e.path)), "|", e.message[:160])
        problems += bool(errors)
        if not errors:
            print("ok", name)
    print("files:", len(files), "problems:", problems)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
