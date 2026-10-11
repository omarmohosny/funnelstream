"""Loads the Silver metadata JSON (the only allowed sources) that the modeler and the validator use."""
import json
from pathlib import Path

DEFAULT_CATALOG = Path(__file__).parent / "metadata" / "silver_catalog.json"
# Never allowed as a source, whatever a catalog file contains (Gold holds no personal data).
PII_COLUMNS = {"first_name", "last_name", "name_ar", "phone", "email", "address", "raw_payload", "raw_value",
               "raw_json", "raw_row"}


def load_catalog(path=None) -> dict:
    cat = json.loads(Path(path or DEFAULT_CATALOG).read_text(encoding="utf-8"))
    for t in cat["tables"]:
        t["columns"] = [c for c in t["columns"] if c["name"].lower() not in PII_COLUMNS]
    return cat


def source_columns(catalog: dict) -> set:
    """Set of 'silver_table.column' strings a spec column may cite as its source."""
    return {"%s.%s" % (t["name"], c["name"]) for t in catalog["tables"] for c in t["columns"]}
