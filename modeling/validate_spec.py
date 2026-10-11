"""Guardrail between the LLM and code generation: returns a list of error strings (empty = valid)."""
import re

from modeling.source_catalog import source_columns

STYLES = ("star", "snowflake", "galaxy")
TABLE_TYPES = ("dimension", "fact", "outrigger")
ROLES = ("pk", "bk", "fk", "attribute", "measure", "scd_meta")
_TYPE_RE = re.compile(r"^(STRING|INT|BIGINT|BOOLEAN|DATE|TIMESTAMP|DECIMAL\(\d+,\d+\))$")
_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def validate_spec(spec: dict, catalog: dict, style: str = None) -> list:
    errors = []
    style = style or spec.get("style")
    if style not in STYLES:
        return ["style must be one of %s, got %r" % (", ".join(STYLES), style)]
    tables = spec.get("tables")
    if not isinstance(tables, list) or not tables:
        return ["spec has no tables"]
    allowed = source_columns(catalog)
    by_name = {}
    for t in tables:
        n = t.get("name", "?")
        if not _NAME_RE.match(str(n)):
            errors.append("bad table name %r" % n)
        if n in by_name:
            errors.append("duplicate table %s" % n)
        by_name[n] = t

    for t in tables:
        n = t.get("name", "?")
        if t.get("type") not in TABLE_TYPES:
            errors.append("%s: type must be one of %s" % (n, ", ".join(TABLE_TYPES)))
        cols = t.get("columns") or []
        if not cols:
            errors.append("%s: no columns" % n)
        names = [c.get("column") for c in cols]
        for d in {x for x in names if names.count(x) > 1}:
            errors.append("%s: duplicate column %s" % (n, d))
        pk = t.get("pk") or []
        if not pk or any(k not in names for k in pk):
            errors.append("%s: pk %s must be made of the table's own columns" % (n, pk))
        for c in cols:
            cn = c.get("column", "?")
            where = "%s.%s" % (n, cn)
            if not _TYPE_RE.match(str(c.get("data_type", "")).upper()):
                errors.append("%s: bad data_type %r" % (where, c.get("data_type")))
            if c.get("role") not in ROLES:
                errors.append("%s: role must be one of %s" % (where, ", ".join(ROLES)))
            src, expr = c.get("source_column") or "", c.get("expression") or ""
            if not src and not expr:
                errors.append("%s: needs a source_column or an expression" % where)
            if src and src not in allowed:
                errors.append("%s: source_column %s is not in the Silver catalog" % (where, src))
            ref = c.get("references") or ""
            if c.get("role") == "fk" and not ref:
                errors.append("%s: fk without references" % where)
            if ref:
                rt, _, rc = ref.partition(".")
                target = by_name.get(rt)
                if target is None:
                    errors.append("%s: references unknown table %s" % (where, rt))
                elif target.get("pk") != [rc]:
                    errors.append("%s: %s is not the single-column pk of %s" % (where, rc, rt))
        if t.get("scd") == 2:
            for need in ("valid_from", "valid_to", "is_current"):
                if need not in names:
                    errors.append("%s: SCD2 table needs %s" % (n, need))

    facts = [t for t in tables if t.get("type") == "fact"]
    if style == "star" and len(facts) != 1:
        errors.append("star needs exactly one fact, found %d" % len(facts))
    if style == "snowflake" and not any(t.get("type") == "outrigger" for t in tables):
        errors.append("snowflake needs at least one outrigger table")
    if style == "galaxy":
        users = {}
        for f in facts:
            for c in f.get("columns", []):
                if c.get("references"):
                    users.setdefault(c["references"].split(".")[0], set()).add(f["name"])
        if not any(len(v) >= 2 for v in users.values()):
            errors.append("galaxy needs at least one dimension shared by 2+ facts")
    return errors
