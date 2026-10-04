"""Render the table spec (warehouse/tables.py) as PostgreSQL or Databricks (Delta) DDL text."""
import re

CATALOG = "funnelstream"
HEADER = "-- GENERATED FILE, edit warehouse/tables.py and run scripts/gen_ddl.py"

_PG_TYPES = {"string": "text", "int": "integer", "bigint": "bigint", "boolean": "boolean", "date": "date",
             "timestamp": "timestamptz"}
_DBX_TYPES = {"string": "STRING", "int": "INT", "bigint": "BIGINT", "boolean": "BOOLEAN", "date": "DATE",
              "timestamp": "TIMESTAMP"}


def _pg_type(t):
    m = re.match(r"decimal\((\d+),(\d+)\)$", t)
    return "numeric(%s,%s)" % m.groups() if m else _PG_TYPES[t]


def _dbx_type(t):
    m = re.match(r"decimal\((\d+),(\d+)\)$", t)
    return "DECIMAL(%s,%s)" % m.groups() if m else _DBX_TYPES[t]


def _gold(tables):
    return [t for t in tables if t.schema_group == "gold"]


def _pg_table(t):
    lines = []
    for c in t.columns:
        if c.identity:
            lines.append("    %s bigint GENERATED ALWAYS AS IDENTITY" % c.name)
        else:
            lines.append("    %s %s%s" % (c.name, _pg_type(c.type), "" if c.nullable else " NOT NULL"))
    if t.keys:
        lines.append("    PRIMARY KEY (%s)" % ", ".join(t.keys))
    if any(c.name == "valid_from" for c in t.columns):  # SCD2: natural id plus version start
        natural = next(c.name for c in t.columns if not c.identity)
        lines.append("    UNIQUE (%s, valid_from)" % natural)
    return "CREATE TABLE IF NOT EXISTS %s (\n%s\n);" % (t.name, ",\n".join(lines))


def _pg_indexes(t):
    out = []
    for col in ("date_key", "order_id"):
        if col in t.column_names() and t.keys[0] != col:
            out.append("CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s (%s);" % (t.name, col, t.name, col))
    return out


def _role_block(role):
    return ("DO $$\nBEGIN\n    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '%s') THEN\n"
            "        CREATE ROLE %s NOLOGIN;\n    END IF;\nEND\n$$;" % (role, role))


def render_postgres(tables) -> str:
    gold = _gold(tables)
    blocks = [HEADER + "\n-- PostgreSQL fallback for the Gold tables. Safe to run more than once.",
              "-- Roles are created NOLOGIN: logins are created by the operator and granted analyst or engineer.\n"
              + _role_block("analyst") + "\n\n" + _role_block("engineer")]
    for t in gold:
        blocks.append(_pg_table(t))
        blocks.extend(_pg_indexes(t))
    names = ", ".join(t.name for t in gold)
    blocks.append("GRANT SELECT ON %s TO analyst;" % names)
    blocks.append("GRANT SELECT, INSERT, UPDATE, DELETE ON %s TO engineer;" % names)
    return "\n\n".join(blocks) + "\n"


def _sq(text):
    return text.replace("'", "''")


def _dbx_table(t):
    lines = []
    for c in t.columns:
        if c.identity:
            lines.append("    %s BIGINT GENERATED ALWAYS AS IDENTITY" % c.name)
        else:
            lines.append("    %s %s%s" % (c.name, _dbx_type(c.type), "" if c.nullable else " NOT NULL"))
    comment = ("keys: %s (documented, not enforced)" % ", ".join(t.keys)) if t.keys else "keys: none"
    if t.comment:
        comment += "; " + t.comment
    tail = "USING DELTA"
    if t.partition:
        tail += "\nPARTITIONED BY (%s)" % t.partition
    tail += "\nCOMMENT '%s'" % _sq(comment)
    return "CREATE TABLE IF NOT EXISTS %s.%s.%s (\n%s\n)\n%s;" % (
        CATALOG, t.schema_group, t.name, ",\n".join(lines), tail)


def render_databricks(tables) -> str:
    blocks = [HEADER + "\n-- Databricks (Delta, Unity Catalog). Keys are documented in COMMENT, not enforced.",
              "CREATE SCHEMA IF NOT EXISTS %s.gold;" % CATALOG,
              "CREATE SCHEMA IF NOT EXISTS %s.silver;" % CATALOG]
    blocks.extend(_dbx_table(t) for t in tables)
    return "\n\n".join(blocks) + "\n"
