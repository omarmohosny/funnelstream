"""GoldStore: one small interface over the Gold/Silver-mirror backends (Databricks, PostgreSQL fallback).

Rules (see spec section 3 and the Task 10 review):
- every identifier interpolated into SQL passes _ident (letters, digits, underscore only);
- values are never interpolated into Postgres SQL (placeholders); for Databricks they are rendered by
  sql_literal (MERGE ... VALUES has no server-side parameters);
- Databricks MERGE lists columns by name and never lists identity (surrogate key) columns;
- a batch holding one key twice keeps the last row (MERGE and ON CONFLICT both reject duplicates);
- the access token is never part of repr, str, exceptions or logs.
"""
import logging
import math
import os
import re
import time
from abc import ABC, abstractmethod
from datetime import date, datetime, timezone
from decimal import Decimal

from warehouse.ddl_render import CATALOG, split_statements
from warehouse.tables import all_tables

log = logging.getLogger("funnelstream.store")

_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# Anchored on purpose: a bare "503" or "429" also appears inside SQL text and parse errors.
_TRANSIENT_RE = re.compile(
    r"HTTP[ /]?(429|503)|status[ =:]*(429|503)|TooManyRequests|TEMPORARILY_UNAVAILABLE|timed out|ReadTimeout"
    r"|ConnectTimeout|Connection (reset|aborted)", re.IGNORECASE)


class StoreConfigError(ValueError):
    """Missing or invalid store configuration. Messages name variables, never values."""


def _ident(name) -> str:
    if not isinstance(name, str) or not _IDENT_RE.fullmatch(name):
        raise ValueError("invalid SQL identifier: %r" % (name,))
    return name


def sql_literal(value) -> str:
    """Render a Python value as a Spark SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):  # before int: bool is an int subclass
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("non-finite Decimal cannot be a SQL literal")
        return format(value, "f")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite float cannot be a SQL literal: %r" % value)
        return repr(value)
    if isinstance(value, str):
        out = (value.replace("\\", "\\\\").replace("'", "\\'").replace("\0", "\\0")
               .replace("\n", "\\n").replace("\r", "\\r"))
        return "'" + out + "'"
    if isinstance(value, datetime):  # before date: datetime is a date subclass
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return "TIMESTAMP'%s'" % value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, date):
        return "DATE'%s'" % value.isoformat()
    raise TypeError("unsupported SQL literal type: %s" % type(value).__name__)


class GoldStore(ABC):
    """Backend-neutral access to Gold (and, for Databricks, the Silver mirror) tables."""

    @abstractmethod
    def upsert(self, table: str, rows: list, key_cols: list) -> int:
        """Idempotent write of rows keyed by key_cols; returns the number of rows written."""

    @abstractmethod
    def query(self, sql: str, params=None, retry: bool = True) -> list:
        """Run a read statement, return rows as tuples."""

    @abstractmethod
    def execute(self, sql: str, retry: bool = True) -> None:
        """Run one statement without a result. retry=True may repeat it after a transient error, so only
        pass statements that are safe to repeat (all current callers are: IF NOT EXISTS DDL and MERGE)."""

    def run_script(self, sql_text: str, retry: bool = True) -> int:
        """Run every statement of a script (split on semicolons outside strings); returns the count."""
        statements = split_statements(sql_text)
        for stmt in statements:
            self.execute(stmt, retry=retry)
        return len(statements)

    def qualified(self, table: str) -> str:
        """Table name to use in a hand-written query (Postgres: the bare name)."""
        return _table_spec(table).name

    @abstractmethod
    def close(self) -> None:
        """Release the connection."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def _table_spec(name):
    spec = next((t for t in all_tables() if t.name == _ident(name)), None)
    if spec is None:
        raise ValueError("unknown table: %s" % name)
    return spec


def _prepare(table, rows, key_cols):
    """Validate an upsert. Returns (spec, columns, keys, rows without repeated keys)."""
    spec = _table_spec(table)
    if not key_cols:
        raise ValueError("key_cols must not be empty")
    keys = [_ident(k) for k in key_cols]
    columns = []
    for row in rows:
        for c in row:
            if c not in columns:
                columns.append(c)
    known = {c.name: c for c in spec.columns}
    for c in columns:
        _ident(c)
        if c not in known:
            raise ValueError("table %s has no column %s" % (table, c))
        if known[c].identity:
            raise ValueError("identity column %s must not be written" % c)
    for k in keys:
        if k not in known:
            raise ValueError("table %s has no key column %s" % (table, k))
        if k not in columns:
            raise ValueError("key column %s missing from rows" % k)
    latest = {}
    for row in rows:
        latest[tuple(row.get(k) for k in keys)] = row
    return spec, columns, keys, list(latest.values())


class PostgresStore(GoldStore):
    """PostgreSQL fallback. Gold tables live in schema public."""

    def __init__(self, dsn, connection=None):
        self._dsn = dsn
        self._conn = connection

    def __repr__(self):
        return "PostgresStore()"

    def _connection(self):
        if self._conn is None:
            import psycopg
            self._conn = psycopg.connect(self._dsn)
        return self._conn

    def upsert(self, table, rows, key_cols):
        if not rows:
            return 0
        spec, columns, keys, rows = _prepare(table, rows, key_cols)
        sets = [c for c in columns if c not in keys]
        sql = "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) " % (
            spec.name, ", ".join(columns), ", ".join(["%s"] * len(columns)), ", ".join(keys))
        sql += ("DO UPDATE SET " + ", ".join("%s = EXCLUDED.%s" % (c, c) for c in sets)) if sets else "DO NOTHING"
        data = [tuple(r.get(c) for c in columns) for r in rows]
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.executemany(sql, data)
        finally:
            cur.close()
        conn.commit()
        return len(data)

    def query(self, sql, params=None, retry=True):
        cur = self._connection().cursor()
        try:
            cur.execute(sql, params)
            return [tuple(r) for r in cur.fetchall()]
        finally:
            cur.close()

    def execute(self, sql, retry=True):
        conn = self._connection()
        cur = conn.cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()
        conn.commit()

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def _connect_databricks(server_hostname, http_path, access_token):
    """Lazy import hook (tests replace it)."""
    from databricks import sql
    return sql.connect(server_hostname=server_hostname, http_path=http_path, access_token=access_token)


class DatabricksStore(GoldStore):
    def __init__(self, host, token, http_path=None, warehouse_id=None, catalog=CATALOG, connection=None,
                 batch_size=500, max_retries=3, sleeper=time.sleep):
        if not host:
            raise StoreConfigError("DATABRICKS_HOST is missing")
        if not token:
            raise StoreConfigError("DATABRICKS_TOKEN is missing")
        if not http_path and not warehouse_id:
            raise StoreConfigError("DATABRICKS_WAREHOUSE_ID or DATABRICKS_HTTP_PATH is missing")
        self._host = re.sub(r"^https?://", "", host.strip()).rstrip("/")
        self._token = token
        if http_path:
            self._http_path = http_path
        else:
            if not re.fullmatch(r"[A-Za-z0-9_-]+", warehouse_id):
                raise StoreConfigError("DATABRICKS_WAREHOUSE_ID has an invalid format")
            self._http_path = "/sql/1.0/warehouses/%s" % warehouse_id
        self.catalog = _ident(catalog)
        self._conn = connection
        self.batch_size = max(1, int(batch_size))
        self.max_retries = max_retries
        self._sleep = sleeper

    def __repr__(self):
        return "DatabricksStore(catalog=%r)" % self.catalog

    __str__ = __repr__

    def _scrub(self, exc):
        text = str(exc)
        if self._token and self._token in text:
            return RuntimeError(text.replace(self._token, "***"))
        return exc

    def _connection(self):
        if self._conn is None:
            try:
                self._conn = _connect_databricks(self._host, self._http_path, self._token)
            except Exception as e:
                raise self._scrub(e) from None
        return self._conn

    def _run(self, sql, params=None, fetch=False, retry=True):
        attempt = 0
        while True:
            try:
                cur = self._connection().cursor()
                try:
                    if params is None:
                        cur.execute(sql)
                    else:
                        cur.execute(sql, params)
                    return [tuple(r) for r in cur.fetchall()] if fetch else None
                finally:
                    cur.close()
            except Exception as e:
                if retry and attempt < self.max_retries and _TRANSIENT_RE.search(str(e)):
                    delay = 2 ** attempt
                    attempt += 1
                    log.warning("transient Databricks error, retry %d/%d in %ss", attempt, self.max_retries, delay)
                    self._sleep(delay)
                    continue
                raise self._scrub(e) from None

    def query(self, sql, params=None, retry=True):
        return self._run(sql, params, fetch=True, retry=retry)

    def execute(self, sql, retry=True):
        self._run(sql, retry=retry)

    def _plan(self, table, rows, key_cols):
        """(statements, row count) for an upsert; one MERGE per batch."""
        if not rows:
            return [], 0
        spec, columns, keys, rows = _prepare(table, rows, key_cols)
        target = "%s.%s.%s" % (self.catalog, spec.schema_group, spec.name)
        on = " AND ".join("t.%s = s.%s" % (k, k) for k in keys)
        sets = [c for c in columns if c not in keys]
        update = (" WHEN MATCHED THEN UPDATE SET " + ", ".join("t.%s = s.%s" % (c, c) for c in sets)) if sets else ""
        collist = ", ".join(columns)
        insert = " WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)" % (collist, ", ".join("s." + c for c in columns))
        out = []
        for i in range(0, len(rows), self.batch_size):
            values = ", ".join("(" + ", ".join(sql_literal(r.get(c)) for c in columns) + ")"
                               for r in rows[i:i + self.batch_size])
            out.append("MERGE INTO %s AS t USING (SELECT * FROM VALUES %s AS s(%s)) ON %s%s%s"
                       % (target, values, collist, on, update, insert))
        return out, len(rows)

    def qualified(self, table):
        spec = _table_spec(table)
        return "%s.%s.%s" % (self.catalog, spec.schema_group, spec.name)

    def upsert(self, table, rows, key_cols):
        statements, count = self._plan(table, rows, key_cols)
        for stmt in statements:
            self._run(stmt)
        return count

    def close(self):
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None


def get_store(backend=None, env=None) -> GoldStore:
    env = os.environ if env is None else env
    backend = (backend or env.get("GOLD_BACKEND") or "postgres").strip().lower()
    if backend == "postgres":
        if not env.get("PG_DSN"):
            raise StoreConfigError("missing environment variable(s): PG_DSN")
        return PostgresStore(env["PG_DSN"])
    if backend == "databricks":
        missing = [n for n in ("DATABRICKS_HOST", "DATABRICKS_TOKEN") if not env.get(n)]
        if not env.get("DATABRICKS_WAREHOUSE_ID") and not env.get("DATABRICKS_HTTP_PATH"):
            missing.append("DATABRICKS_WAREHOUSE_ID (or DATABRICKS_HTTP_PATH)")
        if missing:
            raise StoreConfigError("missing environment variable(s): " + ", ".join(missing))
        return DatabricksStore(env["DATABRICKS_HOST"], env["DATABRICKS_TOKEN"],
                               http_path=env.get("DATABRICKS_HTTP_PATH") or None,
                               warehouse_id=env.get("DATABRICKS_WAREHOUSE_ID") or None,
                               catalog=env_catalog(env))
    raise StoreConfigError("unknown GOLD_BACKEND %r (use postgres or databricks)" % backend)


def env_catalog(env=None) -> str:
    """The one place the catalog name comes from: DATABRICKS_CATALOG, default funnelstream."""
    env = os.environ if env is None else env
    return _ident(env.get("DATABRICKS_CATALOG") or CATALOG)


def env_volume(env=None) -> str:
    """The upload volume name: DATABRICKS_VOLUME, default silver_upload."""
    env = os.environ if env is None else env
    return _ident(env.get("DATABRICKS_VOLUME") or "silver_upload")
