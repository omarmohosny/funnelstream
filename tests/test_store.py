"""Host tests for warehouse/store.py. Fake connections only, no network."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

import warehouse.store as store_mod
from warehouse.store import (DatabricksStore, GoldStore, PostgresStore, StoreConfigError, _ident, get_store,
                             sql_literal)

TOKEN = "dapi-SECRET-TOKEN-0123456789"


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        self.conn.statements.append(sql if params is None else (sql, params))
        if self.conn.failures:
            raise self.conn.failures.pop(0)

    def executemany(self, sql, data):
        self.conn.many.append((sql, list(data)))

    def fetchall(self):
        return self.conn.result

    def close(self):
        pass


class FakeConn:
    def __init__(self, failures=None, result=None):
        self.statements, self.many, self.commits = [], [], 0
        self.failures = list(failures or [])
        self.result = result or []
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


def dbx(conn=None, **kw):
    kw.setdefault("sleeper", lambda s: None)
    return DatabricksStore("https://adb-1.example.invalid/", TOKEN, warehouse_id="abc123",
                           connection=conn or FakeConn(), **kw)


# 1. sql_literal
def test_sql_literal_scalars():
    assert sql_literal(None) == "NULL"
    assert sql_literal(True) == "TRUE" and sql_literal(False) == "FALSE"
    assert sql_literal(1) == "1" and sql_literal(0) == "0" and sql_literal(-7) == "-7"
    assert sql_literal(1.5) == "1.5"
    assert sql_literal(Decimal("12.30")) == "12.30"
    assert sql_literal(Decimal("0.0001")) == "0.0001"
    assert sql_literal(Decimal("1E+2")) == "100"
    assert sql_literal(Decimal("12345678901234567890.123456789")) == "12345678901234567890.123456789"
    assert sql_literal(date(2026, 10, 4)) == "DATE'2026-10-04'"


def test_sql_literal_bool_checked_before_int():
    assert sql_literal(True) != "1"
    assert sql_literal(1) != "TRUE"


def test_sql_literal_strings_adversarial():
    assert sql_literal("abc") == "'abc'"
    assert sql_literal("it's") == "'it\\'s'"
    assert sql_literal("a\\b") == "'a\\\\b'"
    assert sql_literal("\\'") == "'\\\\\\''"  # backslash escaped first, then the quote
    assert sql_literal("'; DROP TABLE x; --") == "'\\'; DROP TABLE x; --'"
    assert sql_literal("l1\nl2\r") == "'l1\\nl2\\r'"
    assert sql_literal("a\0b") == "'a\\0b'"
    assert sql_literal("café \U0001F600") == "'café \U0001F600'"
    for s in ("x'y", "'; DROP", "\\", "\n", "\0"):  # no raw control char or unescaped quote survives
        body = sql_literal(s)[1:-1]
        assert "\n" not in body and "\0" not in body and "\r" not in body
        assert "'" not in body.replace("\\\\", "").replace("\\'", "")


def test_sql_literal_datetime_utc():
    assert sql_literal(datetime(2026, 10, 4, 1, 2, 3, 4)) == "TIMESTAMP'2026-10-04 01:02:03.000004'"
    cairo = timezone(timedelta(hours=3))
    assert sql_literal(datetime(2026, 10, 4, 3, 0, 0, tzinfo=cairo)) == "TIMESTAMP'2026-10-04 00:00:00.000000'"
    assert sql_literal(datetime(2026, 10, 4, tzinfo=timezone.utc)) == "TIMESTAMP'2026-10-04 00:00:00.000000'"


def test_sql_literal_rejections():
    for bad in (float("nan"), float("inf"), float("-inf"), Decimal("NaN")):
        with pytest.raises(ValueError):
            sql_literal(bad)
    for bad in (b"x", [1], {"a": 1}, object()):
        with pytest.raises(TypeError):
            sql_literal(bad)


# 2. _ident
def test_ident_accepts_and_rejects():
    for ok in ("fact_orders", "_x", "A1", "silver_events"):
        assert _ident(ok) == ok
    for bad in ("a b", "a;b", "a-b", "", "1abc", "a'b", 'a"b', "café", "a\n", "a.b", None, 5):
        with pytest.raises(ValueError):
            _ident(bad)


# 3. DatabricksStore.upsert
def test_upsert_dimension_like_table_statement_exact():
    conn = FakeConn()
    s = dbx(conn)
    rows = [{"campaign_id": "c1", "utm_campaign": "u1", "platform": "meta"},
            {"campaign_id": "c2", "utm_campaign": "u2", "platform": "tiktok"}]
    assert s.upsert("dim_campaign", rows, ["campaign_id"]) == 2
    (stmt,) = conn.statements
    assert stmt == (
        "MERGE INTO funnelstream.gold.dim_campaign AS t USING (SELECT * FROM VALUES "
        "('c1', 'u1', 'meta'), ('c2', 'u2', 'tiktok') AS s(campaign_id, utm_campaign, platform)) "
        "ON t.campaign_id = s.campaign_id "
        "WHEN MATCHED THEN UPDATE SET t.utm_campaign = s.utm_campaign, t.platform = s.platform "
        "WHEN NOT MATCHED THEN INSERT (campaign_id, utm_campaign, platform) "
        "VALUES (s.campaign_id, s.utm_campaign, s.platform)")
    assert "customer_key" not in stmt and "campaign_key" not in stmt  # identity never written
    assert "INSERT *" not in stmt


def test_upsert_fact_table_composite_key_and_types():
    conn = FakeConn()
    s = dbx(conn)
    rows = [{"order_id": "o1", "line_no": 1, "quantity": 2, "line_total": Decimal("19.98"), "customer_key": None}]
    s.upsert("fact_order_lines", rows, ["order_id", "line_no"])
    stmt = conn.statements[0]
    assert stmt.startswith("MERGE INTO funnelstream.gold.fact_order_lines AS t")
    assert "ON t.order_id = s.order_id AND t.line_no = s.line_no" in stmt
    assert "('o1', 1, 2, 19.98, NULL)" in stmt
    assert "UPDATE SET t.quantity = s.quantity, t.line_total = s.line_total, t.customer_key = s.customer_key" in stmt
    assert "t.order_id = s.order_id," not in stmt and "SET t.order_id" not in stmt and "SET t.line_no" not in stmt


def test_upsert_batches_by_batch_size():
    conn = FakeConn()
    s = dbx(conn, batch_size=2)
    rows = [{"campaign_id": "c%d" % i, "platform": "p"} for i in range(5)]
    assert s.upsert("dim_campaign", rows, ["campaign_id"]) == 5
    assert len(conn.statements) == 3
    assert conn.statements[0].count("('c") == 2 and conn.statements[2].count("('c") == 1


def test_upsert_key_only_table_has_no_update_clause():
    conn = FakeConn()
    s = dbx(conn)
    s.upsert("dim_date", [{"date_key": 20261004}], ["date_key"])
    stmt = conn.statements[0]
    assert "WHEN MATCHED" not in stmt and "UPDATE" not in stmt
    assert "WHEN NOT MATCHED THEN INSERT (date_key) VALUES (s.date_key)" in stmt


def test_upsert_unknown_table_and_columns_raise():
    s = dbx()
    with pytest.raises(ValueError):
        s.upsert("no_such_table", [{"a": 1}], ["a"])
    with pytest.raises(ValueError):
        s.upsert("dim_campaign; DROP", [{"a": 1}], ["a"])
    with pytest.raises(ValueError):
        s.upsert("dim_campaign", [{"nope": 1}], ["nope"])
    with pytest.raises(ValueError):
        s.upsert("dim_campaign", [{"campaign_id": "c"}], ["not_a_key"])
    with pytest.raises(ValueError):  # identity column in rows
        s.upsert("dim_campaign", [{"campaign_key": 1, "campaign_id": "c"}], ["campaign_id"])
    with pytest.raises(ValueError):
        s.upsert("dim_campaign", [{"campaign_id": "c", "bad col": 1}], ["campaign_id"])


def test_upsert_empty_rows_sends_nothing():
    conn = FakeConn()
    assert dbx(conn).upsert("dim_campaign", [], ["campaign_id"]) == 0
    assert conn.statements == []


def test_upsert_missing_columns_become_null_in_first_row_order():
    conn = FakeConn()
    s = dbx(conn)
    rows = [{"campaign_id": "c1", "platform": "meta"}, {"campaign_id": "c2", "name": "N"}]
    s.upsert("dim_campaign", rows, ["campaign_id"])
    stmt = conn.statements[0]
    assert "AS s(campaign_id, platform, name)" in stmt
    assert "('c1', 'meta', NULL), ('c2', NULL, 'N')" in stmt


def test_upsert_catalog_and_schema_qualification():
    conn = FakeConn()
    s = DatabricksStore("h", TOKEN, warehouse_id="w", catalog="other_cat", connection=conn)
    s.upsert("silver_events", [{"event_id": "e1"}], ["event_id"])
    s.upsert("fact_orders", [{"order_id": "o1"}], ["order_id"])
    assert "MERGE INTO other_cat.silver.silver_events AS t" in conn.statements[0]
    assert "MERGE INTO other_cat.gold.fact_orders AS t" in conn.statements[1]
    with pytest.raises(ValueError):
        DatabricksStore("h", TOKEN, warehouse_id="w", catalog="bad cat")


def test_upsert_duplicate_keys_in_batch_keep_last():
    conn = FakeConn()
    n = dbx(conn).upsert("dim_campaign", [{"campaign_id": "c", "platform": "a"},
                                          {"campaign_id": "c", "platform": "b"}], ["campaign_id"])
    assert n == 1 and "'b'" in conn.statements[0] and "'a'" not in conn.statements[0]


def test_connection_arguments(monkeypatch):
    seen = {}
    monkeypatch.setattr(store_mod, "_connect_databricks", lambda h, p, t: seen.update(h=h, p=p) or FakeConn())
    DatabricksStore("https://adb-9.example.invalid/", TOKEN, warehouse_id="wh42").execute("SELECT 1")
    assert seen == {"h": "adb-9.example.invalid", "p": "/sql/1.0/warehouses/wh42"}
    DatabricksStore("adb-9.example.invalid", TOKEN, http_path="/sql/1.0/custom").execute("SELECT 1")
    assert seen["p"] == "/sql/1.0/custom"


def test_query_and_run_script():
    conn = FakeConn(result=[(1,)])
    s = dbx(conn)
    assert s.query("SELECT 1") == [(1,)]
    assert s.query("SELECT ?", (3,)) == [(1,)]
    assert conn.statements[1] == ("SELECT ?", (3,))
    n = s.run_script("CREATE SCHEMA a; -- c\nCREATE SCHEMA b;")
    assert n == 2 and conn.statements[2:] == ["CREATE SCHEMA a;", "CREATE SCHEMA b;"]
    with dbx(conn) as ctx:
        assert isinstance(ctx, GoldStore)
    assert conn.closed


# 4. retry
def test_retry_transient_then_success_backoff_increases():
    sleeps = []
    conn = FakeConn(failures=[Exception("HTTP 503 Service Unavailable"), Exception("request timed out")])
    s = dbx(conn, sleeper=sleeps.append)
    s.execute("SELECT 1")
    assert sleeps == [1, 2]
    assert len(conn.statements) == 3


def test_retry_non_transient_propagates_immediately():
    sleeps = []
    conn = FakeConn(failures=[Exception("PARSE_SYNTAX_ERROR near x")])
    with pytest.raises(Exception, match="PARSE_SYNTAX_ERROR"):
        dbx(conn, sleeper=sleeps.append).execute("bad")
    assert sleeps == [] and len(conn.statements) == 1


def test_retry_exhausted_raises_last_error():
    sleeps = []
    conn = FakeConn(failures=[Exception("429 TooManyRequests #%d" % i) for i in range(10)])
    with pytest.raises(Exception, match="#3"):
        dbx(conn, sleeper=sleeps.append, max_retries=3).query("SELECT 1")
    assert sleeps == [1, 2, 4] and len(conn.statements) == 4


def test_upsert_retries_the_failed_batch():
    sleeps = []
    conn = FakeConn(failures=[Exception("HTTP 503")])
    n = dbx(conn, sleeper=sleeps.append).upsert("dim_campaign", [{"campaign_id": "c"}], ["campaign_id"])
    assert n == 1 and sleeps == [1] and len(conn.statements) == 2


# 5. token safety
def test_token_never_in_repr_str_or_exceptions(monkeypatch):
    s = dbx()
    assert TOKEN not in repr(s) and TOKEN not in str(s)
    conn = FakeConn(failures=[Exception("auth failed for token %s" % TOKEN)])
    with pytest.raises(Exception) as ei:
        dbx(conn).execute("SELECT 1")
    assert TOKEN not in str(ei.value) and TOKEN not in repr(ei.value)
    assert ei.value.__cause__ is None and ei.value.__suppress_context__

    def boom(h, p, t):
        raise OSError("cannot connect with %s" % t)

    monkeypatch.setattr(store_mod, "_connect_databricks", boom)
    with pytest.raises(Exception) as ei2:
        DatabricksStore("h", TOKEN, warehouse_id="w").query("SELECT 1")
    assert TOKEN not in str(ei2.value)
    for bad in (lambda: DatabricksStore("h", TOKEN, warehouse_id="bad id"),
                lambda: DatabricksStore("h", TOKEN, catalog="bad cat", warehouse_id="w"),
                lambda: dbx().upsert("nope", [{"a": 1}], ["a"])):
        with pytest.raises(ValueError) as ei3:
            bad()
        assert TOKEN not in str(ei3.value)


# 6. PostgresStore
def test_postgres_upsert_statement_and_rows():
    conn = FakeConn()
    s = PostgresStore("postgresql://x", connection=conn)
    rows = [{"order_id": "o1", "city": "Cairo", "country": "EG"}, {"order_id": "o2", "city": "Giza", "country": "EG"}]
    assert s.upsert("fact_orders", rows, ["order_id"]) == 2
    sql, data = conn.many[0]
    assert sql == ("INSERT INTO fact_orders (order_id, city, country) VALUES (%s, %s, %s) "
                   "ON CONFLICT (order_id) DO UPDATE SET city = EXCLUDED.city, country = EXCLUDED.country")
    assert data == [("o1", "Cairo", "EG"), ("o2", "Giza", "EG")]
    assert conn.commits == 1


def test_postgres_key_only_do_nothing_composite_and_empty():
    conn = FakeConn()
    s = PostgresStore("postgresql://x", connection=conn)
    s.upsert("fact_order_lines", [{"order_id": "o", "line_no": 1}], ["order_id", "line_no"])
    assert conn.many[0][0].endswith("ON CONFLICT (order_id, line_no) DO NOTHING")
    assert s.upsert("fact_orders", [], ["order_id"]) == 0 and len(conn.many) == 1
    with pytest.raises(ValueError):
        s.upsert("fact_orders; x", [{"order_id": "o"}], ["order_id"])
    with pytest.raises(ValueError):
        s.upsert("dim_customer", [{"customer_key": 1, "customer_id": "c"}], ["customer_id"])


def test_postgres_query_execute_close():
    conn = FakeConn(result=[(1,)])
    s = PostgresStore("postgresql://x", connection=conn)
    assert s.query("SELECT %s", (1,)) == [(1,)]
    s.execute("DO $$ BEGIN END $$;")
    assert conn.statements[-1] == "DO $$ BEGIN END $$;" and conn.commits == 1
    s.close()
    assert conn.closed


# 7. get_store
def test_get_store_default_postgres_needs_dsn():
    with pytest.raises(StoreConfigError, match="PG_DSN"):
        get_store(env={})
    assert isinstance(get_store(env={"PG_DSN": "postgresql://x"}), PostgresStore)
    assert issubclass(StoreConfigError, ValueError)


def test_get_store_databricks_builds_store_with_fake_connect(monkeypatch):
    seen = {}
    monkeypatch.setattr(store_mod, "_connect_databricks",
                        lambda h, p, t: seen.update(h=h, p=p, t=t) or FakeConn(result=[(1,)]))
    env = {"GOLD_BACKEND": "databricks", "DATABRICKS_HOST": "https://adb-5.example.invalid",
           "DATABRICKS_TOKEN": TOKEN, "DATABRICKS_WAREHOUSE_ID": "w1", "DATABRICKS_CATALOG": "cat2"}
    s = get_store(env=env)
    assert isinstance(s, DatabricksStore) and s.catalog == "cat2"
    assert s.query("SELECT 1") == [(1,)]
    assert seen["h"] == "adb-5.example.invalid" and seen["p"] == "/sql/1.0/warehouses/w1" and seen["t"] == TOKEN
    env2 = dict(env, DATABRICKS_HTTP_PATH="/sql/1.0/endpoints/z")
    del env2["DATABRICKS_WAREHOUSE_ID"]
    get_store("databricks", env=env2).execute("SELECT 1")
    assert seen["p"] == "/sql/1.0/endpoints/z"


def test_get_store_missing_variables_listed_by_name_only():
    with pytest.raises(StoreConfigError) as ei:
        get_store("databricks", env={"DATABRICKS_TOKEN": TOKEN})
    msg = str(ei.value)
    assert "DATABRICKS_HOST" in msg and "DATABRICKS_WAREHOUSE_ID" in msg and "DATABRICKS_TOKEN" not in msg
    assert TOKEN not in msg
    with pytest.raises(StoreConfigError) as ei:
        get_store("databricks", env={"DATABRICKS_HOST": "secret-host.example.invalid"})
    assert "secret-host" not in str(ei.value) and "DATABRICKS_TOKEN" in str(ei.value)


def test_get_store_unknown_backend_and_env_honoured(monkeypatch):
    with pytest.raises(StoreConfigError, match="unknown"):
        get_store("oracle", env={})
    with pytest.raises(StoreConfigError, match="DATABRICKS_HOST"):
        get_store(env={"GOLD_BACKEND": "databricks"})  # env var selects the backend
    assert isinstance(get_store("postgres", env={"GOLD_BACKEND": "databricks", "PG_DSN": "x"}), PostgresStore)
    monkeypatch.setenv("GOLD_BACKEND", "postgres")
    monkeypatch.setenv("PG_DSN", "postgresql://y")
    assert isinstance(get_store(), PostgresStore)


# I-3: anchored retry regex, retry=False
@pytest.mark.parametrize("msg", ["HTTP 503", "HTTP/429", "HTTP429", "status=503", "status: 429", "status 503",
                                 "TooManyRequests", "[TEMPORARILY_UNAVAILABLE] try later", "read timed out",
                                 "ReadTimeout on socket", "ConnectTimeout", "Connection reset by peer",
                                 "Connection aborted"])
def test_each_anchored_transient_phrase_is_retried(msg):
    sleeps = []
    conn = FakeConn(failures=[Exception(msg)])
    dbx(conn, sleeper=sleeps.append).execute("SELECT 1")
    assert sleeps == [1] and len(conn.statements) == 2


def test_parse_error_with_503_and_429_in_sql_is_not_retried():
    sleeps = []
    err = Exception("[PARSE_SYNTAX_ERROR] near 'SELECT 503, 429 FROM t WHERE id = 5030 AND x = 4290'")
    conn = FakeConn(failures=[err])
    with pytest.raises(Exception, match="PARSE_SYNTAX_ERROR"):
        dbx(conn, sleeper=sleeps.append).execute("SELECT 503, 429")
    assert sleeps == [] and len(conn.statements) == 1


def test_retry_false_raises_immediately_on_execute_query_and_run_script():
    for call in (lambda s: s.execute("SELECT 1", retry=False), lambda s: s.query("SELECT 1", retry=False),
                 lambda s: s.run_script("SELECT 1; SELECT 2;", retry=False)):
        sleeps = []
        conn = FakeConn(failures=[Exception("HTTP 503")])
        with pytest.raises(Exception, match="503"):
            call(dbx(conn, sleeper=sleeps.append))
        assert sleeps == [] and len(conn.statements) == 1
    conn = FakeConn(failures=[Exception("HTTP 503")])
    dbx(conn).run_script("SELECT 1;")  # default keeps retrying
    assert len(conn.statements) == 2


def test_qualified_names_per_backend():
    assert PostgresStore("postgresql://x").qualified("silver_campaign_snapshots") == "silver_campaign_snapshots"
    dbx = DatabricksStore("https://h", TOKEN, warehouse_id="w", catalog="funnelstream", connection=object())
    assert dbx.qualified("silver_campaign_snapshots") == "funnelstream.silver.silver_campaign_snapshots"
    assert dbx.qualified("fact_orders") == "funnelstream.gold.fact_orders"
