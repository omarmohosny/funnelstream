"""Read-only DuckDB layer over the generated FunnelStream batch data.

Builds clean, PII-free views (the same rules as the design doc: latest-pull for
ads, SCD2 validity windows, no names/phones) that the LLM agent may query.
"""
from __future__ import annotations

import csv
import re
import shutil
import sys
import threading
from pathlib import Path
from datetime import date, datetime, timezone

import duckdb

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Documented in the view comments below; handed to the LLM as its schema context.
VIEWS_SQL = """
-- The four files are read once into tables (_campaigns, ...) so that file access can be switched off afterwards.
CREATE TABLE _campaigns AS
  SELECT * FROM read_csv_auto('{base}/master/campaigns.csv');
CREATE VIEW campaigns AS SELECT * FROM _campaigns;

-- SCD2 history. One row per snapshot where a tracked attribute (city, device) changed.
-- Names and phone numbers are deliberately NOT exposed.
CREATE TABLE _customers AS
  SELECT * FROM read_csv_auto('{base}/batch/customers/*.csv', union_by_name=true);
CREATE VIEW customer_snapshots AS
  SELECT customer_id, country, city, signup_date, primary_device,
         CAST(marketing_consent AS BOOLEAN) AS marketing_consent,
         CAST(updated_at AS TIMESTAMP) AS updated_at
  FROM _customers;

CREATE VIEW dim_customer AS
  WITH ch AS (
    SELECT *, lag(city) OVER w AS pc, lag(primary_device) OVER w AS pd
    FROM customer_snapshots WINDOW w AS (PARTITION BY customer_id ORDER BY updated_at)
  ), v AS (
    SELECT * FROM ch WHERE pc IS NULL OR pc IS DISTINCT FROM city OR pd IS DISTINCT FROM primary_device
  )
  SELECT customer_id, country, city, signup_date, primary_device, marketing_consent,
         updated_at AS valid_from,
         coalesce(lead(updated_at) OVER (PARTITION BY customer_id ORDER BY updated_at),
                  TIMESTAMP '9999-12-31') AS valid_to,
         lead(updated_at) OVER (PARTITION BY customer_id ORDER BY updated_at) IS NULL AS is_current
  FROM v;

CREATE TABLE _products AS
  SELECT * FROM read_csv_auto('{base}/batch/products/*.csv', union_by_name=true);
CREATE VIEW product_snapshots AS
  SELECT product_id, name, category, price, CAST(is_active AS BOOLEAN) AS is_active,
         CAST(updated_at AS TIMESTAMP) AS updated_at
  FROM _products;

CREATE VIEW dim_product AS
  WITH ch AS (
    SELECT *, lag(category) OVER w AS pc, lag(price) OVER w AS pp
    FROM product_snapshots WINDOW w AS (PARTITION BY product_id ORDER BY updated_at)
  ), v AS (
    SELECT * FROM ch WHERE pc IS NULL OR pc IS DISTINCT FROM category OR pp IS DISTINCT FROM price
  )
  SELECT product_id, name, category, price, is_active,
         updated_at AS valid_from,
         coalesce(lead(updated_at) OVER (PARTITION BY product_id ORDER BY updated_at),
                  TIMESTAMP '9999-12-31') AS valid_to,
         lead(updated_at) OVER (PARTITION BY product_id ORDER BY updated_at) IS NULL AS is_current
  FROM v;

-- Raw ad pulls, several per campaign/day, including noisy rows.
CREATE TABLE _ads AS
  SELECT * FROM read_csv_auto('{base}/batch/ads/*.csv', union_by_name=true);
CREATE VIEW ad_pulls AS
  SELECT * REPLACE (CAST(report_date AS DATE) AS report_date, CAST(pulled_at AS TIMESTAMP) AS pulled_at)
  FROM _ads;

-- Clean daily grain: latest pull wins; rows with clicks > impressions or negative spend dropped.
CREATE VIEW campaign_daily AS
  SELECT campaign_id, platform, campaign_name, utm_campaign, report_date,
         impressions, clicks, spend, currency, platform_conversions,
         round(100.0 * clicks / nullif(impressions, 0), 3) AS ctr_pct,
         round(spend / nullif(platform_conversions, 0), 2) AS platform_cpa
  FROM ad_pulls
  WHERE clicks <= impressions AND spend >= 0
  QUALIFY row_number() OVER (PARTITION BY campaign_id, report_date ORDER BY pulled_at DESC) = 1;
"""

_UNUSED = ["campaigns", "campaign_daily", "ad_pulls", "dim_customer",
             "customer_snapshots", "dim_product", "product_snapshots"]

# Never exposed to the model, whatever a Delta table contains (design doc: no PII, no raw payloads).
PII_COLUMNS = {"first_name", "last_name", "name_ar", "phone", "email", "address",
               "raw_payload", "raw_value", "raw_json", "raw_row"}
# Any column whose name contains one of these words is dropped as well, so a renamed column (phone_number,
# customer_name, address_line) cannot slip through. A bare "name" stays for products and is dropped for tables
# about people.
PII_WORDS = ("phone", "email", "address", "first_name", "last_name", "full_name", "customer_name", "name_ar",
             "raw_payload", "raw_value", "raw_json", "raw_row")
PERSON_TABLES = ("customer", "user", "person")


def keep_columns(names, table: str = "") -> list[str]:
    """Columns of a Delta table that the model may see (no names, phones, addresses or raw payloads)."""
    drop_bare_name = any(w in table.lower() for w in PERSON_TABLES)
    keep = []
    for c in names:
        low = c.lower()
        if low in PII_COLUMNS or any(w in low for w in PII_WORDS) or (low == "name" and drop_bare_name):
            continue
        keep.append(c)
    return keep


REQUIRED_FILES = ("master/campaigns.csv", "batch/customers/customers.csv", "batch/products/products.csv",
                  "batch/ads/ads.csv")


def resolve_base(scale: str) -> Path:
    """Folder with the generated batch files; generated on first use. A folder left half written by an interrupted
    run is rebuilt: generation happens in a temporary folder that is renamed only when it is complete."""
    base = ROOT / "data" / "generated" / scale
    if all((base / f).exists() for f in REQUIRED_FILES):
        return base
    if scale not in ("small", "medium", "large"):
        raise ValueError(f"Unknown dataset scale {scale!r}; choose small, medium, or large")
    tmp = base.parent / (scale + ".tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    generate_data(scale, tmp)
    shutil.rmtree(base, ignore_errors=True)
    tmp.rename(base)
    return base


def generate_data(scale: str, base: Path) -> None:
    """Create the LLM's small, PII-free batch dataset with this repo's simulator."""
    from simulator.catalog import make_catalog, make_customers
    from simulator.campaigns import CAMPAIGNS, campaign_numbers

    sizes = {"small": (500, 40), "medium": (5_000, 200), "large": (50_000, 1_000)}
    if scale not in sizes:
        raise ValueError(f"Unknown dataset scale {scale!r}; choose small, medium, or large")
    n_customers, n_products = sizes[scale]
    seed = 42
    customers = make_customers(seed, n_customers)
    products = make_catalog(seed, n_products)

    def write_csv(path: Path, rows: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else [])
            writer.writeheader()
            writer.writerows(rows)

    write_csv(base / "master" / "campaigns.csv", [
        {"campaign_id": c["campaign_id"], "platform": c["platform"],
         "campaign_name": c["utm_campaign"], "utm_campaign": c["utm_campaign"]}
        for c in CAMPAIGNS
    ])
    write_csv(base / "batch" / "customers" / "customers.csv", [
        {"customer_id": c["customer_id"], "country": c["country"], "city": c["city"],
         "signup_date": c["signup_date"], "primary_device": c["device_primary"],
         "marketing_consent": c["marketing_consent"], "updated_at": "2026-10-01T00:00:00"}
        for c in customers
    ])
    write_csv(base / "batch" / "products" / "products.csv", [
        {**p, "updated_at": "2026-10-01T00:00:00"} for p in products
    ])
    ads = []
    for day in (date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3)):
        as_of = datetime(day.year, day.month, day.day, 23, 59, tzinfo=timezone.utc)
        for c in CAMPAIGNS:
            ads.append({**campaign_numbers(c["campaign_id"], day, as_of, seed),
                        "platform": c["platform"], "campaign_name": c["utm_campaign"],
                        "utm_campaign": c["utm_campaign"]})
    write_csv(base / "batch" / "ads" / "ads.csv", ads)


def lock_down(con: duckdb.DuckDBPyConnection) -> None:
    """Switch off file, network and extension access and freeze the settings. After this the SQL the model writes can
    only read the in-memory tables and views, whatever functions it calls (read_text, read_blob, a quoted file name
    in FROM, sniff_csv, ATTACH, COPY ...). Call it once all data is loaded."""
    con.execute("SET enable_external_access=false")
    con.execute("SET lock_configuration=true")


def connect(scale: str = "small", lock: bool = True) -> duckdb.DuckDBPyConnection:
    """Batch CSV source (customers, products, campaigns, ad pulls). lock=False only for a caller that still has to
    load data and then calls lock_down itself."""
    con = duckdb.connect(":memory:")
    con.execute(VIEWS_SQL.format(base=resolve_base(scale).as_posix()))
    if lock:
        lock_down(con)
    return con


def connect_delta(lake: str | Path, layers=("silver", "gold")) -> duckdb.DuckDBPyConnection:
    """Kafka -> Bronze -> Silver (-> Gold) Delta tables.

    Registers every Delta table found under <lake>/<layer>/<table> as a view named
    after the table, minus PII columns. Bronze is skipped on purpose: it holds raw payloads.
    """
    from deltalake import DeltaTable  # imported lazily so the CSV mode needs no deltalake

    lake = Path(lake)
    con = duckdb.connect(":memory:")
    names = []
    for layer in layers:
        for t in sorted((lake / layer).glob("*/_delta_log")):
            path = t.parent
            ds = DeltaTable(str(path)).to_pyarrow_dataset()
            keep = keep_columns(ds.schema.names, path.name)
            con.register(f"_{path.name}", ds.to_table(columns=keep))
            con.execute(f"CREATE VIEW {path.name} AS SELECT * FROM _{path.name}")
            names.append(path.name)
    if not names:
        raise FileNotFoundError(f"No Delta tables under {lake}/{{{','.join(layers)}}} - run the Kafka->Bronze->Silver pipeline first.")
    lock_down(con)
    return con


_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|create|alter|attach|detach|copy|export|import|install|load|pragma|set|call)\b"
    r"|read_csv|read_parquet|read_json|read_text|read_blob|glob\(|\bfrom\s+'",
    re.I,
)
QUERY_TIMEOUT_SECONDS = 20.0


def run_readonly(con: duckdb.DuckDBPyConnection, sql: str, max_rows: int = 200,
                 timeout: float = QUERY_TIMEOUT_SECONDS):
    """Run a single SELECT/WITH statement against the views only. Returns (columns, rows, truncated).

    Four layers: DuckDB must parse the text as exactly one SELECT statement; a keyword filter rejects the usual
    suspects; the connection has file, network and extension access switched off (lock_down); and the query runs on
    its own cursor and is interrupted after `timeout` seconds, so a runaway query cannot hang a shared connection."""
    try:
        statements = con.extract_statements(sql.strip())
    except Exception as e:
        raise ValueError(f"Could not parse the query: {e}") from None
    if len(statements) != 1:
        raise ValueError("Only one statement is allowed.")
    if statements[0].type != duckdb.StatementType.SELECT:
        raise ValueError("Only SELECT/WITH queries are allowed.")
    stmt = sql.strip().rstrip(";")
    if _FORBIDDEN.search(stmt):
        raise ValueError("Query uses a forbidden keyword or direct file access; query the documented views only.")
    cur = con.cursor()
    timer = threading.Timer(timeout, cur.interrupt)
    timer.start()
    try:
        cur.execute(stmt)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchmany(max_rows + 1)
    except duckdb.InterruptException:
        raise ValueError(f"The query was stopped after {timeout:g} seconds; aggregate or filter more.") from None
    finally:
        timer.cancel()
        cur.close()
    return cols, rows[:max_rows], len(rows) > max_rows


def view_names(con) -> list[str]:
    rows = con.execute("SELECT table_name FROM information_schema.tables "
                       "WHERE table_schema='main' AND table_type='VIEW' ORDER BY 1").fetchall()
    return [r[0] for r in rows if not r[0].startswith("_")]


def describe(con: duckdb.DuckDBPyConnection) -> str:
    """Compact schema text for the system prompt, taken from the live views."""
    out = []
    for v in view_names(con):
        cols = con.execute(f"DESCRIBE {v}").fetchall()
        n = con.execute(f"SELECT count(*) FROM {v}").fetchone()[0]
        out.append(f"{v} ({n} rows): " + ", ".join(f"{c[0]} {c[1]}" for c in cols))
    return "\n".join(out)
