"""Gold build (spec section 16.2 and 17.1): one job that recomputes the Gold tables from the Silver mirror.

python -m warehouse.gold_build [--from-date D --to-date D | --days N | --full] [--steps a,b] [--dry-run]

Every step is a script of statements in warehouse/gold/NN_<step>.sql (NN is the position in STEPS), split by
warehouse.ddl_render.split_statements and run through store.execute. The SQL is Databricks SQL / Spark SQL with
Delta (MERGE); it is NOT portable to the Postgres fallback. Steps whose SQL file does not exist yet
(rec_product, checks) are named empty slots: the later tasks only drop their file in.

Placeholders in the SQL (anything else is rejected): {silver} and {gold} schema prefixes (store.schema_prefix,
for example funnelstream.silver), {from_date} and {to_date} (validated ISO dates, written as DATE'{from_date}'),
{campaigns_values}, {calendar_ranges}, {fixed_holidays}, {dim_date_start}, {dim_date_end}.

The window (from_date .. to_date, Cairo dates) selects what is recomputed; the full rules are in
docs/metrics.md ("Gold build rules"). No psycopg and no connector use here: only the GoldStore interface.
"""
import argparse
import os
import re
import sys
from datetime import date, datetime, timedelta

from warehouse.calendar_egypt import (DIM_DATE_END, DIM_DATE_START, calendar_ranges_values,
                                      fixed_holidays_values)
from warehouse.ddl_render import CATALOG, split_statements
from warehouse.store import StoreConfigError, get_store, sql_literal

GOLD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gold")
DEFAULT_DAYS = 7
FULL_FROM = date(1900, 1, 1)
FULL_TO = date(9999, 12, 31)

# The fixed order of spec 16.2. Position + 1 is the file number: warehouse/gold/<NN>_<name>.sql.
STEPS = [
    "dim_customer", "dim_product", "dim_campaign", "dim_date", "fact_orders", "fact_order_lines",
    "fact_attribution", "agg_orders_daily", "agg_funnel_daily", "agg_campaign_daily", "agg_city_product_daily",
    "rec_product", "checks",
]
_NO_TABLE = ("checks",)  # steps that have no target table to count

# Cairo dates are made with from_utc_timestamp(ts, 'Africa/Cairo') and then cast or formatted in the SESSION time
# zone, which is only right when that zone is UTC. Every step starts with this statement (so it also holds after a
# store reconnect between steps) and the first executed statement of a run is this one.
TIME_ZONE_SQL = "SET TIME ZONE 'UTC'"

ORDERS_WITHOUT_CREATED_AT_SQL = (
    "SELECT COUNT(DISTINCT o.order_id) FROM {silver}.silver_orders o "
    "JOIN {silver}.silver_order_status s ON s.order_id = o.order_id WHERE o.order_created_at IS NULL")

# Reported by the fact_attribution step (counts over the whole table, cheap). orders_without_click: orders whose own
# session has no usable click (the last_touch row does not carry the order's own click_id); orders_direct: orders
# with no campaign in the last_touch model. unresolved_clicks: first UTM events of non-bot sessions whose campaign
# neither silver_events.campaign_id nor dim_campaign resolves (same click rule as 07_fact_attribution.sql).
ATTRIBUTION_REPORTED_SQL = {
    "orders_without_click": (
        "SELECT COUNT(*) FROM {gold}.fact_attribution a JOIN {gold}.fact_orders f ON f.order_id = a.order_id "
        "WHERE a.model = 'last_touch' AND (a.click_id IS NULL OR f.click_id IS NULL OR a.click_id <> f.click_id)"),
    "orders_direct": (
        "SELECT COUNT(*) FROM {gold}.fact_attribution a WHERE a.model = 'last_touch' AND a.campaign_id IS NULL"),
    "unresolved_clicks": (
        "WITH cand AS (SELECT e.session_id, e.campaign_id, e.utm_campaign, e.event_time, e.event_id "
        "FROM {silver}.silver_events e WHERE e.session_id IS NOT NULL AND e.event_time IS NOT NULL "
        "AND e.utm_campaign IS NOT NULL AND e.event_type IN ('page_view', 'product_view')), "
        "timed AS (SELECT e.session_id, unix_micros(e.event_time) AS us FROM {silver}.silver_events e "
        "JOIN (SELECT DISTINCT session_id FROM cand) c ON c.session_id = e.session_id "
        "WHERE e.event_time IS NOT NULL), "
        "burst AS (SELECT session_id, COUNT(1) OVER (PARTITION BY session_id ORDER BY us "
        "RANGE BETWEEN 10000000 PRECEDING AND CURRENT ROW) AS n FROM timed), "
        "bots AS (SELECT DISTINCT session_id FROM burst WHERE n > 100), "
        "fc AS (SELECT x.* FROM (SELECT c.*, ROW_NUMBER() OVER (PARTITION BY c.session_id "
        "ORDER BY c.event_time, c.event_id) AS rn FROM cand c LEFT ANTI JOIN bots b "
        "ON b.session_id = c.session_id) x WHERE x.rn = 1) "
        "SELECT COUNT(*) FROM fc f WHERE f.campaign_id IS NULL AND NOT EXISTS ("
        "SELECT 1 FROM {gold}.dim_campaign d WHERE d.utm_campaign = f.utm_campaign AND d.campaign_id <> 'unknown')"),
}

_PLACEHOLDER_RE = re.compile(r"\{(\w+)\}")
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


class GoldBuildError(RuntimeError):
    """A step failed. .step names it, .results holds what finished before it; the cause is chained."""

    def __init__(self, step, cause, results=None):
        super().__init__("gold build failed at step %s: %s: %s" % (
            step, type(cause).__name__, " ".join(str(cause).split())[:300]))
        self.step = step
        self.results = results or {}


def parse_date(value, what="date") -> date:
    """ISO yyyy-mm-dd (a date or a string) to a date; anything else raises ValueError."""
    if isinstance(value, datetime):
        raise ValueError("%s must be a date, not a datetime: %r" % (what, value))
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not _DATE_RE.fullmatch(text):
        raise ValueError("%s must be an ISO date yyyy-mm-dd, got %r" % (what, text[:30]))
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise ValueError("%s is not a calendar date: %r" % (what, text)) from None


def cairo_today() -> date:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Africa/Cairo")).date()
    except Exception:  # no tz database: Egypt is UTC+2 (UTC+3 in summer time); good enough for a default window
        return (datetime.utcnow() + timedelta(hours=2)).date()


def resolve_window(from_date=None, to_date=None, days=None, full=False, today=None):
    """(from, to) as dates. full: everything. days=N: the last N days up to today. Neither: the last 7 days."""
    if full:
        if from_date or to_date or days:
            raise ValueError("--full cannot be combined with a date window or --days")
        return FULL_FROM, FULL_TO
    if days is not None:
        if from_date or to_date:
            raise ValueError("--days cannot be combined with --from-date/--to-date")
        if int(days) < 1:
            raise ValueError("--days must be at least 1")
    if from_date or to_date:
        if not (from_date and to_date):
            raise ValueError("--from-date and --to-date must be given together")
        start, end = parse_date(from_date, "from_date"), parse_date(to_date, "to_date")
        if start > end:
            raise ValueError("from_date %s is after to_date %s" % (start, end))
        return start, end
    end = today or cairo_today()
    return end - timedelta(days=(int(days) if days is not None else DEFAULT_DAYS) - 1), end


def campaigns_values() -> str:
    """VALUES rows for dim_campaign from simulator.campaigns.CAMPAIGNS (name = utm_campaign, no dates)."""
    from simulator.campaigns import CAMPAIGNS
    return ", ".join("(%s, %s, %s, %s, CAST(NULL AS DATE), CAST(NULL AS DATE))" % (
        sql_literal(c["campaign_id"]), sql_literal(c["utm_campaign"]), sql_literal(c["platform"]),
        sql_literal(c["utm_campaign"])) for c in CAMPAIGNS)


def placeholders(silver, gold, from_date, to_date) -> dict:
    return {
        "silver": silver, "gold": gold,
        "from_date": parse_date(from_date, "from_date").isoformat(),
        "to_date": parse_date(to_date, "to_date").isoformat(),
        "campaigns_values": campaigns_values(),
        "calendar_ranges": calendar_ranges_values(),
        "fixed_holidays": fixed_holidays_values(),
        "dim_date_start": DIM_DATE_START.isoformat(),
        "dim_date_end": DIM_DATE_END.isoformat(),
    }


def render_sql(text, values) -> str:
    """Replace every {name} with values[name]; an unknown name raises ValueError (nothing is left half-done)."""
    def sub(m):
        if m.group(1) not in values:
            raise ValueError("unknown placeholder {%s}" % m.group(1))
        return str(values[m.group(1)])
    return _PLACEHOLDER_RE.sub(sub, text)


def step_file(step) -> str:
    return os.path.join(GOLD_DIR, "%02d_%s.sql" % (STEPS.index(step) + 1, step))


def step_statements(step, values):
    """Rendered statements of one step, or None for an empty slot (no SQL file yet)."""
    path = step_file(step)
    if not os.path.isfile(path):
        return None
    with open(path, encoding="utf-8") as fh:
        return split_statements(render_sql(fh.read(), values))


def _schema_prefix(store, group):
    return store.schema_prefix(group)


def _count(store, gold, table):
    rows = store.query("SELECT count(*) FROM %s.%s" % (gold, table))
    return int(rows[0][0]) if rows else None


def run_build(store=None, from_date=None, to_date=None, full=False, dry_run=False, steps=None, days=None,
              today=None) -> dict:
    """Run the steps in the fixed order. Returns
    {"window": {"from", "to", "full"}, "dry_run", "steps": {name: {"status", "statements", "rows"}}, "reported"}.

    status: ok, empty (no SQL file yet), skipped (not in `steps`), dry-run. dry_run executes and queries nothing and
    puts the rendered statements in steps[name]["sql"]. The first failing step raises GoldBuildError (the cause
    is chained, .results holds the finished steps)."""
    if steps is not None:
        steps = list(steps)
        unknown = [s for s in steps if s not in STEPS]
        if unknown:
            raise ValueError("unknown step(s): %s (known: %s)" % (", ".join(unknown), ", ".join(STEPS)))
    start, end = resolve_window(from_date, to_date, days=days, full=full, today=today)
    owns_store = store is None
    if owns_store:
        store = get_store("databricks")
    try:
        silver, gold = _schema_prefix(store, "silver"), _schema_prefix(store, "gold")
        values = placeholders(silver, gold, start, end)
        out = {"window": {"from": start.isoformat(), "to": end.isoformat(), "full": bool(full)},
               "dry_run": bool(dry_run), "steps": {}, "reported": {}}
        # render everything first: a bad placeholder fails before any statement runs
        rendered = {name: step_statements(name, values) for name in STEPS if steps is None or name in steps}
        for name in STEPS:
            if name not in rendered:
                out["steps"][name] = {"status": "skipped", "statements": 0, "rows": None}
                continue
            statements = rendered[name]
            if statements is None:
                out["steps"][name] = {"status": "empty", "statements": 0, "rows": None}
                continue
            if dry_run:
                out["steps"][name] = {"status": "dry-run", "statements": len(statements), "rows": None,
                                      "sql": [TIME_ZONE_SQL] + statements}
                continue
            try:
                for stmt in [TIME_ZONE_SQL] + statements:
                    store.execute(stmt)
                rows = None if name in _NO_TABLE else _count(store, gold, name)
                if name == "fact_orders":
                    out["reported"]["orders_without_created_at"] = int(store.query(
                        render_sql(ORDERS_WITHOUT_CREATED_AT_SQL, values))[0][0])
                if name == "fact_attribution":
                    for key, text in ATTRIBUTION_REPORTED_SQL.items():
                        out["reported"][key] = int(store.query(render_sql(text, values))[0][0])
            except Exception as e:
                raise GoldBuildError(name, e, out["steps"]) from e
            out["steps"][name] = {"status": "ok", "statements": len(statements), "rows": rows}
        return out
    finally:
        if owns_store:
            store.close()


class _OfflineStore:
    """Only for --dry-run without Databricks settings: knows the schema prefixes, never connects."""

    def __init__(self, catalog):
        self._catalog = catalog

    def schema_prefix(self, group):
        return "%s.%s" % (self._catalog, group)

    def close(self):
        pass


def _print_result(result):
    w = result["window"]
    print("window: %s .. %s%s" % (w["from"], w["to"], " (full)" if w["full"] else ""))
    for name, r in result["steps"].items():
        if r["status"] == "dry-run":
            print("-- step %s: %d statement(s), nothing executed" % (name, r["statements"]))
            for i, stmt in enumerate(r["sql"], 1):
                print("-- %s statement %d" % (name, i))
                print(stmt)
            continue
        extra = "" if r["rows"] is None else " rows=%d" % r["rows"]
        print("%-24s %-8s statements=%d%s" % (name, r["status"], r["statements"], extra))
    for k, v in result["reported"].items():
        print("reported %s: %d" % (k, v))


def main(argv=None):
    ap = argparse.ArgumentParser(description="Recompute the Gold tables from the Silver mirror (Databricks).")
    ap.add_argument("--from-date", help="first Cairo date of the window (with --to-date), yyyy-mm-dd")
    ap.add_argument("--to-date", help="last Cairo date of the window, yyyy-mm-dd")
    ap.add_argument("--days", type=int, help="window = the last N days up to today (Cairo). Default: 7")
    ap.add_argument("--full", action="store_true", help="rebuild everything")
    ap.add_argument("--steps", help="comma separated subset of: " + ", ".join(STEPS) + " (always run in fixed order)")
    ap.add_argument("--dry-run", action="store_true", help="print the rendered statements, execute nothing")
    args = ap.parse_args(argv)
    steps = [s.strip() for s in args.steps.split(",") if s.strip()] if args.steps else None
    store = None
    try:
        try:
            store = get_store("databricks")
        except StoreConfigError:
            if not args.dry_run:
                raise
            from warehouse.store import env_catalog
            store = _OfflineStore(env_catalog() if os.environ.get("DATABRICKS_CATALOG") else CATALOG)
        result = run_build(store, from_date=args.from_date, to_date=args.to_date, full=args.full,
                           dry_run=args.dry_run, steps=steps, days=args.days)
    except (StoreConfigError, ValueError) as e:
        print("error: %s" % e, file=sys.stderr)
        return 2
    except GoldBuildError as e:
        print("error: %s" % e, file=sys.stderr)
        for name, r in e.results.items():
            print("  done before the failure: %s (%s)" % (name, r["status"]), file=sys.stderr)
        return 1
    finally:
        if store is not None:
            store.close()
    _print_result(result)
    if not args.dry_run:
        from warehouse.heartbeat import beat
        beat("gold_build", "steps: %s" % ",".join(n for n, r in result["steps"].items() if r["status"] == "ok"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
