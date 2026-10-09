"""SQL data quality checks (spec section 10 and section 17 rule 10): the `checks` step of the Gold build.

Every check is a Check(name, group, severity, sql, description). `sql` is ONE read-only statement that returns ONE row
with ONE number: the violation count (severity "fail": any non-zero value fails the batch) or the value to print
(severity "report": printed, never fails). The SQL lives here, in Python, and uses only the placeholders {silver} and
{gold} plus {district_pairs} (the allowed (city, cleaned district) pairs, rendered from contracts/geo.py by
check_values()). tests/test_checks.py lints every check: placeholders known, one statement, no write keyword.

run_checks(store, values, manifest_path) runs every check with store.query and never raises for a failing check: the
full list is visible in the result. It raises GoldBuildError only for a technical error (a query that cannot run, an
unreadable manifest). The manifest check is a Python check (no SQL): see manifest_check().

Pure Python plus SQL text: no psycopg and no pyspark import (the table keys are repeated below, and a test compares
them with warehouse/tables.py).
"""
import json
import os
from dataclasses import dataclass

from contracts.geo import DISTRICTS, _LOOKUP
from contracts.validate import CLOCK_SKEW
from streaming.common.status import STATUS_RANK
from warehouse.gold_build import GoldBuildError, render_sql

CHECK_TAG = "/* funnelstream check %s */ "
SEVERITIES = ("fail", "report")
GROUPS = ("keys", "reconciliation", "silver_hygiene", "hierarchy_history", "reported", "manifest")
PLACEHOLDERS = ("silver", "gold", "district_pairs")

# --- thresholds (documented in docs/metrics.md, "Data quality checks") -----------------------------------------------
FUTURE_TOLERANCE_MINUTES = int(CLOCK_SKEW.total_seconds() // 60)  # the same clock skew the Silver validator allows
LINE_TOTAL_TOLERANCE = "0.01"  # EGP: rounding allowed between line_total and quantity * unit_price
OPEN_AFTER_DAYS = 14  # an order not delivered, cancelled or returned after this many days is "open"
MANIFEST_TOLERANCE_PERCENT = 2  # the quarantine count may differ from the manifest by this much (relative)
MANIFEST_QUARANTINE_KINDS = (
    "malformed_json", "missing_field", "unknown_type", "bad_version", "negative_price", "future_timestamp",
    "null_product_id", "unknown_product_id", "cancel_after_ship")

# Primary keys. Silver mirror keys and the built Gold tables, as in warehouse/tables.py (tests compare them).
# silver_quarantine has no key. rec_product and customer_360 are not built yet: add them here when their step exists.
SILVER_KEYS = {
    "silver_events": ("event_id",),
    "silver_orders": ("order_id",),
    "silver_order_status": ("event_id",),
    "silver_order_items": ("order_id", "line_no"),
    "silver_products": ("product_id", "updated_at"),
    "silver_customers": ("customer_id", "updated_at"),
    "silver_campaign_snapshots": ("campaign_id", "report_date", "pulled_at"),
}
GOLD_KEYS = {
    "dim_customer": ("customer_key",),
    "dim_product": ("product_key",),
    "dim_campaign": ("campaign_key",),
    "dim_date": ("date_key",),
    "fact_orders": ("order_id",),
    "fact_order_lines": ("order_id", "line_no"),
    "fact_attribution": ("order_id", "model"),
    "agg_orders_daily": ("date_key", "channel", "payment_method", "platform"),
    "agg_funnel_daily": ("event_date", "country", "channel", "platform"),
    "agg_campaign_daily": ("campaign_id", "report_date", "attribution_model"),
    "agg_city_product_daily": ("city", "product_id", "date_key"),
}


@dataclass(frozen=True)
class Check:
    name: str
    group: str
    severity: str  # "fail" or "report"
    sql: str
    description: str

    def __post_init__(self):
        if self.severity not in SEVERITIES:
            raise ValueError("severity must be fail or report: %s" % self.severity)
        if self.group not in GROUPS:
            raise ValueError("unknown check group: %s" % self.group)


# --------------------------------------------------------------------------------------------- group builders
def _dup_key(schema, table, keys):
    cols = ", ".join(keys)
    return Check(
        "dup_key.%s" % table, "keys", "fail",
        "SELECT COUNT(*) FROM (SELECT %s FROM {%s}.%s GROUP BY %s HAVING COUNT(*) > 1) d" % (cols, schema, table, cols),
        "Key values of %s (%s) that appear in more than one row." % (table, cols))


def _keys_group():
    out = [_dup_key("silver", t, k) for t, k in SILVER_KEYS.items()]
    out += [_dup_key("gold", t, k) for t, k in GOLD_KEYS.items()]
    out += [
        Check("orphan.fact_orders.customer_key", "keys", "fail",
              "SELECT COUNT(*) FROM {gold}.fact_orders f WHERE f.customer_key IS NULL OR (f.customer_key <> -1 AND "
              "NOT EXISTS (SELECT 1 FROM {gold}.dim_customer d WHERE d.customer_key = f.customer_key))",
              "fact_orders rows whose customer_key is NULL or not in dim_customer (-1 is the unknown member)."),
        Check("orphan.fact_order_lines.customer_key", "keys", "fail",
              "SELECT COUNT(*) FROM {gold}.fact_order_lines f WHERE f.customer_key IS NULL OR (f.customer_key <> -1 AND "
              "NOT EXISTS (SELECT 1 FROM {gold}.dim_customer d WHERE d.customer_key = f.customer_key))",
              "fact_order_lines rows whose customer_key is NULL or not in dim_customer (-1 is allowed)."),
        Check("orphan.fact_order_lines.product_key", "keys", "fail",
              "SELECT COUNT(*) FROM {gold}.fact_order_lines f WHERE f.product_key IS NULL OR (f.product_key <> -1 AND "
              "NOT EXISTS (SELECT 1 FROM {gold}.dim_product d WHERE d.product_key = f.product_key))",
              "fact_order_lines rows whose product_key is NULL or not in dim_product (-1 is allowed)."),
        Check("orphan.fact_order_lines.order_id", "keys", "fail",
              "SELECT COUNT(*) FROM {gold}.fact_order_lines l WHERE NOT EXISTS "
              "(SELECT 1 FROM {gold}.fact_orders o WHERE o.order_id = l.order_id)",
              "fact_order_lines rows whose order_id is not in fact_orders."),
        Check("orphan.fact_attribution.order_id", "keys", "fail",
              "SELECT COUNT(*) FROM {gold}.fact_attribution a WHERE NOT EXISTS "
              "(SELECT 1 FROM {gold}.fact_orders o WHERE o.order_id = a.order_id)",
              "fact_attribution rows whose order_id is not in fact_orders."),
        Check("unknown_customer_share", "keys", "report",
              "SELECT ROUND(SUM(CASE WHEN customer_key = -1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 4) "
              "FROM {gold}.fact_orders",
              "Share of fact_orders rows with the unknown customer key -1 (0 to 1)."),
    ]
    return out


def _reconciliation_group():
    g = "reconciliation"
    return [
        Check("recon.order_total_sum", g, "fail",
              "SELECT CASE WHEN (SELECT COALESCE(SUM(order_total), 0) FROM {gold}.fact_orders) = "
              "(SELECT COALESCE(SUM(line_total), 0) FROM {gold}.fact_order_lines) THEN 0 ELSE 1 END",
              "1 when SUM(fact_orders.order_total) differs from SUM(fact_order_lines.line_total)."),
        Check("recon.order_total_per_order", g, "fail",
              "SELECT COUNT(*) FROM {gold}.fact_orders f LEFT JOIN (SELECT order_id, SUM(line_total) AS s "
              "FROM {gold}.fact_order_lines GROUP BY order_id) l ON l.order_id = f.order_id "
              "WHERE COALESCE(f.order_total, 0) <> COALESCE(l.s, 0)",
              "Orders whose order_total differs from the sum of their line_total (no lines count as 0)."),
        Check("recon.orders_count", g, "fail",
              "SELECT ABS((SELECT COUNT(*) FROM {gold}.fact_orders) - (SELECT COUNT(DISTINCT o.order_id) "
              "FROM {silver}.silver_orders o WHERE o.order_created_at IS NOT NULL AND EXISTS "
              "(SELECT 1 FROM {silver}.silver_order_status s WHERE s.order_id = o.order_id)))",
              "Difference between the fact_orders rows and the distinct silver_orders that have a created time and a "
              "status row (the orders the Gold build includes)."),
        Check("recon.lines_count", g, "fail",
              "SELECT ABS((SELECT COUNT(*) FROM {gold}.fact_order_lines) - (SELECT COUNT(*) "
              "FROM {silver}.silver_order_items i WHERE EXISTS "
              "(SELECT 1 FROM {gold}.fact_orders f WHERE f.order_id = i.order_id)))",
              "Difference between the fact_order_lines rows and the silver_order_items rows whose order is in fact_orders."),
        Check("recon.attribution_two_rows", g, "fail",
              "SELECT COUNT(*) FROM {gold}.fact_orders f LEFT JOIN (SELECT order_id, COUNT(*) AS n, "
              "COUNT(DISTINCT CASE WHEN model IN ('last_touch', 'first_touch') THEN model END) AS m "
              "FROM {gold}.fact_attribution GROUP BY order_id) a ON a.order_id = f.order_id "
              "WHERE COALESCE(a.n, 0) <> 2 OR COALESCE(a.m, 0) <> 2",
              "Orders that do not have exactly one last_touch and one first_touch attribution row."),
        Check("recon.attribution_order_total", g, "fail",
              "SELECT COUNT(*) FROM {gold}.fact_attribution a JOIN {gold}.fact_orders f ON f.order_id = a.order_id "
              "WHERE NOT (a.order_total <=> f.order_total)",
              "Attribution rows whose order_total differs from fact_orders.order_total."),
        Check("recon.campaign_is_last_touch", g, "fail",
              "SELECT COUNT(*) FROM {gold}.fact_orders f LEFT JOIN {gold}.fact_attribution a "
              "ON a.order_id = f.order_id AND a.model = 'last_touch' WHERE NOT (f.campaign_id <=> a.campaign_id)",
              "Orders whose fact_orders.campaign_id differs from the last_touch campaign of that order."),
    ]


def _status_list():
    return ", ".join("'%s'" % s for s in sorted(STATUS_RANK, key=lambda s: (STATUS_RANK[s], s)))


def _silver_hygiene_group():
    g = "silver_hygiene"
    return [
        Check("silver.events_null_key_or_time", g, "fail",
              "SELECT COUNT(*) FROM {silver}.silver_events WHERE event_id IS NULL OR event_time IS NULL",
              "silver_events rows with a NULL event_id or event_time."),
        Check("silver.events_future_time", g, "fail",
              "SELECT COUNT(*) FROM {silver}.silver_events WHERE event_time > current_timestamp() + INTERVAL %d MINUTES"
              % FUTURE_TOLERANCE_MINUTES,
              "silver_events rows more than %d minutes in the future (the clock skew the Silver validator allows)."
              % FUTURE_TOLERANCE_MINUTES),
        Check("silver.events_unknown_product", g, "fail",
              "SELECT COUNT(*) FROM {silver}.silver_events e WHERE e.product_id IS NOT NULL AND NOT EXISTS "
              "(SELECT 1 FROM {silver}.silver_products p WHERE p.product_id = e.product_id)",
              "silver_events rows with a product_id that is not in silver_products (NULL product_id is allowed)."),
        Check("silver.order_status_not_allowed", g, "fail",
              "SELECT COUNT(*) FROM {silver}.silver_order_status WHERE status IS NULL OR status NOT IN (%s)"
              % _status_list(),
              "silver_order_status rows whose status is not one of the statuses of the Silver contract "
              "(streaming/common/status.py STATUS_RANK)."),
        Check("silver.order_items_line_total", g, "fail",
              "SELECT COUNT(*) FROM {silver}.silver_order_items WHERE ABS(line_total - quantity * unit_price) > %s"
              % LINE_TOTAL_TOLERANCE,
              "silver_order_items rows where line_total differs from quantity * unit_price by more than %s."
              % LINE_TOTAL_TOLERANCE),
        Check("silver.snapshots_clicks_gt_impressions", g, "fail",
              "SELECT COUNT(*) FROM {silver}.silver_campaign_snapshots WHERE clicks > impressions",
              "silver_campaign_snapshots rows with more clicks than impressions."),
        Check("silver.snapshots_negative_spend", g, "fail",
              "SELECT COUNT(*) FROM {silver}.silver_campaign_snapshots WHERE spend < 0",
              "silver_campaign_snapshots rows with negative spend."),
    ]


# The district rule is contracts.geo.district_violation, row by row: a non-blank district that is not 'unknown'
# (any case), whose city is a known canonical city, and that normalize_district() cannot place in that city. The
# cleaned form of the district is lower(), runs of anything outside a-z0-9 replaced by one space, trimmed (geo._key).
# (?U): \s means Unicode white space, like Python's str.strip (the few control characters 0x1c-0x1f that Python
# strips but Unicode does not call white space are the one known difference).
_BLANK = r"length(regexp_replace(%(c)s, '(?U)^\\s+|\\s+$', '')) = 0"
_UNKNOWN = r"lower(regexp_replace(%(c)s, '(?U)^\\s+|\\s+$', '')) = 'unknown'"
_CLEAN = "trim(regexp_replace(lower(%(c)s), '[^a-z0-9]+', ' '))"


def district_violation_sql(table, schema="silver"):
    """The district check of one Silver table, mirroring contracts.geo.district_violation."""
    col = "t.district"
    cond = ("t.district IS NOT NULL AND NOT (%s) AND NOT (%s) AND t.city IN (%s) AND NOT EXISTS ("
            "SELECT 1 FROM (VALUES {district_pairs}) AS p(city, dkey) WHERE p.city = t.city AND p.dkey = %s)" % (
                _BLANK % {"c": col}, _UNKNOWN % {"c": col},
                ", ".join("'%s'" % c for c in sorted(DISTRICTS)), _CLEAN % {"c": col}))
    return "SELECT COUNT(*) FROM {%s}.%s t WHERE %s" % (schema, table, cond)


def district_pairs_values():
    """VALUES rows (city, cleaned district) that normalize_district accepts: every lookup key whose district
    belongs to the city."""
    rows = []
    for city, districts in DISTRICTS.items():
        for key, district in sorted(_LOOKUP.items()):
            if district in districts:
                rows.append("('%s', '%s')" % (city, key))
    return ", ".join(rows)


def _scd2(table, bk):
    return [
        Check("scd2.%s.one_current" % table, "hierarchy_history", "fail",
              "SELECT COUNT(*) FROM (SELECT %s FROM {gold}.%s GROUP BY %s HAVING "
              "SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1) d" % (bk, table, bk),
              "%s values of %s that do not have exactly one is_current row." % (table, bk)),
        Check("scd2.%s.no_overlap" % table, "hierarchy_history", "fail",
              "SELECT COUNT(*) FROM {gold}.%(t)s a JOIN {gold}.%(t)s b ON a.%(bk)s = b.%(bk)s "
              "AND a.%(k)s < b.%(k)s AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to"
              % {"t": table, "bk": bk, "k": GOLD_KEYS[table][0]},
              "Pairs of %s versions of the same %s whose validity ranges overlap." % (table, bk)),
    ]


def _hierarchy_group():
    g = "hierarchy_history"
    out = [Check("district.%s" % t, g, "fail", district_violation_sql(t),
                 "%s rows with a district that is not a district of their city (contracts/geo.py "
                 "district_violation; blank, 'unknown' and an unknown city are not violations)." % t)
           for t in ("silver_customers", "silver_orders", "silver_events")]
    return out + _scd2("dim_customer", "customer_id") + _scd2("dim_product", "product_id")


def _reported_group():
    g = "reported"
    out = [
        Check("report.late_rate", g, "report",
              "SELECT ROUND(SUM(CASE WHEN is_late THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 4) "
              "FROM {silver}.silver_events",
              "Share of silver_events rows with is_late (0 to 1). Spec: alert above 0.05."),
        Check("report.quarantine_share", g, "report",
              "SELECT ROUND(q.n / NULLIF(q.n + e.n + o.n + s.n, 0), 4) FROM "
              "(SELECT COUNT(*) AS n FROM {silver}.silver_quarantine) q CROSS JOIN "
              "(SELECT COUNT(*) AS n FROM {silver}.silver_events) e CROSS JOIN "
              "(SELECT COUNT(*) AS n FROM {silver}.silver_orders) o CROSS JOIN "
              "(SELECT COUNT(*) AS n FROM {silver}.silver_order_status) s",
              "silver_quarantine rows / (quarantine + events + orders + order status rows) (0 to 1). Spec: alert "
              "above 0.02."),
        Check("report.negative_delivery_lead_hours", g, "report",
              "SELECT COUNT(*) FROM {gold}.fact_orders WHERE delivery_lead_hours < 0",
              "Orders with a negative delivery_lead_hours (delivered before they were created)."),
        Check("report.cancelled_with_shipment", g, "report",
              "SELECT COUNT(*) FROM {gold}.fact_orders f WHERE COALESCE(f.is_cancelled, FALSE) AND EXISTS "
              "(SELECT 1 FROM {silver}.silver_order_status s WHERE s.order_id = f.order_id "
              "AND s.status = 'order_shipped')",
              "Orders that count as cancelled and also have an order_shipped status row."),
        Check("report.open_over_%d_days" % OPEN_AFTER_DAYS, g, "report",
              "SELECT COUNT(*) FROM {gold}.fact_orders f WHERE NOT COALESCE(f.is_delivered, FALSE) "
              "AND NOT COALESCE(f.is_cancelled, FALSE) AND NOT COALESCE(f.is_returned, FALSE) "
              "AND f.order_created_at < current_timestamp() - INTERVAL %d DAYS" % OPEN_AFTER_DAYS,
              "Orders not delivered, cancelled or returned that were placed more than %d days ago (same rule as "
              "orders_open in agg_campaign_daily)." % OPEN_AFTER_DAYS),
        Check("report.direct_attribution_share", g, "report",
              "SELECT ROUND(SUM(CASE WHEN campaign_id IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 4) "
              "FROM {gold}.fact_attribution WHERE model = 'last_touch'",
              "Share of orders with no campaign in the last_touch model (0 to 1)."),
    ]
    out += [Check("rows.%s" % t, g, "report", "SELECT COUNT(*) FROM {gold}.%s" % t,
                  "Row count of %s (compare with what you expect after this run)." % t) for t in GOLD_KEYS]
    return out


def build_checks():
    return (_keys_group() + _reconciliation_group() + _silver_hygiene_group() + _hierarchy_group()
            + _reported_group())


CHECKS = build_checks()


def check_values(values):
    """The build placeholders plus the ones only the checks use."""
    out = dict(values)
    out["district_pairs"] = district_pairs_values()
    return out


def render_check(check, values):
    """The check query as it is sent: placeholders replaced, tagged with a leading comment (shows in the query
    history of the warehouse). Raises ValueError for an unknown placeholder."""
    return (CHECK_TAG % check.name) + render_sql(check.sql, check_values(values))


# --------------------------------------------------------------------------------------------- manifest check
class ManifestError(ValueError):
    """The noise manifest cannot be used (missing file, not JSON, wrong shape)."""


def load_manifest(path):
    """Read the noise manifest; returns the `injected` dict of counts. ManifestError for anything unusable."""
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        raise ManifestError("manifest file not found: %s" % path) from None
    except (OSError, ValueError) as e:  # JSONDecodeError and UnicodeDecodeError are ValueErrors
        raise ManifestError("manifest %s cannot be read: %s" % (path, type(e).__name__)) from None
    injected = data.get("injected") if isinstance(data, dict) else None
    if not isinstance(injected, dict):
        raise ManifestError("manifest %s has no 'injected' object" % path)
    for key in MANIFEST_QUARANTINE_KINDS + ("duplicate",):
        value = injected.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ManifestError("manifest %s: injected.%s must be a non-negative integer, got %r" % (
                path, key, value))
    return injected


def expected_quarantine(injected):
    return sum(int(injected[k]) for k in MANIFEST_QUARANTINE_KINDS)


def within_tolerance(observed, expected):
    """True when observed is within MANIFEST_TOLERANCE_PERCENT of expected, relative to expected (inclusive).
    An expected count of 0 allows exactly 0 (a relative tolerance of 0 rows)."""
    return abs(int(observed) - int(expected)) * 100 <= MANIFEST_TOLERANCE_PERCENT * int(expected)


MANIFEST_SKIPPED = "manifest: skipped (no --manifest)"
DUPLICATES_NOT_CHECKABLE = "duplicates: not checkable on the mirror"
QUARANTINE_COUNT_SQL = "SELECT COUNT(*) FROM {silver}.silver_quarantine"


def manifest_check(store, values, manifest_path):
    """Result entries of the manifest check: [] never; one 'skipped' entry without a path, else the quarantine
    comparison (fail) and the duplicates note (report). Technical problems raise ManifestError / the query error."""
    if not manifest_path:
        return [{"name": "manifest", "group": "manifest", "severity": "fail", "status": "skipped", "value": None,
                 "message": MANIFEST_SKIPPED, "description": MANIFEST_SKIPPED}]
    injected = load_manifest(manifest_path)
    expected = expected_quarantine(injected)
    rows = store.query(CHECK_TAG % "manifest.quarantine_count" + render_sql(QUARANTINE_COUNT_SQL, check_values(values)))
    observed = int(rows[0][0])
    ok = within_tolerance(observed, expected)
    message = "manifest quarantine: expected %d, observed %d, tolerance %d%% -> %s" % (
        expected, observed, MANIFEST_TOLERANCE_PERCENT, "ok" if ok else "FAILED")
    return [
        {"name": "manifest.quarantine_count", "group": "manifest", "severity": "fail",
         "status": "ok" if ok else "failed", "value": observed, "expected": expected, "message": message,
         "description": "silver_quarantine rows within %d%% of the manifest kinds (%s); expected 0 allows exactly 0."
                        % (MANIFEST_TOLERANCE_PERCENT, ", ".join(MANIFEST_QUARANTINE_KINDS))},
        {"name": "manifest.duplicates", "group": "manifest", "severity": "report", "status": "not checkable",
         "value": None, "message": DUPLICATES_NOT_CHECKABLE,
         "description": "The manifest says %d duplicates were injected; the Silver job drops them before they reach "
                        "the mirror and keeps no count, so the number cannot be compared." % int(injected["duplicate"])},
    ]


# --------------------------------------------------------------------------------------------- the run
def _number(value):
    if value is None:
        return None
    return value if isinstance(value, (int, float)) else float(value)  # Decimal and similar


def _run_one(store, check, values):
    rows = store.query(render_check(check, values))
    if not rows or len(rows[0]) != 1:
        raise ValueError("check %s must return one row with one column" % check.name)
    return _number(rows[0][0])


def run_checks(store, values, manifest_path=None, heartbeat=True) -> dict:
    """Run every check. Returns {"status": "ok"|"failed", "statements": N, "rows": None, "failed": [names],
    "results": [entry, ...]}. A failing check never raises; a query that cannot run raises GoldBuildError("checks")
    with .results holding what ran. After the run a best-effort heartbeat is written (never fails the run)."""
    results, statements, current = [], 0, None
    try:
        for check in CHECKS:
            statements += 1
            current = check.name
            value = _run_one(store, check, values)
            if check.severity == "fail":
                if value is None:
                    raise ValueError("check %s returned NULL" % check.name)
                status = "failed" if value != 0 else "ok"
            else:
                status = "report"
            results.append({"name": check.name, "group": check.group, "severity": check.severity, "status": status,
                            "value": value, "description": check.description})
        if manifest_path:
            statements += 1
            current = "manifest"
        results.extend(manifest_check(store, values, manifest_path))
    except Exception as e:
        err = GoldBuildError("checks", RuntimeError("check %s: %s: %s" % (current, type(e).__name__, e)))
        err.check_results = results  # what ran before the error
        raise err from e
    failed = [r["name"] for r in results if r["status"] == "failed"]
    out = {"status": "failed" if failed else "ok", "statements": statements, "rows": None, "failed": failed,
           "results": results}
    if heartbeat:
        _heartbeat(failed)
    return out


def _heartbeat(failed):
    try:
        from warehouse.heartbeat import beat
        beat("checks", "ok" if not failed else "failed: %s" % ",".join(failed))
    except Exception:
        pass


def dry_run_plan(values, manifest_path=None):
    """(names, rendered statements) for --dry-run: nothing is executed."""
    names = [c.name for c in CHECKS]
    sql = [render_check(c, values) for c in CHECKS]
    if manifest_path:
        names.append("manifest.quarantine_count")
        sql.append(CHECK_TAG % "manifest.quarantine_count" + render_sql(QUARANTINE_COUNT_SQL, check_values(values)))
    return names, sql


def format_results(step):
    """Lines that print a checks step result (used by gold_build and run_batch)."""
    lines = []
    for r in step.get("results", []):
        if r.get("message") and r["group"] == "manifest":
            lines.append("check %s" % r["message"])
            continue
        value = "n/a" if r["value"] is None else ("%.4f" % r["value"] if isinstance(r["value"], float) else str(r["value"]))
        mark = {"ok": "ok", "failed": "FAILED", "report": "report"}.get(r["status"], r["status"])
        lines.append("check %-42s %-17s %-6s value=%s" % (r["name"], r["group"], mark, value))
    n_fail = len(step.get("failed", []))
    lines.append("checks: %d run, %d failed%s" % (len(step.get("results", [])), n_fail,
                                                   " (%s)" % ", ".join(step["failed"]) if n_fail else ""))
    return lines
