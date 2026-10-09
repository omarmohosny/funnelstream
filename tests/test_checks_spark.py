"""SQL checks on local Spark with Delta (run in Docker: RUN_SPARK_TESTS=1).

The rendered queries of warehouse/checks.py run through warehouse.checks.run_checks against Delta tables built from the
simulator dataset (30,000 events, seed 11, noise on) by the real Gold build. Clean build: every `fail` check returns 0.
Then ONE known violation at a time is injected, the checks run again and exactly the expected checks fail with the
expected count. Injection is in place and undone with Delta RESTORE (a context manager), which equals working on a
copy and is much cheaper; every case starts from the clean tables.
"""
import json
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal

import pytest

pytestmark = pytest.mark.spark

from pyspark.sql import functions as F  # noqa: E402

from contracts.geo import DISTRICTS, district_violation  # noqa: E402
from scripts.seed_silver_reference import seed  # noqa: E402
from streaming.jobs.silver_clean import load_known_products, process_batch  # noqa: E402
from streaming.silver.writer import silver_path  # noqa: E402
from tests.test_gold_build_spark import (NO_CHECKS, SILVER_FACTS, ev, load, make_env,  # noqa: E402
                                         small_shuffles)  # noqa: F401  (small_shuffles is an autouse fixture)
from tests.test_gold_reference import (CATALOG, CUSTOMERS, NOW, bronze_df, order_row, simulated,  # noqa: E402
                                       status_row, t)
from warehouse import checks as ck  # noqa: E402
from warehouse.checks import CHECKS, run_checks  # noqa: E402
from warehouse.gold_build import placeholders, run_build  # noqa: E402

GOLD_ALL = ("dim_customer", "dim_product", "dim_campaign", "dim_date", "fact_orders", "fact_order_lines",
            "fact_attribution", "agg_orders_daily", "agg_funnel_daily", "agg_campaign_daily",
            "agg_city_product_daily", "silver_quarantine")  # silver_quarantine: make_env creates it by name
FAIL_NAMES = [c.name for c in CHECKS if c.severity == "fail"]


@pytest.fixture(scope="module")
def built(spark, tmp_path_factory):
    base = tmp_path_factory.mktemp("checks")
    lake = str(base / "lake")
    msgs, manifest = simulated(30_000)
    seed(spark, lake, datetime(2025, 1, 1, tzinfo=timezone.utc), CATALOG, CUSTOMERS)
    # like the real job (silver_clean.main): the product catalog is known, so unknown product ids are quarantined
    process_batch(spark, bronze_df(spark, msgs), lake, load_known_products(lake, spark), now=NOW)
    store = make_env(spark, str(base / "wh"), "silver_chk", "gold_chk", gold_tables=GOLD_ALL)
    for table in SILVER_FACTS + ("silver_quarantine",):
        load(spark, "silver_chk", table, df=spark.read.format("delta").load(silver_path(lake, table)))
    build = run_build(store, full=True, steps=NO_CHECKS)
    values = placeholders(store.silver, store.gold, "2026-01-01", "2026-12-31")
    manifest_file = base / "noise_manifest.json"
    manifest_file.write_text(json.dumps(manifest), encoding="utf-8")
    return {"store": store, "values": values, "build": build, "manifest": manifest, "manifest_path": str(manifest_file)}


def run_all(built, manifest_path=None):
    return run_checks(built["store"], built["values"], manifest_path=manifest_path, heartbeat=False)


def failed_values(out):
    return {r["name"]: r["value"] for r in out["results"] if r["status"] == "failed"}


def value_of(out, name):
    return next(r["value"] for r in out["results"] if r["name"] == name)


@contextmanager
def injected(spark, built, *tables):
    """Undo everything done to the named tables (e.g. "silver_orders", "fact_orders") by restoring the version they
    had on entry."""
    store = built["store"]
    full = {t_: "%s.%s" % (store.silver if t_.startswith("silver_") else store.gold, t_) for t_ in tables}
    version = {t_: spark.sql("DESCRIBE HISTORY %s" % f).agg(F.max("version")).first()[0] for t_, f in full.items()}
    try:
        yield store
    finally:
        for t_, f in full.items():
            spark.sql("RESTORE TABLE %s TO VERSION AS OF %d" % (f, version[t_]))


def sql(built, text):
    return built["store"].spark.sql(text.format(silver=built["store"].silver, gold=built["store"].gold))


def one(built, text):
    return sql(built, text).first()[0]


# ------------------------------------------------------------------ clean build
def test_clean_build_every_fail_check_returns_zero(spark, built):
    out = run_all(built)
    rows = {r["name"]: r["value"] for r in out["results"]}
    report = {r["name"]: r["value"] for r in out["results"] if r["severity"] == "report"}
    print("\nCHECKS run=%d fail_checks=%d failed=%s" % (len(CHECKS), len(FAIL_NAMES), out["failed"]))
    for r in out["results"]:
        print("CHECK %-45s %-8s %s" % (r["name"], r["status"], r["value"]))
    assert out["failed"] == [], {n: rows[n] for n in out["failed"]}
    assert out["status"] == "ok" and out["statements"] == len(CHECKS)
    assert len(FAIL_NAMES) > 35 and all(rows[n] == 0 for n in FAIL_NAMES)
    assert report["rows.fact_orders"] == built["build"]["steps"]["fact_orders"]["rows"] > 150
    assert report["rows.fact_order_lines"] == built["build"]["steps"]["fact_order_lines"]["rows"] > 300
    assert report["rows.dim_date"] == 1095
    assert 0 <= report["unknown_customer_share"] < 1
    assert 0 <= report["report.late_rate"] < 1 and 0 <= report["report.quarantine_share"] < 1
    assert 0 <= report["report.direct_attribution_share"] <= 1
    for name in ("report.negative_delivery_lead_hours", "report.cancelled_with_shipment", "report.open_over_14_days"):
        assert isinstance(report[name], int) and report[name] >= 0


def test_report_values_equal_a_direct_computation(spark, built):
    out = run_all(built)
    late = one(built, "SELECT AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) FROM {silver}.silver_events")
    assert value_of(out, "report.late_rate") == pytest.approx(float(late), abs=5e-5)
    n_q = one(built, "SELECT COUNT(*) FROM {silver}.silver_quarantine")
    n_all = n_q + sum(one(built, "SELECT COUNT(*) FROM {silver}.%s" % x)
                      for x in ("silver_events", "silver_orders", "silver_order_status"))
    assert value_of(out, "report.quarantine_share") == pytest.approx(n_q / n_all, abs=5e-5)
    assert value_of(out, "report.negative_delivery_lead_hours") == one(
        built, "SELECT COUNT(*) FROM {gold}.fact_orders WHERE delivery_lead_hours < 0")
    for t_ in ck.GOLD_KEYS:
        assert value_of(out, "rows." + t_) == one(built, "SELECT COUNT(*) FROM {gold}.%s" % t_)


# ------------------------------------------------------------------ manifest on the simulated run
def test_manifest_check_on_the_fresh_run(spark, built):
    out = run_all(built, built["manifest_path"])
    q = next(r for r in out["results"] if r["name"] == "manifest.quarantine_count")
    injected_ = built["manifest"]["injected"]
    print("\nMANIFEST expected=%d observed=%d status=%s injected=%s" % (q["expected"], q["value"], q["status"],
                                                                      injected_))
    assert q["expected"] == ck.expected_quarantine(injected_)
    assert q["value"] == one(built, "SELECT COUNT(*) FROM {silver}.silver_quarantine")
    assert q["status"] == "ok", q["message"]
    d = next(r for r in out["results"] if r["name"] == "manifest.duplicates")
    assert d["message"] == "duplicates: not checkable on the mirror"
    assert out["failed"] == []


def test_manifest_check_fails_when_the_manifest_does_not_match(spark, built, tmp_path):
    other = json.loads(json.dumps(built["manifest"]))
    other["injected"]["malformed_json"] += 20
    path = tmp_path / "other.json"
    path.write_text(json.dumps(other), encoding="utf-8")
    assert run_all(built, str(path))["failed"] == ["manifest.quarantine_count"]


# ------------------------------------------------------------------ one violation at a time
def clean_pick(built):
    """An order that has lines, a real customer key and a last_touch row."""
    return one(built, "SELECT MIN(f.order_id) FROM {gold}.fact_orders f WHERE f.customer_key <> -1 AND EXISTS "
                      "(SELECT 1 FROM {gold}.fact_order_lines l WHERE l.order_id = f.order_id)")


def case(spark, built, tables, action, expected):
    with injected(spark, built, *tables) as store:
        action(store)
        out = run_all(built)
        got = failed_values(out)
        print("\nINJECT %s -> failed %s" % (tables, got))
        assert got == expected
        assert out["status"] == "failed" and sorted(out["failed"]) == sorted(expected)
        assert len(out["results"]) == len(CHECKS) + 1  # every check still ran (plus the manifest note)


def test_duplicate_key(spark, built):
    def act(s):
        spark.sql("INSERT INTO {0}.silver_products SELECT * FROM {0}.silver_products LIMIT 1".format(s.silver))
    case(spark, built, ["silver_products"], act, {"dup_key.silver_products": 1})


def test_duplicate_key_in_gold(spark, built):
    def act(s):
        spark.sql("INSERT INTO {0}.dim_date SELECT * FROM {0}.dim_date LIMIT 2".format(s.gold))
    case(spark, built, ["dim_date"], act, {"dup_key.dim_date": 2})


def test_orphan_customer_key(spark, built):
    oid = clean_pick(built)

    def act(s):
        spark.sql("UPDATE %s.fact_orders SET customer_key = 987654 WHERE order_id = '%s'" % (s.gold, oid))
    case(spark, built, ["fact_orders"], act, {"orphan.fact_orders.customer_key": 1})


def test_orphan_product_key_and_line_customer_key(spark, built):
    oid = clean_pick(built)

    def act(s):
        spark.sql("UPDATE %s.fact_order_lines SET product_key = 987654 WHERE order_id = '%s' AND line_no = 1"
                  % (s.gold, oid))
        spark.sql("UPDATE %s.fact_order_lines SET customer_key = 987655 WHERE order_id = '%s' AND line_no = 1"
                  % (s.gold, oid))
    case(spark, built, ["fact_order_lines"], act, {"orphan.fact_order_lines.product_key": 1,
                                                   "orphan.fact_order_lines.customer_key": 1})


def test_orphan_attribution_order(spark, built):
    def act(s):
        spark.sql("INSERT INTO %s.fact_attribution VALUES ('ghost_order', 'last_touch', NULL, NULL, 1.00)" % s.gold)
    case(spark, built, ["fact_attribution"], act, {"orphan.fact_attribution.order_id": 1})


def test_order_total_mismatch(spark, built):
    oid = clean_pick(built)

    def act(s):
        spark.sql("UPDATE %s.fact_order_lines SET line_total = line_total + 1 WHERE order_id = '%s' AND line_no = 1"
                  % (s.gold, oid))
    case(spark, built, ["fact_order_lines"], act, {"recon.order_total_sum": 1, "recon.order_total_per_order": 1})


def test_order_total_changed_on_the_order_only_also_breaks_attribution_total(spark, built):
    oid = clean_pick(built)

    def act(s):
        spark.sql("UPDATE %s.fact_orders SET order_total = order_total + 1 WHERE order_id = '%s'" % (s.gold, oid))
    case(spark, built, ["fact_orders"], act, {"recon.order_total_sum": 1, "recon.order_total_per_order": 1,
                                              "recon.attribution_order_total": 2})


def test_third_attribution_row(spark, built):
    oid = clean_pick(built)

    def act(s):
        spark.sql("INSERT INTO {0}.fact_attribution SELECT order_id, 'view_through', campaign_id, click_id, order_total "
                  "FROM {0}.fact_attribution WHERE order_id = '{1}' AND model = 'last_touch'".format(s.gold, oid))
    case(spark, built, ["fact_attribution"], act, {"recon.attribution_two_rows": 1})


def test_attribution_total_and_campaign_mismatch(spark, built):
    oid = clean_pick(built)

    def act(s):
        spark.sql("UPDATE %s.fact_attribution SET order_total = order_total + 1 WHERE order_id = '%s' "
                  "AND model = 'first_touch'" % (s.gold, oid))
        spark.sql("UPDATE %s.fact_orders SET campaign_id = 'camp_zzz' WHERE order_id = '%s'" % (s.gold, oid))
    case(spark, built, ["fact_attribution", "fact_orders"], act,
         {"recon.attribution_order_total": 1, "recon.campaign_is_last_touch": 1})


def test_count_reconciliation_with_silver(spark, built):
    oid = clean_pick(built)

    def act(s):  # a silver order the Gold build does not have, and an extra silver item of a built order
        load(spark, s.silver, "silver_orders", [order_row("o_not_built", customer="x", created=t(2, 10))])
        load(spark, s.silver, "silver_order_status", [status_row("o_not_built", "order_created", t(2, 10))])
        load(spark, s.silver, "silver_order_items", [{
            "order_id": oid, "line_no": "99", "product_id": None, "quantity": 1, "unit_price": Decimal("5.00"),
            "line_total": Decimal("5.00"), "currency": "EGP", "order_time": t(1, 10)}])
    case(spark, built, ["silver_orders", "silver_order_status", "silver_order_items"], act,
         {"recon.orders_count": 1, "recon.lines_count": 1})


def test_silver_hygiene_violations(spark, built):
    eid = one(built, "SELECT MIN(event_id) FROM {silver}.silver_events")

    def act(s):
        load(spark, s.silver, "silver_events", [
            ev(1, "sx", t(3, 9), "page_view", event_id=None),                      # null event_id
            ev(2, "sx", t(3, 9), "page_view", event_time=None),                    # null event_time
            ev(3, "sx", t(3, 9), "product_view", product_id="p_not_in_products"),  # unknown product
        ])
        spark.sql("UPDATE %s.silver_events SET event_time = current_timestamp() + INTERVAL 1 DAY WHERE event_id = '%s'"
                  % (s.silver, eid))
        load(spark, s.silver, "silver_order_status", [status_row("o_any", "order_created", t(3, 9)),
                                                      dict(status_row("o_any", "order_created", t(3, 9)),
                                                           status="teleported")])
        load(spark, s.silver, "silver_order_items", [{
            "order_id": "o_any", "line_no": "1", "product_id": "p1", "quantity": 3, "unit_price": Decimal("10.00"),
            "line_total": Decimal("30.02"), "currency": "EGP", "order_time": t(3, 9)}, {      # 0.02 off: violation
            "order_id": "o_any", "line_no": "2", "product_id": "p1", "quantity": 3, "unit_price": Decimal("10.00"),
            "line_total": Decimal("30.01"), "currency": "EGP", "order_time": t(3, 9)}])       # 0.01 off: tolerated
        spark.sql("INSERT INTO %s.silver_campaign_snapshots VALUES "
                  "('c', DATE'2026-10-03', TIMESTAMP'2026-10-03 01:00:00', 'u', 'google', 5, 10, 0, 1.0000, 'EGP'), "
                  "('c', DATE'2026-10-03', TIMESTAMP'2026-10-03 02:00:00', 'u', 'google', 5, 5, 0, -0.5000, 'EGP')"
                  % s.silver)
    with injected(spark, built, "silver_events", "silver_order_status", "silver_order_items",
                  "silver_campaign_snapshots") as store:
        act(store)
        got = failed_values(run_all(built))
        print("\nINJECT silver hygiene -> failed %s" % got)
        assert got == {
            "silver.events_null_key_or_time": 2, "silver.events_future_time": 1, "silver.events_unknown_product": 1,
            "silver.order_status_not_allowed": 1, "silver.order_items_line_total": 1,
            "silver.snapshots_clicks_gt_impressions": 1, "silver.snapshots_negative_spend": 1}


def test_bad_district_pair_in_each_silver_table(spark, built):
    picks = [(table, key, one(built, "SELECT MIN(%s) FROM {silver}.%s" % (key, table)))
             for table, key in (("silver_orders", "order_id"), ("silver_customers", "customer_id"),
                                ("silver_events", "event_id"))]

    def act(s):
        for table, key, value in picks:  # Cairo / Dokki is a bad pair whatever the row had before
            spark.sql("UPDATE %s.%s SET city = 'Cairo', district = 'Dokki' WHERE %s = '%s'" % (s.silver, table, key, value))
    case(spark, built, ["silver_orders", "silver_customers", "silver_events"], act,
         {"district.silver_orders": 1, "district.silver_customers": 1, "district.silver_events": 1})


def test_scd2_two_current_rows_and_overlap(spark, built):
    cid = one(built, "SELECT MIN(customer_id) FROM {gold}.dim_customer WHERE customer_id <> 'unknown' AND is_current")
    pid = one(built, "SELECT MIN(product_id) FROM {gold}.dim_product WHERE product_id <> 'unknown' AND is_current")

    def act_two_current(s):
        spark.sql("INSERT INTO {0}.dim_customer SELECT 777001, customer_id, country, city, district, signup_date, "
                  "primary_device, marketing_consent, valid_from + INTERVAL 1 DAY, valid_to, TRUE "
                  "FROM {0}.dim_customer WHERE customer_id = '{1}'".format(s.gold, cid))
    case(spark, built, ["dim_customer"], act_two_current,
         {"scd2.dim_customer.one_current": 1, "scd2.dim_customer.no_overlap": 1})

    def act_overlap_only(s):
        spark.sql("INSERT INTO {0}.dim_product SELECT 777002, product_id, name, category, price, is_active, valid_from, "
                  "valid_from + INTERVAL 1 DAY, FALSE FROM {0}.dim_product WHERE product_id = '{1}'".format(s.gold, pid))
    case(spark, built, ["dim_product"], act_overlap_only, {"scd2.dim_product.no_overlap": 1})

    def act_no_current(s):
        spark.sql("UPDATE %s.dim_product SET is_current = FALSE WHERE product_id = '%s'" % (s.gold, pid))
    case(spark, built, ["dim_product"], act_no_current, {"scd2.dim_product.one_current": 1})


# ------------------------------------------------------------------ the district rule equals district_violation
def test_district_sql_equals_contracts_geo_district_violation(spark, built):
    pairs = []
    for city, districts in DISTRICTS.items():
        for d in districts:
            pairs += [(city, d), (city, d.upper()), (city, " %s " % d.lower()), (city, d.replace(" ", "-"))]
    pairs += [("Cairo", "el maadi"), ("Cairo", "El-Maadi"), ("Cairo", "EL MAADI"), ("Cairo", "madinet nasr"),
              ("Cairo", "5th Settlement"), ("Cairo", "Nasr  City"), ("Cairo", "tagamoa"), ("Giza", "giza"),
              ("Giza", "Giza Centre"), ("Giza", "pyramids"), ("Cairo", "pyramids"), ("Mansoura", "mansoura 1"),
              ("Tanta", "Tanta 2"), ("Assiut", "assiut 1"), ("Assiut", "tanta 1"), ("Giza", "Maadi"),
              ("Cairo", "Dokki"), ("Alexandria", "Zamalek"), ("Other", "Other"), ("Other", "other "),
              ("Cairo", "Other"), ("Giza", "Other"), ("Cairo", "unknown"), ("Cairo", "UNKNOWN"),
              ("Cairo", " Unknown "), ("Cairo", "unknown district"), ("Cairo", ""), ("Cairo", "   "),
              ("Cairo", "\t"), ("Cairo", " "), ("Cairo", "Maadi "), ("Cairo", "---"), ("Cairo", "?"),
              ("Cairo", "Maadi!"), ("Cairo", "Maa di"), ("Cairo", "12345"), ("Cairo", None), (None, "Maadi"),
              (None, None), ("Atlantis", "Maadi"), ("cairo", "Maadi"), ("CAIRO", "Maadi"), (" Cairo", "Maadi"),
              ("unknown", "Maadi"), ("", "Maadi"), ("Cairo", "Maadi"), ("Giza", "Dokki"), ("Cairo", "Madïna")]
    df = spark.createDataFrame([(i, c, d) for i, (c, d) in enumerate(pairs)], "id int, city string, district string")
    df.createOrReplaceTempView("district_probe")
    text = ck.district_violation_sql("silver_orders")
    assert text.startswith("SELECT COUNT(*) FROM {silver}.silver_orders t WHERE ")
    text = text.replace("SELECT COUNT(*) FROM {silver}.silver_orders t", "SELECT t.id FROM district_probe t")
    query = ck.render_sql(text, ck.check_values(built["values"]))
    got = {r[0] for r in spark.sql(query).collect()}
    want = {i for i, (c, d) in enumerate(pairs) if district_violation(c, d)}
    wrong = sorted((pairs[i], i in got) for i in got ^ want)
    print("\nDISTRICT pairs=%d violations_python=%d violations_sql=%d differences=%s" % (
        len(pairs), len(want), len(got), wrong))
    assert got == want, wrong
    assert len(want) > 8 and len(want) < len(pairs) - 40  # both outcomes are well represented
    assert ("Giza", "Maadi") in [pairs[i] for i in want] and ("Cairo", "Maadi") not in [pairs[i] for i in want]


def test_tables_are_clean_again_after_every_injection(spark, built):
    assert run_all(built)["failed"] == []
