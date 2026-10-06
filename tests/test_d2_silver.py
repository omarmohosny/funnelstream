"""D2 (spec section 18): district and page_url through Silver, publish, Gold model, DDL. Host tests, no Spark."""
import copy
import json
import random
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from contracts.geo import DISTRICTS
from scripts.seed_silver_reference import catalog_to_silver_rows
from simulator.campaigns import CAMPAIGNS
from simulator.catalog import make_catalog, make_customers
from simulator.core import journey
from simulator.orders import make_order
from streaming.silver.schemas import SILVER_EVENTS_COLUMNS, SILVER_ORDERS_COLUMNS
from streaming.silver.spark_schemas import SILVER_CUSTOMERS_COLUMNS
from streaming.silver.transform import count_district_violations, parse_page_url, transform_message
from warehouse.publish import BANNED_COLUMNS, PUBLISH_ALLOWLIST
from warehouse.tables import SILVER_MIRROR_TABLES, gold_table
from warehouse.unknown_members import UNKNOWN_ROWS

ROOT = Path(__file__).resolve().parent.parent
START = datetime(2026, 10, 1, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 20, tzinfo=timezone.utc)
RECEIVED = NOW
CATALOG = make_catalog(3)
CATALOG_IDS = {p["product_id"] for p in CATALOG}
CUSTOMERS = make_customers(3, 300)
URL = "https://shop.example.eg/p/prod_0042?utm_source=meta&utm_medium=paid_social&utm_campaign=ramadan_offer_1"


def tm(e, topic="app.events"):
    k = datetime.fromisoformat(e["event_time"]) + timedelta(seconds=1)
    return transform_message(json.dumps(e), topic, k, RECEIVED, known_products=CATALOG_IDS, now=NOW)


def _app_event():
    evs = journey(random.Random(6), CUSTOMERS[0], CATALOG, CAMPAIGNS[0], START)
    e = copy.deepcopy(next(x for x in evs if x["event_type"] == "product_view"))
    assert e["payload"]["page_url"]
    return e


def _row(e):
    rows = tm(e)
    assert [t for t, _ in rows] == ["silver_events"]
    return rows[0][1]


def _with(city, district):
    e = _app_event()
    e["city"] = city
    if district is None:
        e.pop("district", None)
    else:
        e["district"] = district
    return e


# ---- district ----
@pytest.mark.parametrize("city,given,want", [
    ("Cairo", "Maadi", "Maadi"),
    ("Cairo", "  nasr-city ", "Nasr City"),
    ("Cairo", "Madinet Nasr", "Nasr City"),
    ("cairo", "MAADI", "Maadi"),                      # city normalised first
    ("Cairo", "Sidi Gaber", "unknown"),               # Alexandria district under Cairo
    ("Cairo", "Atlantis", "unknown"),
    ("Cairo", None, "unknown"),
    ("Cairo", "", "unknown"),
    ("Other", "Other", "Other"),
    ("Other", "Maadi", "unknown"),
    ("Cairo", "Other", "unknown"),
    ("Atlantis", "Maadi", "unknown"),                 # unrecognised city: no district under it
])
def test_event_district_normalised_and_hierarchy_checked(city, given, want):
    row = _row(_with(city, given))
    assert row["district"] == want
    if want != "unknown":
        assert row["district"] in DISTRICTS[row["city"]]


def test_events_row_column_positions_and_every_simulated_event_has_valid_district():
    assert SILVER_EVENTS_COLUMNS.index("district") == SILVER_EVENTS_COLUMNS.index("city") + 1
    assert SILVER_EVENTS_COLUMNS.index("utm_medium") == SILVER_EVENTS_COLUMNS.index("utm_campaign") + 1
    assert SILVER_EVENTS_COLUMNS.index("page_url") == SILVER_EVENTS_COLUMNS.index("click_id") + 1
    n = 0
    for i, c in enumerate(CUSTOMERS[:40]):
        for e in journey(random.Random(i), c, CATALOG, None, START):
            row = tm(e)[0][1]
            assert list(row) == SILVER_EVENTS_COLUMNS
            assert row["district"] in DISTRICTS[row["city"]]
            n += 1
    assert n > 100


# ---- UTM from page_url ----
def test_utm_parsed_from_page_url_all_three():
    e = _app_event()
    e["payload"]["page_url"] = URL
    row = _row(e)
    assert (row["utm_source"], row["utm_medium"], row["utm_campaign"]) == ("meta", "paid_social", "ramadan_offer_1")
    assert row["page_url"] == URL


def test_utm_upper_case_and_padded_values_are_cleaned():
    e = _app_event()
    e["payload"]["page_url"] = ("https://x.eg/p/1?utm_source=%20META%20&utm_medium=PAID_Social"
                                "&utm_campaign=+Ramadan_Offer_1+")
    row = _row(e)
    assert (row["utm_source"], row["utm_medium"], row["utm_campaign"]) == ("meta", "paid_social", "ramadan_offer_1")
    assert row["page_url"].startswith("https://x.eg/p/1?utm_source=%20META")  # URL kept as sent


@pytest.mark.parametrize("url", ["https://shop.example.eg/p/prod_0042", "https://x.eg/?q=1&utm_source=&utm_medium=%20",
                                 "", "not a url at all", "http://[bad/p?utm_source=meta"])
def test_utm_null_without_usable_query_and_row_kept(url):
    e = _app_event()
    e["payload"]["page_url"] = url
    e["payload"]["utm_source"] = "stale"   # payload fields are ignored when page_url exists
    row = _row(e)
    assert (row["utm_source"], row["utm_medium"], row["utm_campaign"]) == (None, None, None)
    assert row["page_url"] == url


def test_payload_fallback_when_event_has_no_page_url():
    e = _app_event()
    e["payload"].pop("page_url")
    e["payload"].update(utm_source=" Meta ", utm_campaign="Old_Camp", utm_medium="ignored")
    row = _row(e)
    assert (row["utm_source"], row["utm_medium"], row["utm_campaign"], row["page_url"]) == (
        "meta", None, "old_camp", None)


def test_parse_page_url_never_raises():
    assert parse_page_url(None) == parse_page_url(5) == parse_page_url(["x"]) == (None, None, None)
    assert parse_page_url("http://[bad") == (None, None, None)
    assert parse_page_url(URL + "&utm_source=google") == ("meta", "paid_social", "ramadan_offer_1")  # first wins


def test_campaign_sessions_have_utm_only_on_landing_and_product_view():
    seen_utm, seen_plain = 0, 0
    for c in CUSTOMERS[:30]:
        for e in journey(random.Random(5), c, CATALOG, CAMPAIGNS[0], START):
            row = tm(e)[0][1]
            has = bool(e["payload"].get("page_url")) and "utm_campaign=" in e["payload"]["page_url"]
            assert (row["utm_campaign"] is not None) == has
            assert (row["utm_source"] is not None) == has and (row["utm_medium"] is not None) == has
            seen_utm += has
            seen_plain += not has
    assert seen_utm > 0 and seen_plain > 0


# ---- orders ----
def _order_created(city="Cairo", district="Heliopolis", camp=CAMPAIGNS[0]):
    evs = make_order(random.Random(9), CUSTOMERS[0], CATALOG, camp, START, "ord_x")
    e = copy.deepcopy(next(x for _, x in evs if x["event_type"] == "order_created"))
    e["city"] = city
    if district is None:
        e.pop("district", None)
    else:
        e["district"] = district
    return e


def test_order_row_carries_district_and_utm_medium():
    assert SILVER_ORDERS_COLUMNS.index("district") == SILVER_ORDERS_COLUMNS.index("city") + 1
    assert SILVER_ORDERS_COLUMNS.index("utm_medium") == SILVER_ORDERS_COLUMNS.index("utm_campaign") + 1
    e = _order_created()
    e["payload"].update(utm_source="META", utm_medium=" Paid_Social ", utm_campaign="Ramadan_Offer_1")
    rows = tm(e, "orders")
    order = next(r for t, r in rows if t == "silver_orders")
    assert list(order) == SILVER_ORDERS_COLUMNS
    assert (order["city"], order["district"]) == ("Cairo", "Heliopolis")
    assert (order["utm_source"], order["utm_medium"], order["utm_campaign"]) == ("meta", "paid_social", "ramadan_offer_1")


@pytest.mark.parametrize("city,district,want", [
    ("Cairo", "Sidi Gaber", "unknown"), ("Cairo", None, "unknown"), ("Other", "Other", "Other"),
    ("Cairo", "mAAdi", "Maadi"), ("Cairo", "Atlantis", "unknown")])
def test_order_district_rules_and_no_quarantine(city, district, want):
    rows = tm(_order_created(city, district), "orders")
    assert "silver_quarantine" not in [t for t, _ in rows]
    order = next(r for t, r in rows if t == "silver_orders")
    assert order["district"] == want


def test_no_quarantine_for_district_or_url_problems():
    for city, district in (("Cairo", "Atlantis"), ("Cairo", "Sidi Gaber"), ("zzz", "zzz")):
        assert _row(_with(city, district))["district"] == "unknown"
    e = _app_event()
    e["payload"]["page_url"] = "http://[bad"
    assert tm(e)[0][0] == "silver_events"


# ---- reported violation count ----
def test_count_district_violations():
    rows = [
        {"city": "Cairo", "district": "Maadi"},          # ok
        {"city": "Cairo", "district": "Madinet Nasr"},   # alias ok
        {"city": "Cairo", "district": "Sidi Gaber"},     # violation
        {"city": " alexandria ", "district": "Maadi"},   # violation, city normalised
        {"city": "Cairo", "district": "Atlantis"},       # violation (unrecognised, non-empty)
        {"city": "Cairo", "district": None},             # missing: not a violation
        {"city": "Cairo", "district": "  "},             # empty: not a violation
        {"city": "Other", "district": "Other"},          # ok
        {"district": "Maadi"}, {"city": None}, {},       # no city
    ]
    assert count_district_violations(rows) == 3
    assert count_district_violations([]) == 0
    assert count_district_violations([{"city": "Cairo", "district": 5}, {"city": ["x"], "district": "Maadi"}]) == 0


def test_violations_in_helper_counts_a_raw_message():
    from streaming.jobs.silver_clean import _violations_in  # imports pyspark only

    e = _with("Cairo", "Sidi Gaber")
    assert _violations_in(json.dumps(e)) == 1 and _violations_in(json.dumps(e).encode()) == 1
    assert _violations_in(json.dumps(_with("Cairo", "Maadi"))) == 0
    assert _violations_in("not json") == 0 and _violations_in(None) == 0 and _violations_in("[1]") == 0


# ---- customers seed ----
def test_seed_rows_carry_normalised_district_and_are_deterministic():
    customers = make_customers(3, 60)
    customers[0].update(city=" CAIRO ", district="madinet nasr")
    customers[1].update(city="Cairo", district="Sidi Gaber")
    customers[2].pop("district", None)
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    _, custs = catalog_to_silver_rows(CATALOG, customers, t0)
    assert list(custs[0]) == SILVER_CUSTOMERS_COLUMNS
    assert SILVER_CUSTOMERS_COLUMNS.index("district") == SILVER_CUSTOMERS_COLUMNS.index("city") + 1
    assert custs[0]["district"] == "Nasr City" and custs[1]["district"] == "unknown" and custs[2]["district"] == "unknown"
    for c in custs[3:]:
        assert c["district"] in DISTRICTS[c["city"]]
    assert catalog_to_silver_rows(CATALOG, customers, t0)[1] == custs
    assert all(not (set(c) & {"name", "phone"}) for c in custs)


# ---- publish allow-list ----
def test_publish_allowlist_has_new_columns_in_place_and_no_pii():
    ev, od, cu = (PUBLISH_ALLOWLIST[t] for t in ("silver_events", "silver_orders", "silver_customers"))
    assert ev == SILVER_EVENTS_COLUMNS and od == SILVER_ORDERS_COLUMNS and cu == SILVER_CUSTOMERS_COLUMNS
    for cols, pairs in ((ev, [("city", "district"), ("utm_campaign", "utm_medium"), ("click_id", "page_url")]),
                        (od, [("city", "district"), ("utm_campaign", "utm_medium")]),
                        (cu, [("city", "district")])):
        for a, b in pairs:
            assert cols.index(b) == cols.index(a) + 1
    for table, cols in PUBLISH_ALLOWLIST.items():
        for c in cols:
            assert c not in BANNED_COLUMNS or (table, c) == ("silver_products", "name")
    assert "raw_payload" not in PUBLISH_ALLOWLIST["silver_quarantine"]


def test_silver_mirror_tables_show_new_columns():
    mirror = {t.name: t for t in SILVER_MIRROR_TABLES}
    for t in ("silver_events", "silver_orders", "silver_customers"):
        assert mirror[t].column_names() == PUBLISH_ALLOWLIST[t]
        assert mirror[t].column("district").type == "string" and mirror[t].column("district").nullable
    assert mirror["silver_events"].column("page_url").nullable and mirror["silver_events"].column("utm_medium").nullable
    assert mirror["silver_orders"].column("utm_medium").nullable


# ---- Gold model, unknown member, DDL ----
def _after(cols, a, b):
    return cols.index(b) == cols.index(a) + 1


def test_gold_tables_have_district_after_city_and_agg_city_does_not():
    for t in ("dim_customer", "fact_orders", "fact_order_lines", "customer_360"):
        tab = gold_table(t)
        assert _after(tab.column_names(), "city", "district"), t
        assert tab.column("district").type == "string" and tab.column("district").nullable
    assert "district" not in gold_table("agg_city_product_daily").column_names()
    assert "tracked: city, district, primary_device" in gold_table("dim_customer").comment


def test_unknown_customer_member_district_is_unknown():
    row = UNKNOWN_ROWS["dim_customer"]
    assert row["district"] == "unknown" and list(row) == gold_table("dim_customer").column_names()


def test_ddl_files_contain_district_and_have_no_drift():
    pg = (ROOT / "warehouse" / "ddl.sql").read_text(encoding="utf-8")
    dbx = (ROOT / "warehouse" / "databricks_ddl.sql").read_text(encoding="utf-8")
    assert pg.count("district text") == 4          # dim_customer, fact_orders, fact_order_lines, customer_360
    assert dbx.count("district STRING") == 7       # the four Gold tables and three Silver mirror tables
    assert "page_url STRING" in dbx and "utm_medium STRING" in dbx
    r = subprocess.run([sys.executable, str(ROOT / "scripts" / "gen_ddl.py"), "--check"], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
