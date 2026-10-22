"""The Cairo-time conversion is a Silver transformation: every Silver timestamp has a *_cairo twin."""
import glob
import json
import os
from datetime import datetime, timezone

from contracts.normalize import to_cairo_naive
from streaming.silver.schemas import CAIRO_TWINS, TABLE_COLUMNS
from streaming.silver.spark_schemas import ALL_COLUMNS, coerce_row
from streaming.silver.transform import transform_message

T = datetime(2026, 10, 5, 22, 30, tzinfo=timezone.utc)  # 01:30 on 6 October in Cairo (UTC+3)


def test_to_cairo_naive_follows_egypts_daylight_saving():
    assert to_cairo_naive("2026-10-05T22:30:00Z") == datetime(2026, 10, 6, 1, 30)       # summer, UTC+3
    assert to_cairo_naive("2026-12-05T22:30:00+00:00") == datetime(2026, 12, 6, 0, 30)  # winter, UTC+2
    assert to_cairo_naive(datetime(2026, 12, 5, 22, 30)) == datetime(2026, 12, 6, 0, 30)  # naive means UTC
    # the clocks go back at 24:00 Cairo on the last Thursday of October: that hour happens twice
    assert to_cairo_naive("2026-10-29T20:59:00Z") == datetime(2026, 10, 29, 23, 59)
    assert to_cairo_naive("2026-10-29T21:00:00Z") == datetime(2026, 10, 29, 23, 0)
    assert to_cairo_naive("2026-10-29T22:00:00Z") == datetime(2026, 10, 30, 0, 0)


def test_every_twin_names_a_utc_column_of_the_same_table():
    for table, cols in ALL_COLUMNS.items():
        for c in cols:
            if c in CAIRO_TWINS:
                assert CAIRO_TWINS[c] in cols, (table, c)
    for table, cols in ALL_COLUMNS.items():  # no business timestamp of a Silver table is left without a twin
        for twin, source in CAIRO_TWINS.items():
            if source in cols and not (source == "kafka_timestamp" and table != "silver_events"):
                assert twin in cols, (table, source)


def _order_event(event_time):
    return json.dumps({
        "event_id": "e1", "event_type": "order_created", "event_version": 1, "event_time": event_time,
        "session_id": "s1", "customer_id": "c1", "order_id": "o1", "city": "Cairo", "district": "Nasr City",
        "payload": {"session_id": "s1", "payment_method": "cod", "order_total": 10,
                    "items": [{"product_id": "p1", "quantity": 1, "unit_price": 10, "line_total": 10}]}})


def test_an_order_gets_its_cairo_time_in_every_row_it_produces():
    rows = dict(transform_message(_order_event("2026-10-05T22:30:00Z"), "orders", T, T))
    assert rows["silver_orders"]["order_created_at_cairo"] == datetime(2026, 10, 6, 1, 30)
    assert rows["silver_order_status"]["status_time_cairo"] == datetime(2026, 10, 6, 1, 30)
    assert rows["silver_order_items"]["order_time_cairo"] == datetime(2026, 10, 6, 1, 30)
    # the day column agrees with the Cairo time
    assert rows["silver_orders"]["event_date"] == rows["silver_orders"]["order_created_at_cairo"].date().isoformat()


def test_an_app_event_gets_cairo_event_and_kafka_times():
    raw = json.dumps({"event_id": "a1", "event_type": "page_view", "event_version": 1,
                      "event_time": "2026-10-05T22:30:00Z", "session_id": "s1", "customer_id": "c1",
                      "city": "Cairo", "district": "Maadi", "platform": "web", "channel": "organic",
                      "payload": {"page_url": "https://x.eg/"}})
    [(table, row)] = transform_message(raw, "app.events", T, T)
    assert table == "silver_events"
    assert row["event_time_cairo"] == datetime(2026, 10, 6, 1, 30)
    assert row["kafka_timestamp_cairo"] == datetime(2026, 10, 6, 1, 30)


def test_coerce_row_derives_a_missing_twin_and_keeps_a_given_one():
    base = {"order_id": "o1", "order_created_at": "2026-10-05T22:30:00+00:00", "event_date": "2026-10-06"}
    assert coerce_row("silver_orders", base)["order_created_at_cairo"] == datetime(2026, 10, 6, 1, 30)
    given = dict(base, order_created_at_cairo=datetime(2030, 1, 1))
    assert coerce_row("silver_orders", given)["order_created_at_cairo"] == datetime(2030, 1, 1)
    assert coerce_row("silver_orders", {"order_id": "o2"})["order_created_at_cairo"] is None


def test_gold_sql_reads_the_silver_cairo_columns_instead_of_converting():
    sql = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "warehouse", "gold")
    for path in sorted(glob.glob(os.path.join(sql, "*.sql"))):
        name = os.path.basename(path)
        text = open(path, encoding="utf-8").read()
        # customer_360 converts a Gold column (last_order_at) and the clock, which Silver does not hold
        if name.startswith("13_"):
            continue
        assert "from_utc_timestamp" not in text, name
