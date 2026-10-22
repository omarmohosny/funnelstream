from datetime import datetime, timezone

import pytest

from contracts.validate import validate_event

NOW = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)


def order(**payload):
    base = {"order_id": "ord_1", "payment_method": "card", "order_total": 10.0,
            "items": [{"product_id": "prod_0001", "quantity": 1, "unit_price": 10.0, "line_total": 10.0}]}
    base.update(payload)
    return {"event_id": "evt_o1", "event_type": "order_created", "event_version": 1,
            "event_time": "2026-10-10T11:00:00+00:00", "order_id": "ord_1", "customer_id": "cust_1",
            "city": "Cairo", "district": "Maadi", "payload": base}


def test_an_order_without_the_new_fields_validates_as_before():
    assert validate_event(order(), NOW) == (True, "")


def test_a_standard_order_may_omit_both_fields_or_carry_them():
    assert validate_event(order(service="standard"), NOW) == (True, "")
    assert validate_event(order(service="standard", promised_minutes=20), NOW) == (True, "")


@pytest.mark.parametrize("minutes", [1, 20, 60])
def test_an_express_order_with_a_promise_passes(minutes):
    assert validate_event(order(service="express", promised_minutes=minutes), NOW) == (True, "")


@pytest.mark.parametrize("service", ["Express", "", "same_day", 1, True, ["express"]])
def test_an_unknown_service_is_bad_value(service):
    assert validate_event(order(service=service), NOW) == (False, "BAD_VALUE")


@pytest.mark.parametrize("minutes", [None, 0, 61, -5, 20.0, 20.5, "20", True, False, [20]])
def test_an_express_order_needs_an_integer_promise_from_1_to_60(minutes):
    assert validate_event(order(service="express", promised_minutes=minutes), NOW) == (False, "BAD_VALUE")


def test_an_express_order_without_the_promise_key_is_bad_value():
    assert validate_event(order(service="express"), NOW) == (False, "BAD_VALUE")


# ---- Silver ----
import json  # noqa: E402

from streaming.silver.schemas import TABLE_COLUMNS  # noqa: E402
from streaming.silver.transform import transform_message  # noqa: E402

KAFKA_TS = datetime(2026, 10, 10, 11, 0, 2, tzinfo=timezone.utc)


def run(event):
    return transform_message(json.dumps(event), "orders", KAFKA_TS, NOW, now=NOW)


def test_the_express_table_has_its_columns():
    assert TABLE_COLUMNS["silver_express_orders"] == ["order_id", "store_id", "promised_minutes", "created_at",
                                                      "event_date", "created_at_cairo"]


def test_an_express_order_adds_one_express_row_to_the_usual_rows():
    rows = run(order(service="express", promised_minutes=20))
    tables = sorted(t for t, _ in rows)
    assert tables == ["silver_express_orders", "silver_order_items", "silver_order_status", "silver_orders"]
    [row] = [r for t, r in rows if t == "silver_express_orders"]
    assert list(row) == TABLE_COLUMNS["silver_express_orders"]
    assert (row["order_id"], row["store_id"], row["promised_minutes"]) == ("ord_1", "store_cairo_maadi", 20)
    assert row["created_at"] == "2026-10-10T11:00:00+00:00" and row["event_date"] == "2026-10-10"


def test_a_standard_order_adds_no_express_row():
    for e in (order(), order(service="standard")):
        assert "silver_express_orders" not in [t for t, _ in run(e)]


def test_the_district_is_normalised_before_the_store_lookup():
    e = order(service="express", promised_minutes=15)
    e["district"] = " MAADI "
    [row] = [r for t, r in run(e) if t == "silver_express_orders"]
    assert row["store_id"] == "store_cairo_maadi" and row["promised_minutes"] == 15


@pytest.mark.parametrize("city,district", [("Assiut", "Assiut"), ("Other", None), ("Cairo", "Nowhere")])
def test_an_express_order_outside_a_served_district_is_quarantined(city, district):
    e = order(service="express", promised_minutes=20)
    e["city"], e["district"] = city, district
    [(table, row)] = run(e)
    assert table == "silver_quarantine" and row["error_code"] == "BAD_VALUE"
    assert row["error_detail"] == "express order outside a served district"


def test_an_invalid_express_promise_is_quarantined():
    [(table, row)] = run(order(service="express", promised_minutes=0))
    assert table == "silver_quarantine" and row["error_code"] == "BAD_VALUE"
