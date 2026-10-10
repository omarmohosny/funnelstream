from datetime import datetime, timezone

import pytest

from contracts.validate import INVENTORY_EVENT_TYPES, KNOWN_TYPES, STOCK_REASONS, validate_event

NOW = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)


def snapshot(**over):
    e = {"event_id": "evt_s1", "event_type": "stock_snapshot", "event_version": 1,
         "event_time": "2026-10-10T11:00:00+00:00", "store_id": "store_cairo_maadi", "product_id": "prod_0001",
         "payload": {"on_hand": 40, "reserved_qty": 0, "reorder_point": 10}}
    e.update(over)
    return e


def movement(**over):
    e = {"event_id": "evt_m1", "event_type": "stock_movement", "event_version": 1,
         "event_time": "2026-10-10T11:00:05+00:00", "store_id": "store_cairo_maadi", "product_id": "prod_0001",
         "payload": {"qty_delta": -2, "reason": "sold", "order_id": "ord_1", "requested_qty": 2}}
    e.update(over)
    return e


def with_payload(base, **payload):
    out = dict(base)
    out["payload"] = {**base["payload"], **payload}
    return out


def test_the_two_types_are_known():
    assert INVENTORY_EVENT_TYPES == {"stock_snapshot", "stock_movement"}
    assert INVENTORY_EVENT_TYPES <= KNOWN_TYPES
    assert STOCK_REASONS == {"received", "sold", "shrinkage", "adjustment"}


def test_valid_events_pass():
    assert validate_event(snapshot(), NOW) == (True, "")
    assert validate_event(movement(), NOW) == (True, "")
    assert validate_event(with_payload(movement(), qty_delta=60, reason="received", order_id=None), NOW) == (True, "")
    assert validate_event(with_payload(movement(), qty_delta=2, reason="adjustment", order_id=None), NOW) == (True, "")
    assert validate_event(with_payload(snapshot(), reserved_qty=None), NOW) == (True, "")  # reserved defaults to 0


@pytest.mark.parametrize("field", ["store_id", "product_id"])
def test_missing_store_or_product_is_missing_field(field):
    assert validate_event(snapshot(**{field: None}), NOW) == (False, "MISSING_FIELD")
    assert validate_event(movement(**{field: ""}), NOW) == (False, "MISSING_FIELD")


def test_a_flat_event_without_payload_is_missing_field():
    flat = snapshot()
    del flat["payload"]
    assert validate_event(flat, NOW) == (False, "MISSING_FIELD")


@pytest.mark.parametrize("bad", [-1, 1.5, "3", True])
def test_snapshot_on_hand_must_be_a_non_negative_integer(bad):
    assert validate_event(with_payload(snapshot(), on_hand=bad), NOW) == (False, "BAD_VALUE")


def test_snapshot_without_on_hand_is_missing_field():
    e = snapshot()
    del e["payload"]["on_hand"]
    assert validate_event(e, NOW) == (False, "MISSING_FIELD")


@pytest.mark.parametrize("delta", [0, 1.5, "2", None])
def test_movement_qty_delta_must_be_a_non_zero_integer(delta):
    code = "MISSING_FIELD" if delta is None else "BAD_VALUE"
    assert validate_event(with_payload(movement(), qty_delta=delta), NOW) == (False, code)


def test_movement_reason_and_sign_rules():
    assert validate_event(with_payload(movement(), reason="stolen"), NOW) == (False, "BAD_VALUE")
    assert validate_event(with_payload(movement(), qty_delta=2), NOW) == (False, "BAD_VALUE")  # sold must be negative
    assert validate_event(with_payload(movement(), qty_delta=-5, reason="received", order_id=None), NOW) == (False, "BAD_VALUE")
    assert validate_event(with_payload(movement(), qty_delta=2, reason="shrinkage", order_id=None), NOW) == (False, "BAD_VALUE")


def test_sold_needs_an_order_id_and_requested_qty_must_be_positive():
    assert validate_event(with_payload(movement(), order_id=None), NOW) == (False, "BAD_VALUE")
    assert validate_event(with_payload(movement(), requested_qty=0), NOW) == (False, "BAD_VALUE")


def test_future_timestamp_and_unknown_product():
    assert validate_event(snapshot(event_time="2026-10-10T13:00:00+00:00"), NOW) == (False, "FUTURE_TIMESTAMP")
    assert validate_event(snapshot(), NOW, known_products={"prod_0002"}) == (False, "UNKNOWN_PRODUCT")
    assert validate_event(snapshot(), NOW, known_products={"prod_0001"}) == (True, "")


def test_wrong_version_is_bad_version():
    assert validate_event(snapshot(event_version=2), NOW) == (False, "BAD_VERSION")
