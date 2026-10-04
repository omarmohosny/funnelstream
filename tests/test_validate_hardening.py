from datetime import datetime, timezone

import pytest

from contracts.validate import validate_event, validate_raw

NOW = datetime(2026, 10, 4, 12, 0, tzinfo=timezone.utc)


def ev(**kw):
    e = {"event_id": "e1", "event_type": "page_view", "event_version": 1,
         "event_time": "2026-10-04T10:00:00+00:00", "session_id": "sess_1", "order_id": None,
         "payload": {"url": "/x"}}
    e.update(kw)
    return e


def test_baseline_valid():
    assert validate_event(ev(), now=NOW) == (True, "")


@pytest.mark.parametrize("kw", [
    {"event_time": 12345}, {"event_time": "not a date"}, {"payload": "x"}, {"payload": None},
    {"payload": {"price": "abc"}}, {"payload": {"price": True}}, {"event_version": [1]},
    {"event_type": ["a"]}, {"event_time": ["x"]},
])
def test_never_raises(kw):
    ok, code = validate_event(ev(**kw), now=NOW)
    assert ok is False and code


def test_bad_value_codes():
    assert validate_event(ev(event_time=12345), now=NOW) == (False, "BAD_VALUE")
    assert validate_event(ev(event_time="not a date"), now=NOW) == (False, "BAD_VALUE")
    assert validate_event(ev(payload="x"), now=NOW) == (False, "BAD_VALUE")
    assert validate_event(ev(payload={"price": "abc"}), now=NOW) == (False, "BAD_VALUE")
    assert validate_event(ev(payload={"price": True}), now=NOW) == (False, "BAD_VALUE")


def test_numeric_string_price():
    assert validate_event(ev(payload={"price": "79.9"}), now=NOW) == (True, "")
    assert validate_event(ev(payload={"price": "-5"}), now=NOW) == (False, "NEGATIVE_PRICE")
    assert validate_event(ev(payload={"price": -5}), now=NOW) == (False, "NEGATIVE_PRICE")


def test_offset_and_z_timestamps():
    assert validate_event(ev(event_time="2026-10-04T12:00:00+02:00"), now=NOW)[0]
    assert validate_event(ev(event_time="2026-10-04T10:00:00Z"), now=NOW)[0]
    assert validate_event(ev(event_time="2026-10-04T10:00:00"), now=NOW)[0]
    # 13:00+02:00 is 11:00 UTC: past; 15:00+02:00 is 13:00 UTC: future
    assert validate_event(ev(event_time="2026-10-04T15:00:00+02:00"), now=NOW) == (False, "FUTURE_TIMESTAMP")


def test_validate_raw():
    assert validate_raw("{bad", now=NOW) == (False, "BAD_JSON")
    assert validate_raw("[]", now=NOW) == (False, "BAD_JSON")
    assert validate_raw(b"\xff", now=NOW) == (False, "BAD_JSON")
    assert validate_raw(b'{"a": 1', now=NOW) == (False, "BAD_JSON")
    assert validate_raw(ev(), now=NOW) == (True, "")
    import json
    assert validate_raw(json.dumps(ev()), now=NOW) == (True, "")
    assert validate_raw(json.dumps(ev()).encode(), now=NOW) == (True, "")


def test_product_checks():
    pv = ev(event_type="product_view", payload={"product_id": "p1", "price": 10})
    assert validate_event(pv, now=NOW)[0]
    assert validate_event(ev(event_type="product_view", payload={"product_id": None}), now=NOW) == (False, "MISSING_FIELD")
    assert validate_event(ev(event_type="product_view", payload={"product_id": ""}), now=NOW) == (False, "MISSING_FIELD")
    assert validate_event(ev(event_type="add_to_cart", payload={}), now=NOW) == (False, "MISSING_FIELD")
    assert validate_event(pv, now=NOW, known_products={"p1"})[0]
    assert validate_event(pv, now=NOW, known_products={"p2"}) == (False, "UNKNOWN_PRODUCT")
    assert validate_raw(pv, now=NOW, known_products={"p2"}) == (False, "UNKNOWN_PRODUCT")


def test_previous_codes():
    assert validate_event(ev(event_id=None), now=NOW) == (False, "MISSING_FIELD")
    assert validate_event(ev(session_id=None), now=NOW) == (False, "MISSING_FIELD")
    assert validate_event(ev(event_type="zzz"), now=NOW) == (False, "UNKNOWN_EVENT_TYPE")
    assert validate_event(ev(event_version=2), now=NOW) == (False, "BAD_VERSION")
    assert validate_event(ev(event_time="2026-10-05T10:00:00+00:00"), now=NOW) == (False, "FUTURE_TIMESTAMP")
    assert validate_event(ev(event_type="order_created", session_id=None, order_id=None), now=NOW) == (False, "MISSING_FIELD")
