from datetime import datetime, timedelta, timezone
from contracts.validate import validate_event

NOW = datetime(2026, 10, 4, 12, 0, tzinfo=timezone.utc)


def base(event_type="product_view", **extra):
    e = {
        "event_id": "evt_1",
        "event_type": event_type,
        "event_version": 1,
        "event_time": NOW.isoformat(),
        "session_id": "sess_1",
        "device": "mobile",
        "channel": "paid_social",
        "country": "EG",
        "city": "Cairo",
        "payload": {"product_id": "prod_501", "price": 79.9},
    }
    e.update(extra)
    return e


def test_valid_event_passes():
    assert validate_event(base(), now=NOW) == (True, "")


def test_missing_session_fails():
    e = base()
    del e["session_id"]
    assert validate_event(e, now=NOW) == (False, "MISSING_FIELD")


def test_unknown_type_fails():
    assert validate_event(base("teleport"), now=NOW) == (False, "UNKNOWN_EVENT_TYPE")


def test_negative_price_fails():
    e = base()
    e["payload"]["price"] = -5
    assert validate_event(e, now=NOW) == (False, "NEGATIVE_PRICE")


def test_future_timestamp_fails():
    e = base(event_time=(NOW + timedelta(hours=2)).isoformat())
    assert validate_event(e, now=NOW) == (False, "FUTURE_TIMESTAMP")


def test_unknown_version_fails():
    assert validate_event(base(event_version=9), now=NOW) == (False, "BAD_VERSION")


def test_anonymous_event_without_customer_passes():
    e = base()
    assert "customer_id" not in e
    assert validate_event(e, now=NOW) == (True, "")


def test_order_event_without_session_passes():
    e = {
        "event_id": "evt_2",
        "event_type": "order_created",
        "event_version": 1,
        "event_time": NOW.isoformat(),
        "order_id": "ord_123",
        "payload": {"total": 99.99},
    }
    assert validate_event(e, now=NOW) == (True, "")


def test_order_event_without_order_id_fails():
    e = base(event_type="order_created", session_id="sess_1")
    e.pop("session_id", None)
    assert validate_event(e, now=NOW) == (False, "MISSING_FIELD")


def test_app_event_still_requires_session():
    e = base()
    del e["session_id"]
    assert validate_event(e, now=NOW) == (False, "MISSING_FIELD")
