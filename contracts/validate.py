import json
import math
from datetime import datetime, timedelta, timezone
import yaml
from pathlib import Path

_TOPICS = yaml.safe_load((Path(__file__).parent / "topics.yaml").read_text())
APP_EVENT_TYPES = set(_TOPICS["app_event_types"])
ORDER_EVENT_TYPES = set(_TOPICS["order_event_types"])
DELIVERY_EVENT_TYPES = set(_TOPICS["delivery_event_types"])
KNOWN_TYPES = APP_EVENT_TYPES | ORDER_EVENT_TYPES | DELIVERY_EVENT_TYPES

REQUIRED_ALL = ["event_id", "event_type", "event_version", "event_time", "payload"]
SUPPORTED_VERSIONS = {1}
PRODUCT_EVENT_TYPES = {"product_view", "product_click", "product_view_end", "add_to_cart", "remove_from_cart"}
CLOCK_SKEW = timedelta(minutes=5)


def _parse_time(value):
    """Aware UTC datetime from an ISO string; None when not a string or not parseable."""
    if not isinstance(value, str):
        return None
    try:
        ts = datetime.fromisoformat(value.strip())
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def _to_number(value):
    """float for int/float/numeric string, None otherwise (bool and nan/inf rejected)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        num = float(value)
    elif isinstance(value, str):
        try:
            num = float(value.strip())
        except ValueError:
            return None
    else:
        return None
    return num if math.isfinite(num) else None


def validate_event(event: dict, now: datetime | None = None,
                   known_products: set | None = None) -> tuple[bool, str]:
    """Never raises. Returns (ok, code); code is '' when ok."""
    try:
        return _validate(event, now, known_products)
    except Exception:  # defensive: validation must not take the pipeline down
        return False, "BAD_VALUE"


def _validate(event, now, known_products):
    if not isinstance(event, dict):
        return False, "BAD_VALUE"
    now = now or datetime.now(timezone.utc)

    for field in REQUIRED_ALL:
        if event.get(field) in (None, ""):
            return False, "MISSING_FIELD"

    event_type = event.get("event_type")
    if not isinstance(event_type, str) or event_type not in KNOWN_TYPES:
        return False, "UNKNOWN_EVENT_TYPE"

    if event_type in APP_EVENT_TYPES:
        if event.get("session_id") in (None, ""):
            return False, "MISSING_FIELD"
    elif event_type in ORDER_EVENT_TYPES or event_type in DELIVERY_EVENT_TYPES:
        if event.get("order_id") in (None, ""):
            return False, "MISSING_FIELD"

    version = event["event_version"]
    if isinstance(version, bool) or not isinstance(version, int) or version not in SUPPORTED_VERSIONS:
        return False, "BAD_VERSION"

    payload = event["payload"]
    if not isinstance(payload, dict):
        return False, "BAD_VALUE"

    ts = _parse_time(event["event_time"])
    if ts is None:
        return False, "BAD_VALUE"
    if ts > now + CLOCK_SKEW:
        return False, "FUTURE_TIMESTAMP"

    price = payload.get("price")
    if price is not None:
        num = _to_number(price)
        if num is None:
            return False, "BAD_VALUE"
        if num < 0:
            return False, "NEGATIVE_PRICE"

    if event_type in PRODUCT_EVENT_TYPES:
        pid = payload.get("product_id")
        if pid in (None, ""):
            return False, "MISSING_FIELD"
        if known_products is not None and pid not in known_products:
            return False, "UNKNOWN_PRODUCT"

    return True, ""


def validate_raw(raw, now: datetime | None = None, known_products: set | None = None) -> tuple[bool, str]:
    """raw is JSON text (str/bytes) or a dict. Never raises."""
    if isinstance(raw, dict):
        return validate_event(raw, now, known_products)
    try:
        if isinstance(raw, (bytes, bytearray)):
            raw = bytes(raw).decode("utf-8")
        obj = json.loads(raw)
    except (ValueError, TypeError):  # includes UnicodeDecodeError and JSONDecodeError
        return False, "BAD_JSON"
    if not isinstance(obj, dict):
        return False, "BAD_JSON"
    return validate_event(obj, now, known_products)
