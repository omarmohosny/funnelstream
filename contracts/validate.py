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
CLOCK_SKEW = timedelta(minutes=5)


def validate_event(event: dict, now: datetime | None = None) -> tuple[bool, str]:
    now = now or datetime.now(timezone.utc)

    # Check fields required for all events
    for field in REQUIRED_ALL:
        if event.get(field) in (None, ""):
            return False, "MISSING_FIELD"

    # Check event type
    event_type = event.get("event_type")
    if event_type not in KNOWN_TYPES:
        return False, "UNKNOWN_EVENT_TYPE"

    # Check session_id or order_id based on event type
    if event_type in APP_EVENT_TYPES:
        if event.get("session_id") in (None, ""):
            return False, "MISSING_FIELD"
    elif event_type in ORDER_EVENT_TYPES or event_type in DELIVERY_EVENT_TYPES:
        if event.get("order_id") in (None, ""):
            return False, "MISSING_FIELD"

    # Check version
    if event["event_version"] not in SUPPORTED_VERSIONS:
        return False, "BAD_VERSION"

    # Check timestamp
    ts = datetime.fromisoformat(event["event_time"])
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    if ts > now + CLOCK_SKEW:
        return False, "FUTURE_TIMESTAMP"

    # Check price if present
    price = event["payload"].get("price")
    if price is not None and price < 0:
        return False, "NEGATIVE_PRICE"

    return True, ""
