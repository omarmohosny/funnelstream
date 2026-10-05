"""Pure-Python Silver rules: one Bronze raw message in, rows per Silver table out."""
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from contracts.normalize import normalize_city, normalize_device, to_decimal, to_utc_iso
from contracts.validate import APP_EVENT_TYPES, validate_raw
from streaming.common.status import STATUS_RANK
from streaming.silver.schemas import (CURRENCY, ERROR_CODES, LATE_AFTER_SECONDS, SILVER_EVENTS_COLUMNS,
                                      SILVER_ORDER_ITEMS_COLUMNS, SILVER_ORDER_STATUS_COLUMNS,
                                      SILVER_ORDERS_COLUMNS, SILVER_QUARANTINE_COLUMNS)

CAIRO = ZoneInfo("Africa/Cairo")
_CENT = Decimal("0.01")
_DETAIL = {
    "BAD_JSON": "The message is not a valid JSON object.",
    "MISSING_FIELD": "A required field is missing or empty.",
    "UNKNOWN_EVENT_TYPE": "The event type is not one of the known types.",
    "BAD_VERSION": "The event version is not supported.",
    "NEGATIVE_PRICE": "A price or total is negative.",
    "FUTURE_TIMESTAMP": "The event time is in the future.",
    "BAD_VALUE": "A field has a value of the wrong type or format.",
    "UNKNOWN_PRODUCT": "The product id is not in the catalog.",
}
_LINE_MISMATCH = "line_total does not match quantity x unit_price"


class _Bad(Exception):
    def __init__(self, detail):
        super().__init__(detail)
        self.detail = detail


def _quarantine(raw, source_topic, code, detail, received_at):
    if isinstance(raw, str):
        raw_text = raw
    elif isinstance(raw, (bytes, bytearray)):
        raw_text = bytes(raw).decode("utf-8", "replace")
    else:
        raw_text = repr(raw)
    qid = hashlib.sha1((str(source_topic) + "\n" + raw_text).encode("utf-8", "surrogatepass")).hexdigest()
    row = {"quarantine_id": qid, "raw_payload": raw_text, "source_topic": source_topic,
           "error_code": code, "error_detail": detail, "received_at": received_at}
    assert code in ERROR_CODES and list(row) == SILVER_QUARANTINE_COLUMNS
    return [("silver_quarantine", row)]


def _text(v):
    if v is None:
        return None
    if isinstance(v, str):
        return v
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return str(v)
    raise _Bad("a text field holds a non-text value")


def _money(v):
    return to_decimal(v).quantize(_CENT)


def _int(v, name):
    if v is None:
        return None
    if isinstance(v, bool):
        raise _Bad(name + " is not an integer")
    d = to_decimal(v)
    if d != d.to_integral_value():
        raise _Bad(name + " is not a whole number")
    return int(d)


def _platform(v):
    """app or web; anything else (missing, old-format events) is NULL."""
    return v if v in ("app", "web") else None


def _row(columns, **values):
    return {c: values.get(c) for c in columns}


def _cairo_date(iso_utc):
    return datetime.fromisoformat(iso_utc).astimezone(CAIRO).date().isoformat()


def _app_event(e, p, kafka_timestamp):
    et = to_utc_iso(e["event_time"])
    ts = datetime.fromisoformat(et)
    return [("silver_events", _row(
        SILVER_EVENTS_COLUMNS,
        event_id=_text(e["event_id"]), event_type=e["event_type"], event_time=et,
        kafka_timestamp=kafka_timestamp,
        is_late=(kafka_timestamp - ts).total_seconds() > LATE_AFTER_SECONDS,
        is_bot=False, event_version=e["event_version"],
        session_id=_text(e.get("session_id")), customer_id=_text(e.get("customer_id")),
        device=normalize_device(e.get("device")), channel=_text(e.get("channel")),
        platform=_platform(e.get("platform")), country=_text(e.get("country")) or "EG", city=normalize_city(e.get("city")),
        campaign_id=None, utm_source=_text(p.get("utm_source")), utm_campaign=_text(p.get("utm_campaign")),
        click_id=_text(p.get("click_id")), product_id=_text(p.get("product_id")),
        element=_text(p.get("element")), dwell_seconds=_int(p.get("dwell_seconds"), "dwell_seconds"),
        cart_id=_text(p.get("cart_id")), order_id=_text(p.get("order_id")),
        search_term=_text(p.get("search_term")), event_date=_cairo_date(et)))]


def _status_row(e, p, et):
    status = e["event_type"]
    return _row(
        SILVER_ORDER_STATUS_COLUMNS,
        event_id=_text(e["event_id"]), order_id=_text(e["order_id"]), customer_id=_text(e.get("customer_id")),
        status=status, status_rank=STATUS_RANK[status], status_time=et,
        payment_method=_text(p.get("payment_method")),
        carrier=_text(p.get("carrier")) if status in ("order_shipped", "out_for_delivery", "delivered") else None,
        reason=_text(p.get("reason")) if status in ("order_cancelled", "delivery_failed", "order_returned") else None,
        event_date=_cairo_date(et))


def _order_event(e, p):
    et = to_utc_iso(e["event_time"])
    status = _status_row(e, p, et)
    if e["event_type"] != "order_created":
        return [("silver_order_status", status)]
    order_id = _text(e["order_id"])
    items = p.get("items")
    if not items:
        raise _Bad("order has no items")
    item_rows = []
    for n, item in enumerate(items, start=1):
        qty = _int(item.get("quantity"), "quantity")
        unit = to_decimal(item.get("unit_price"))
        line = to_decimal(item.get("line_total"))
        if qty is None or abs(line - qty * unit) > _CENT:
            raise _Bad(_LINE_MISMATCH)
        item_rows.append(("silver_order_items", _row(
            SILVER_ORDER_ITEMS_COLUMNS, order_id=order_id, line_no=n, product_id=_text(item.get("product_id")),
            quantity=qty, unit_price=unit.quantize(_CENT), line_total=line.quantize(_CENT),
            currency=CURRENCY, order_time=et)))
    order = _row(
        SILVER_ORDERS_COLUMNS, order_id=order_id, customer_id=_text(e.get("customer_id")),
        session_id=_text(p.get("session_id")), channel=_text(p.get("channel")),
        platform=_platform(p.get("platform")), utm_source=_text(p.get("utm_source")), utm_campaign=_text(p.get("utm_campaign")),
        click_id=_text(p.get("click_id")), payment_method=_text(p.get("payment_method")),
        city=normalize_city(e.get("city")), country=_text(e.get("country")) or "EG",
        order_total=_money(p.get("order_total")), order_created_at=et, event_date=_cairo_date(et))
    return [("silver_orders", order), ("silver_order_status", status)] + item_rows


def transform_message(raw, source_topic, kafka_timestamp, received_at, known_products=None, now=None):
    """Return [(table_name, row_dict)]. Never raises."""
    try:
        now = now or datetime.now(timezone.utc)
        ok, code = validate_raw(raw, now, known_products)
        if not ok:
            return _quarantine(raw, source_topic, code, _DETAIL[code], received_at)
        if isinstance(raw, dict):
            e = raw
        else:
            if isinstance(raw, (bytes, bytearray)):
                raw = bytes(raw).decode("utf-8")
            e = json.loads(raw)
        p = e["payload"]
        if e["event_type"] in APP_EVENT_TYPES:
            return _app_event(e, p, kafka_timestamp)
        return _order_event(e, p)
    except _Bad as ex:
        return _quarantine(raw, source_topic, "BAD_VALUE", ex.detail, received_at)
    except Exception:
        return _quarantine(raw, source_topic, "BAD_VALUE", _DETAIL["BAD_VALUE"], received_at)
