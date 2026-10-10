"""Pure-Python Silver rules: one Bronze raw message in, rows per Silver table out."""
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

from contracts.geo import district_violation, normalize_district
from contracts.stores import store_for
from contracts.normalize import normalize_city, normalize_device, to_decimal, to_utc_iso
from contracts.validate import APP_EVENT_TYPES, INVENTORY_EVENT_TYPES, validate_raw
from streaming.common.status import STATUS_RANK
from streaming.silver.schemas import (CURRENCY, ERROR_CODES, LATE_AFTER_SECONDS, SILVER_EVENTS_COLUMNS,
                                      SILVER_EXPRESS_ORDERS_COLUMNS, SILVER_ORDER_ITEMS_COLUMNS,
                                      SILVER_ORDER_STATUS_COLUMNS,
                                      SILVER_ORDERS_COLUMNS, SILVER_QUARANTINE_COLUMNS,
                                      SILVER_STOCK_MOVEMENTS_COLUMNS, SILVER_STOCK_SNAPSHOTS_COLUMNS)

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


def _clean_utm(v):
    """Lower-cased, trimmed text; empty or non-text becomes None."""
    if not isinstance(v, str):
        return None
    v = v.strip().lower()
    return v or None


def parse_page_url(page_url):
    """(utm_source, utm_medium, utm_campaign) of a tracking URL. Never raises: a malformed or non-text
    URL gives all None."""
    if not isinstance(page_url, str):
        return None, None, None
    try:
        query = parse_qs(urlsplit(page_url.strip()).query, keep_blank_values=True)
    except ValueError:
        return None, None, None
    return tuple(_clean_utm((query.get(k) or [None])[0]) for k in ("utm_source", "utm_medium", "utm_campaign"))


def _event_utm(p):
    """(utm_source, utm_medium, utm_campaign) of an app event: parsed from page_url; payload fields only
    for older Bronze data that has no page_url (utm_medium NULL there)."""
    if p.get("page_url") is not None:
        return parse_page_url(p["page_url"])
    return _clean_utm(p.get("utm_source")), None, _clean_utm(p.get("utm_campaign"))


def count_district_violations(rows):
    """Number of event or order dicts (as sent, before normalisation) whose non-empty district does not
    belong to their normalised city. Reported only: such rows are kept with district 'unknown'."""
    n = 0
    for e in rows:
        try:
            if district_violation(normalize_city(e.get("city")), e.get("district")):
                n += 1
        except Exception:
            pass
    return n


def _cairo_date(iso_utc):
    return datetime.fromisoformat(iso_utc).astimezone(CAIRO).date().isoformat()


def _app_event(e, p, kafka_timestamp):
    et = to_utc_iso(e["event_time"])
    ts = datetime.fromisoformat(et)
    city = normalize_city(e.get("city"))
    utm_source, utm_medium, utm_campaign = _event_utm(p)
    page_url = p.get("page_url")
    return [("silver_events", _row(
        SILVER_EVENTS_COLUMNS,
        event_id=_text(e["event_id"]), event_type=e["event_type"], event_time=et,
        kafka_timestamp=kafka_timestamp,
        is_late=(kafka_timestamp - ts).total_seconds() > LATE_AFTER_SECONDS,
        is_bot=False, event_version=e["event_version"],
        session_id=_text(e.get("session_id")), customer_id=_text(e.get("customer_id")),
        device=normalize_device(e.get("device")), channel=_text(e.get("channel")),
        platform=_platform(e.get("platform")), country=_text(e.get("country")) or "EG", city=city,
        district=normalize_district(city, e.get("district")),
        campaign_id=None, utm_source=utm_source, utm_campaign=utm_campaign, utm_medium=utm_medium,
        click_id=_text(p.get("click_id")), page_url=page_url if isinstance(page_url, str) else None,
        product_id=_text(p.get("product_id")),
        element=_text(p.get("element")), dwell_seconds=_int(p.get("dwell_seconds"), "dwell_seconds"),
        cart_id=_text(p.get("cart_id")), order_id=_text(p.get("order_id")),
        search_term=_text(p.get("search_term")), event_date=_cairo_date(et)))]


def _stock_event(e, p, kafka_timestamp):
    et = to_utc_iso(e["event_time"])
    common = dict(event_id=_text(e["event_id"]), store_id=_text(e["store_id"]), product_id=_text(e["product_id"]),
                  event_time=et, kafka_timestamp=kafka_timestamp, event_date=_cairo_date(et))
    if e["event_type"] == "stock_snapshot":
        return [("silver_stock_snapshots", _row(
            SILVER_STOCK_SNAPSHOTS_COLUMNS, **common, on_hand=_int(p.get("on_hand"), "on_hand"),
            reserved_qty=_int(p.get("reserved_qty") or 0, "reserved_qty"),
            reorder_point=_int(p.get("reorder_point"), "reorder_point")))]
    return [("silver_stock_movements", _row(
        SILVER_STOCK_MOVEMENTS_COLUMNS, **common, qty_delta=_int(p.get("qty_delta"), "qty_delta"),
        reason=_text(p.get("reason")), order_id=_text(p.get("order_id")),
        requested_qty=_int(p.get("requested_qty"), "requested_qty")))]


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
    city = normalize_city(e.get("city"))
    order = _row(
        SILVER_ORDERS_COLUMNS, order_id=order_id, customer_id=_text(e.get("customer_id")),
        session_id=_text(p.get("session_id")), channel=_text(p.get("channel")),
        platform=_platform(p.get("platform")), utm_source=_clean_utm(p.get("utm_source")),
        utm_campaign=_clean_utm(p.get("utm_campaign")), utm_medium=_clean_utm(p.get("utm_medium")),
        click_id=_text(p.get("click_id")), payment_method=_text(p.get("payment_method")),
        city=city, district=normalize_district(city, e.get("district")), country=_text(e.get("country")) or "EG",
        order_total=_money(p.get("order_total")), order_created_at=et, event_date=_cairo_date(et))
    express_rows = []
    if p.get("service") == "express":
        store_id = store_for(city, order["district"])
        if store_id is None:
            raise _Bad("express order outside a served district")
        express_rows.append(("silver_express_orders", _row(
            SILVER_EXPRESS_ORDERS_COLUMNS, order_id=order_id, store_id=store_id,
            promised_minutes=_int(p.get("promised_minutes"), "promised_minutes"), created_at=et, event_date=_cairo_date(et))))
    return [("silver_orders", order), ("silver_order_status", status)] + item_rows + express_rows


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
        if e["event_type"] in INVENTORY_EVENT_TYPES:
            return _stock_event(e, p, kafka_timestamp)
        return _order_event(e, p)
    except _Bad as ex:
        return _quarantine(raw, source_topic, "BAD_VALUE", ex.detail, received_at)
    except Exception:
        return _quarantine(raw, source_topic, "BAD_VALUE", _DETAIL["BAD_VALUE"], received_at)
