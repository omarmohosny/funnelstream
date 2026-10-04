"""Order-level Silver rules that need several rows of one order."""
import hashlib
import json
from datetime import datetime, timezone

from streaming.silver.schemas import SILVER_QUARANTINE_COLUMNS


def _ts(value):
    ts = datetime.fromisoformat(value)
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def split_cancel_after_ship(status_rows, now=None):
    """(kept_rows, quarantine_rows). A cancel is quarantined when its order has an order_shipped row
    with an earlier status_time. Input rows are not mutated; kept rows keep their input order."""
    now = now or datetime.now(timezone.utc)
    first_ship = {}
    for r in status_rows:
        if r["status"] == "order_shipped":
            t = _ts(r["status_time"])
            if r["order_id"] not in first_ship or t < first_ship[r["order_id"]]:
                first_ship[r["order_id"]] = t
    kept, quarantined = [], []
    for r in status_rows:
        ship = first_ship.get(r["order_id"])
        if r["status"] == "order_cancelled" and ship is not None and ship < _ts(r["status_time"]):
            row = {
                "quarantine_id": hashlib.sha1(("cancel_after_ship" + str(r["event_id"])).encode("utf-8")).hexdigest(),
                "raw_payload": json.dumps(r, sort_keys=True, default=str),
                "source_topic": "orders",
                "error_code": "CANCEL_AFTER_SHIPMENT",
                "error_detail": "order_cancelled arrived after order_shipped for the same order",
                "received_at": now,
            }
            assert list(row) == SILVER_QUARANTINE_COLUMNS
            quarantined.append(row)
        else:
            kept.append(r)
    return kept, quarantined
