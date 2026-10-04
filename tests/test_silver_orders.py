import copy
import json
from datetime import datetime, timezone

from streaming.silver.orders import split_cancel_after_ship
from streaming.silver.schemas import SILVER_QUARANTINE_COLUMNS

NOW = datetime(2026, 10, 20, tzinfo=timezone.utc)


def row(eid, oid, status, t):
    return {"event_id": eid, "order_id": oid, "customer_id": "c1", "status": status, "status_rank": 0,
            "status_time": t, "payment_method": None, "carrier": None, "reason": None, "event_date": "2026-10-01"}


def test_cancel_after_ship_quarantined():
    rows = [row("e1", "o1", "order_shipped", "2026-10-01T10:00:00+00:00"),
            row("e2", "o1", "order_cancelled", "2026-10-01T11:00:00+00:00")]
    kept, q = split_cancel_after_ship(rows, now=NOW)
    assert [r["event_id"] for r in kept] == ["e1"]
    assert len(q) == 1
    assert list(q[0]) == SILVER_QUARANTINE_COLUMNS
    assert q[0]["error_code"] == "CANCEL_AFTER_SHIPMENT" and q[0]["source_topic"] == "orders"
    assert q[0]["received_at"] == NOW
    assert json.loads(q[0]["raw_payload"])["event_id"] == "e2"
    import hashlib
    assert q[0]["quarantine_id"] == hashlib.sha1(b"cancel_after_ship" + b"e2").hexdigest()


def test_cancel_before_ship_kept():
    rows = [row("e1", "o1", "order_cancelled", "2026-10-01T09:00:00+00:00"),
            row("e2", "o1", "order_shipped", "2026-10-01T10:00:00+00:00")]
    kept, q = split_cancel_after_ship(rows, now=NOW)
    assert len(kept) == 2 and q == []


def test_cancel_alone_kept():
    kept, q = split_cancel_after_ship([row("e1", "o1", "order_cancelled", "2026-10-01T09:00:00+00:00")], now=NOW)
    assert len(kept) == 1 and q == []


def test_many_orders_handled_per_order():
    rows = [row("a1", "oA", "order_shipped", "2026-10-01T10:00:00+00:00"),
            row("b1", "oB", "order_cancelled", "2026-10-01T11:00:00+00:00"),  # oB never shipped: kept
            row("a2", "oA", "order_cancelled", "2026-10-01T12:00:00+00:00"),  # quarantined
            row("c1", "oC", "order_cancelled", "2026-10-01T08:00:00+00:00"),  # before ship: kept
            row("c2", "oC", "order_shipped", "2026-10-01T09:00:00+00:00"),
            row("d1", "oD", "order_shipped", "2026-10-01T10:00:00Z"),
            row("d2", "oD", "order_cancelled", "2026-10-01T12:00:00+02:00")]  # 10:00Z equals ship: kept
    kept, q = split_cancel_after_ship(rows, now=NOW)
    assert [json.loads(r["raw_payload"])["event_id"] for r in q] == ["a2"]
    assert [r["event_id"] for r in kept] == ["a1", "b1", "c1", "c2", "d1", "d2"]


def test_idempotent_on_kept_output():
    rows = [row("a1", "oA", "order_shipped", "2026-10-01T10:00:00+00:00"),
            row("a2", "oA", "order_cancelled", "2026-10-01T12:00:00+00:00"),
            row("b1", "oB", "order_cancelled", "2026-10-01T11:00:00+00:00")]
    kept, q = split_cancel_after_ship(rows, now=NOW)
    kept2, q2 = split_cancel_after_ship(kept, now=NOW)
    assert kept2 == kept and q2 == []
    assert split_cancel_after_ship(rows, now=NOW) == (kept, q)


def test_input_not_mutated():
    rows = [row("a1", "oA", "order_shipped", "2026-10-01T10:00:00+00:00"),
            row("a2", "oA", "order_cancelled", "2026-10-01T12:00:00+00:00")]
    before = copy.deepcopy(rows)
    split_cancel_after_ship(rows, now=NOW)
    assert rows == before
