"""The JSON schemas in contracts/schemas are documentation; these tests keep them from drifting away from
contracts/topics.yaml, the validator and the simulator (data model review I12)."""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft7Validator

from contracts.validate import validate_event
from simulator.catalog import make_catalog
from simulator.stock import StockLedger

SCHEMAS = Path(__file__).resolve().parent.parent / "contracts" / "schemas"
TOPICS = yaml.safe_load((SCHEMAS.parent / "topics.yaml").read_text())
NOW = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)


def schema(name):
    s = json.loads((SCHEMAS / name).read_text())
    Draft7Validator.check_schema(s)
    return Draft7Validator(s)


@pytest.mark.parametrize("name,key", [("order_event.json", "order_event_types"),
                                      ("delivery_event.json", "delivery_event_types"),
                                      ("inventory_event.json", "inventory_event_types")])
def test_event_type_enums_equal_topics_yaml(name, key):
    enum = json.loads((SCHEMAS / name).read_text())["properties"]["event_type"]["enum"]
    assert sorted(enum) == sorted(TOPICS[key])


def snapshot(**payload):
    return {"event_id": "e1", "event_type": "stock_snapshot", "event_version": 1,
            "event_time": "2026-10-10T11:00:00+00:00", "store_id": "store_cairo_maadi", "product_id": "prod_0001",
            "payload": {"on_hand": 40, "reserved_qty": 0, "reorder_point": 10, **payload}}


def movement(**payload):
    return {"event_id": "e2", "event_type": "stock_movement", "event_version": 1,
            "event_time": "2026-10-10T11:00:05+00:00", "store_id": "store_cairo_maadi", "product_id": "prod_0001",
            "payload": {"qty_delta": -2, "reason": "sold", "order_id": "ord_1", "requested_qty": 2, **payload}}


def drop(event, key):
    e = json.loads(json.dumps(event))
    e["payload"].pop(key, None)
    return e


GOOD = [snapshot(), drop(snapshot(), "reserved_qty"), movement(),
        movement(qty_delta=60, reason="received", order_id="x"), drop(movement(qty_delta=2, reason="adjustment"), "order_id"),
        movement(qty_delta=-1, reason="shrinkage")]
BAD = [snapshot(on_hand=-1), snapshot(on_hand=1.5), snapshot(on_hand="3"), drop(snapshot(), "on_hand"),
       movement(qty_delta=0), movement(qty_delta=1.5), movement(reason="stolen"), movement(qty_delta=2),
       movement(qty_delta=-5, reason="received"), movement(qty_delta=2, reason="shrinkage"),
       drop(movement(), "order_id"), drop(movement(), "qty_delta"), movement(requested_qty=0)]


@pytest.mark.parametrize("event", GOOD)
def test_inventory_schema_accepts_what_the_validator_accepts(event):
    assert not list(schema("inventory_event.json").iter_errors(event))
    assert validate_event(event, NOW) == (True, "")


@pytest.mark.parametrize("event", BAD)
def test_inventory_schema_rejects_what_the_validator_rejects(event):
    assert list(schema("inventory_event.json").iter_errors(event))
    assert validate_event(event, NOW)[0] is False


def test_inventory_schema_requires_store_and_product():
    for field in ("store_id", "product_id"):
        e = snapshot()
        del e[field]
        assert list(schema("inventory_event.json").iter_errors(e))


def test_every_event_of_the_stock_ledger_matches_the_inventory_schema():
    start = datetime(2026, 10, 10, 8, 0, tzinfo=timezone.utc)
    catalog = make_catalog(3, 20)
    ledger = StockLedger(3, catalog, start)
    events = ledger.opening()
    pid = catalog[0]["product_id"]
    ledger.order({"event_time": start.isoformat(), "order_id": "ord_9",
                  "payload": {"items": [{"product_id": pid, "quantity": 2}]}}, "store_cairo_maadi")
    events += ledger.due(start + timedelta(hours=2))
    assert any(e["event_type"] == "stock_movement" for e in events)
    v = schema("inventory_event.json")
    for e in events:
        assert not list(v.iter_errors(e)), e
