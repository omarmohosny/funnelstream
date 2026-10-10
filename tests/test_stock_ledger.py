import hashlib
import json
import random
from datetime import datetime, timedelta, timezone

from contracts.stores import STORES, store_for
from scripts.run_simulator import iter_batch
from simulator.catalog import make_catalog, make_customers
from simulator.stock import DEFAULT_HERO_UNITS, HERO_STORE, StockLedger

T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
CATALOG = make_catalog(7)
CUSTOMERS = make_customers(7, 1000)
ACTIVE = [p for p in CATALOG if p["is_active"]]

# digest of the stream before the inventory work (seed 5, universe 7, 300 events): proves "flags off = unchanged"
BASELINE_DIGEST = "4c861df1e4ef8b827014a7eee5e42490c9fc492d"
BASELINE_COUNT = 320


def order_event(order_id, product_id, qty, t):
    return {"event_type": "order_created", "order_id": order_id, "event_time": t.isoformat(),
            "payload": {"items": [{"product_id": product_id, "quantity": qty}]}}


def replay(events):
    """Apply emitted events in order like a consumer: snapshots set the level, movements add to it."""
    level, low = {}, 0
    for e in events:
        key = (e["store_id"], e["product_id"])
        if e["event_type"] == "stock_snapshot":
            level[key] = e["payload"]["on_hand"]
        else:
            level[key] = level.get(key, 0) + e["payload"]["qty_delta"]
        low = min(low, level[key])
    return level, low


def test_the_stream_without_inventory_is_unchanged():
    out = list(iter_batch(random.Random(5), CUSTOMERS, CATALOG, T0, max_events=300))
    assert len(out) == BASELINE_COUNT
    assert hashlib.sha1(json.dumps(out, sort_keys=True).encode()).hexdigest() == BASELINE_DIGEST


def test_opening_snapshots_cover_every_store_and_active_product_and_are_deterministic():
    a, b = StockLedger(1, CATALOG, T0), StockLedger(1, CATALOG, T0)
    opening = a.opening()
    assert len(opening) == len(STORES) * len(ACTIVE)
    assert opening == b.opening()
    assert opening != StockLedger(2, CATALOG, T0).opening()
    assert all(200 <= e["payload"]["on_hand"] <= 500 for e in opening)
    assert all(e["event_time"] == T0.isoformat() and e["event_type"] == "stock_snapshot" for e in opening)


def test_a_sale_larger_than_the_stock_is_clamped_counted_and_never_negative():
    ledger = StockLedger(1, CATALOG, T0, scenario=True, hero_units=5)
    hero = ledger.scenario.products[0]["product_id"]
    ledger.order(order_event("ord_1", hero, 3, T0 + timedelta(minutes=1)), HERO_STORE)
    ledger.order(order_event("ord_2", hero, 3, T0 + timedelta(minutes=2)), HERO_STORE)
    ledger.order(order_event("ord_3", hero, 1, T0 + timedelta(minutes=3)), HERO_STORE)  # nothing left: no event
    events = ledger.due(T0 + timedelta(minutes=2, seconds=5))
    sold = [e for e in events if e["event_type"] == "stock_movement" and e["payload"]["reason"] == "sold"]
    assert [e["payload"]["qty_delta"] for e in sold] == [-3, -2]
    assert sold[1]["payload"]["requested_qty"] == 3 and sold[1]["payload"]["order_id"] == "ord_2"
    assert ledger.stock[(HERO_STORE, hero)] == 0 and ledger.oversold_units == 1
    rest = ledger.due(T0 + timedelta(hours=1))
    assert [e for e in rest if e["payload"].get("reason") == "sold"] == []  # the zero sale emitted nothing
    assert ledger.oversold_units == 2
    received = [e for e in rest if e["payload"].get("reason") == "received"]
    assert len(received) == 1 and received[0]["payload"]["qty_delta"] >= 60
    assert received[0]["event_time"] == (T0 + timedelta(minutes=2, seconds=90)).isoformat()  # 90 s after the stock-out
    assert ledger.stock[(HERO_STORE, hero)] == received[0]["payload"]["qty_delta"]


def test_emitted_events_never_drive_stock_below_zero_and_end_where_the_ledger_ends():
    ledger = StockLedger(3, CATALOG, T0, scenario=True)
    events = [e for h, e in iter_batch(random.Random(9), CUSTOMERS, CATALOG, T0, max_events=1500, mean_gap=1.2,
                                       ledger=ledger, scenario=ledger.scenario) if h == "inventory"]
    assert events, "no inventory events were produced"
    level, low = replay(events)
    assert low >= 0
    assert level == ledger.stock


def test_the_sink_rule_gives_the_ledger_stock():
    """latest snapshot per pair plus the movements strictly after it (the rule of live_stock_now)."""
    ledger = StockLedger(3, CATALOG, T0, scenario=True)
    events = [e for h, e in iter_batch(random.Random(9), CUSTOMERS, CATALOG, T0, max_events=1500, mean_gap=1.2,
                                       ledger=ledger, scenario=ledger.scenario) if h == "inventory"]
    snaps, moves = {}, {}
    for e in events:
        key = (e["store_id"], e["product_id"])
        if e["event_type"] == "stock_snapshot":
            if key not in snaps or (e["event_time"], e["event_id"]) > (snaps[key]["event_time"], snaps[key]["event_id"]):
                snaps[key] = e
        else:
            moves.setdefault(key, []).append(e)
    for key, snap in snaps.items():
        after = sum(m["payload"]["qty_delta"] for m in moves.get(key, []) if m["event_time"] > snap["event_time"])
        assert snap["payload"]["on_hand"] + after == ledger.stock[key], key


def test_the_hero_scenario_runs_a_product_out_and_restocks_it():
    ledger = StockLedger(3, CATALOG, T0, scenario=True, hero_units=DEFAULT_HERO_UNITS)
    hero_ids = {p["product_id"] for p in ledger.scenario.products}
    events = [e for h, e in iter_batch(random.Random(9), CUSTOMERS, CATALOG, T0, max_events=3000, mean_gap=1.2,
                                       ledger=ledger, scenario=ledger.scenario) if h == "inventory"]
    restocks = [e for e in events if e["store_id"] == HERO_STORE and e["product_id"] in hero_ids
                and e["event_type"] == "stock_movement" and e["payload"]["reason"] == "received"]
    assert restocks, "no hero product ran out within 3000 events: lower DEFAULT_HERO_UNITS in simulator/stock.py"


def test_hero_sessions_come_from_customers_of_the_hero_store_and_other_cities_are_untouched():
    ledger = StockLedger(3, CATALOG, T0, scenario=True)
    stream = list(iter_batch(random.Random(9), CUSTOMERS, CATALOG, T0, max_events=1500, mean_gap=1.2,
                             ledger=ledger, scenario=ledger.scenario))
    hero_ids = {p["product_id"] for p in ledger.scenario.products}
    views = [e for h, e in stream if h == "app" and e["event_type"] == "product_view"
             and e["payload"]["product_id"] in hero_ids]
    assert views
    served = [e for e in views if e["city"] == "Cairo" and store_for("Cairo", e["district"]) == HERO_STORE]
    assert len(served) / len(views) > 0.8
    assert any(e["event_type"] == "add_to_cart" and e["payload"]["product_id"] in hero_ids for h, e in stream if h == "app")


def test_customers_without_a_store_buy_without_stock_events():
    ledger = StockLedger(3, CATALOG, T0)
    unserved = [c for c in CUSTOMERS if store_for(c["city"], c.get("district")) is None]
    assert unserved, "the universe has no Assiut or Other customer"
    stream = list(iter_batch(random.Random(4), unserved, CATALOG, T0, max_events=1500, ledger=ledger))
    assert [e for h, e in stream if h == "order"], "no order was placed"
    assert [e for h, e in stream if h == "inventory" and e["payload"].get("reason") == "sold"] == []
