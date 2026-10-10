"""Deterministic stock model for the inventory events (inventory design, section 6).

Time order is the whole point. A sale is queued at the time of its order and applied only when the clock passes
that time, so every snapshot carries exactly the stock at its own event_time and the live rule "latest snapshot plus
the movements strictly after it" gives the right level. Everything draws from one private rng derived from the run
seed; the main event stream is never touched.
"""
import hashlib
import heapq
import random
from dataclasses import dataclass
from datetime import datetime, timedelta

from contracts.stores import STORES

HERO_STORE = "store_cairo_nasr_city"
DEFAULT_HERO_UNITS = 12
RESTOCK_UNITS = 60
_ORDER = {"sell": 0, "restock": 1, "extra": 2, "grid": 3}  # at equal times: movements before the snapshot grid


@dataclass
class HeroScenario:
    store_id: str
    products: list  # product dicts of the catalog
    share: float = 0.6  # share of sessions drawn from customers served by the hero store


class StockLedger:
    def __init__(self, seed, catalog, start, scenario=False, hero_units=DEFAULT_HERO_UNITS,
                 snapshot_every=timedelta(minutes=10), restock_after=timedelta(seconds=90)):
        self.seed = seed
        self._rng = random.Random(seed * 7919 + 13)
        self._start = start
        self._snapshot_every = snapshot_every
        self._restock_after = restock_after
        self._n = 0
        self._seq = 0
        self._heap = []
        self._dirty = set()
        self._restock_pending = set()
        self.oversold_units = 0
        self.stock, self.reorder, self._opening = {}, {}, {}
        active = [p for p in catalog if p["is_active"]]
        for store_id in sorted(STORES):
            for p in active:
                qty = self._rng.randint(200, 500)
                key = (store_id, p["product_id"])
                self.stock[key] = self._opening[key] = qty
                self.reorder[key] = max(10, qty // 5)
        self.scenario = None
        if scenario:
            hero = self._rng.sample(active, 3)
            for p in hero:
                key = (HERO_STORE, p["product_id"])
                self.stock[key] = self._opening[key] = hero_units
                self.reorder[key] = 3
            self.scenario = HeroScenario(HERO_STORE, hero)
            others = [k for k in sorted(self.stock) if k[0] != HERO_STORE]
            self._push(start + timedelta(minutes=3), "extra", ("shrinkage", self._rng.choice(others), -1))
            self._push(start + timedelta(minutes=4), "extra", ("adjustment", self._rng.choice(others), 2))
        self._push(start + snapshot_every, "grid", None)

    # -- events ------------------------------------------------------------------------------------------
    def _event_id(self, kind):
        self._n += 1
        return "evt_" + hashlib.sha1(("%d|%s|%d" % (self.seed, kind, self._n)).encode()).hexdigest()[:16]

    def _event(self, event_type, key, t, payload):
        return {"event_id": self._event_id(event_type), "event_type": event_type, "event_version": 1,
                "event_time": t.isoformat(), "store_id": key[0], "product_id": key[1], "payload": payload}

    def _snapshot(self, key, t):
        return self._event("stock_snapshot", key, t, {"on_hand": self.stock[key], "reserved_qty": 0,
                                                      "reorder_point": self.reorder[key]})

    def _movement(self, key, t, qty_delta, reason, order_id=None, requested=None):
        payload = {"qty_delta": qty_delta, "reason": reason}
        if order_id:
            payload["order_id"] = order_id
        if requested is not None:
            payload["requested_qty"] = requested
        return self._event("stock_movement", key, t, payload)

    # -- clock ---------------------------------------------------------------------------------------------
    def _push(self, when, kind, data):
        heapq.heappush(self._heap, (when, _ORDER[kind], self._seq, kind, data))
        self._seq += 1

    def opening(self):
        """The opening snapshot of every store and active product, at the start time."""
        return [self._snapshot(key, self._start) for key in sorted(self.stock)]

    def order(self, order_created, store_id):
        """Queue the lines of an order_created event as sales at the order time."""
        t = datetime.fromisoformat(order_created["event_time"])
        for line in order_created["payload"]["items"]:
            self._push(t, "sell", ((store_id, line["product_id"]), int(line["quantity"]), order_created["order_id"]))

    def due(self, t):
        """Every stock event whose time is at or before t, in time order."""
        out = []
        while self._heap and self._heap[0][0] <= t:
            when, _, _, kind, data = heapq.heappop(self._heap)
            out.extend(self._apply(when, kind, data))
        return out

    def finish(self):
        """What is still queued at the end of a run, except the snapshot grid."""
        out = []
        while any(item[3] != "grid" for item in self._heap):
            item = min(i for i in self._heap if i[3] != "grid")
            self._heap.remove(item)
            heapq.heapify(self._heap)
            out.extend(self._apply(item[0], item[3], item[4]))
        return out

    def _apply(self, when, kind, data):
        if kind == "sell":
            key, qty, order_id = data
            have = self.stock.get(key)
            if have is None:  # an inactive product is not stocked
                return []
            taken = min(qty, have)
            self.oversold_units += qty - taken
            if taken == 0:
                return []
            self.stock[key] = have - taken
            self._dirty.add(key)
            if self.stock[key] == 0 and key not in self._restock_pending:
                self._restock_pending.add(key)
                self._push(when + self._restock_after, "restock", key)
            return [self._movement(key, when, -taken, "sold", order_id, qty)]
        if kind == "restock":
            key = data
            units = max(RESTOCK_UNITS, self._opening[key] // 2)
            self._restock_pending.discard(key)
            self.stock[key] += units
            self._dirty.add(key)
            return [self._movement(key, when, units, "received")]
        if kind == "extra":
            reason, key, delta = data
            if delta < 0:
                delta = -min(-delta, self.stock[key])  # never below zero
            if delta == 0:
                return []
            self.stock[key] += delta
            self._dirty.add(key)
            return [self._movement(key, when, delta, reason)]
        events = [self._snapshot(key, when) for key in sorted(self._dirty)]  # grid: only pairs that changed
        self._dirty.clear()
        self._push(when + self._snapshot_every, "grid", None)
        return events
