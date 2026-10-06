"""Seeded noise injection (spec section 4, "Injected noise").

apply_noise() sits between iter_batch and the publisher. All randomness comes from the passed
rng. At most one noise kind hits a message. For every message each kind (in KINDS order) is
tried: if the message is eligible for it and rng.random() < rate, the kind is applied and the
rest are skipped, so a rate is the share of *eligible* messages it hits. Because kinds are tried in
order, a later kind fires at about rate x (1 - earlier rates).
"""
import copy
import json
import random
from dataclasses import dataclass, fields, replace
from datetime import datetime, timedelta, timezone

from contracts.geo import DISTRICTS, alias_variants, normalize_district
from contracts.validate import APP_EVENT_TYPES, ORDER_EVENT_TYPES, DELIVERY_EVENT_TYPES, PRODUCT_EVENT_TYPES

ORDERISH = ORDER_EVENT_TYPES | DELIVERY_EVENT_TYPES


@dataclass
class NoiseConfig:
    duplicate: float = 0.01
    malformed_json: float = 0.003
    missing_field: float = 0.003
    unknown_type: float = 0.001
    bad_version: float = 0.001
    negative_price: float = 0.002
    future_timestamp: float = 0.001
    late: float = 0.02
    very_late: float = 0.003
    tz_offset: float = 0.01
    null_product_id: float = 0.005
    unknown_product_id: float = 0.003
    dirty_text: float = 0.03
    numeric_string: float = 0.003
    out_of_order_status: float = 0.005
    cancel_after_ship: float = 0.003
    bot_session: float = 0.001  # share of sessions, not of messages
    # spec 18.6. Declared last so they are tried last, and they draw from their own rng (see
    # SIDE_KINDS) so the draws of every older kind stay identical.
    dirty_district: float = 0.02  # share of app events with a valid district
    dirty_utm: float = 0.05  # share of events whose page_url carries UTM parameters

    @classmethod
    def off(cls) -> "NoiseConfig":
        return cls(**{f.name: 0.0 for f in fields(cls)})

    def scaled(self, factor: float) -> "NoiseConfig":
        return replace(self, **{f.name: getattr(self, f.name) * factor for f in fields(self)})


KINDS = [f.name for f in fields(NoiseConfig)]
# per-message kinds tried in this order; bot_session is decided once per new session
MESSAGE_KINDS = [k for k in KINDS if k != "bot_session"]
# kinds that use a side rng derived from the main one, so older seeded noise streams do not move
SIDE_KINDS = ("dirty_district", "dirty_utm")


class NoiseCounter:
    def __init__(self):
        self.counts = {k: 0 for k in KINDS}
        self.total_messages = 0  # messages yielded by apply_noise (includes duplicates and bot replays)

    def to_manifest(self) -> dict:
        return {"total_messages": self.total_messages, "injected": dict(self.counts)}


_UNKNOWN_TYPES = ["page_viewed", "order_lost", "mystery_event", "PAGE_VIEW"]
_CITY_DIRTY = {
    "Cairo": ["cairo ", "CAIRO", " Cairo", "cairo"],
    "Giza": ["giza", "GIZA", "el-giza", " Giza "],
    "Alexandria": ["alex", "ALEX", "alexandria", "ALEXANDRIA", " Alexandria "],
    "Mansoura": ["mansoura", "MANSOURA", "mansoura "],
    "Tanta": ["tanta", "TANTA", " Tanta "],
    "Assiut": ["assiut", "ASSIUT", "assiut "],
    "Other": ["other", "OTHER", "other "],
}
_DEVICE_DIRTY = {
    "mobile": ["Mobile", " MOBILE ", "phone", "MOBILE", "mobile "],
    "desktop": ["Desktop", "DESKTOP", "pc", "PC", " desktop "],
    "tablet": ["Tablet", "TABLET", " tablet", "tablet "],
}


def _district_variants(city, district):
    base = {district.lower().replace(" ", "-"), district.upper(), " %s " % district, district.lower(),
            district.lower().replace(" ", "_")}
    for alias in alias_variants(district):
        base |= {alias, alias.upper(), alias.title(), alias.replace(" ", "-")}
    return sorted(v for v in base if v != district and normalize_district(city, v) == district)


def _utm_dirty(url, style):
    base, sep, query = url.partition("?")
    parts = []
    for pair in query.split("&"):
        k, eq, v = pair.partition("=")
        if k.startswith("utm_") and eq and v:
            v = v.upper() if style == 0 else "%20" + v + "%20"
        parts.append(k + eq + v)
    return base + sep + "&".join(parts)


_NUM_FIELDS = ("price", "total", "order_total")


def _is_num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _t(event):
    dt = datetime.fromisoformat(event["event_time"])
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _numeric_slots(event):
    """(container, key) pairs holding a numeric price/total/unit_price."""
    p = event.get("payload")
    slots = []
    if isinstance(p, dict):
        slots += [(p, k) for k in _NUM_FIELDS if _is_num(p.get(k))]
        for item in p.get("items") or []:
            if isinstance(item, dict):
                slots += [(item, k) for k in ("unit_price", "line_total") if _is_num(item.get(k))]
    return slots


def _eligible(kind, e):
    et = e.get("event_type")
    p = e.get("payload") if isinstance(e.get("payload"), dict) else {}
    if kind in ("duplicate", "malformed_json", "unknown_type", "bad_version", "late", "very_late",
                "tz_offset", "future_timestamp", "missing_field"):
        return True
    if kind == "negative_price":
        return _is_num(p.get("price"))
    if kind in ("null_product_id", "unknown_product_id"):
        return et in PRODUCT_EVENT_TYPES and p.get("product_id") not in (None, "")
    if kind == "dirty_text":
        return any(isinstance(e.get(k), str) and e.get(k) for k in ("city", "device"))
    if kind == "dirty_district":
        return (et in APP_EVENT_TYPES and isinstance(e.get("district"), str)
                and normalize_district(e.get("city"), e["district"]) == e["district"])
    if kind == "dirty_utm":
        url = p.get("page_url")
        return isinstance(url, str) and "?utm_" in url
    if kind == "numeric_string":
        return bool(_numeric_slots(e))
    if kind == "cancel_after_ship":
        return et == "order_shipped"
    if kind == "out_of_order_status":
        return et in ORDERISH
    return False


def _new_id(rng):
    return "evt_%016x" % rng.getrandbits(64)


def _mutate(kind, e, rng, catalog_ids, now):
    """Return a changed deep copy for the in-place kinds."""
    e = copy.deepcopy(e)
    if kind == "missing_field":
        names = ["event_id", "event_type"]
        names.append("session_id" if e["event_type"] in APP_EVENT_TYPES else "order_id")
        e.pop(names[rng.randrange(len(names))], None)
    elif kind == "unknown_type":
        e["event_type"] = _UNKNOWN_TYPES[rng.randrange(len(_UNKNOWN_TYPES))]
    elif kind == "bad_version":
        e["event_version"] = 2
    elif kind == "negative_price":
        price = e["payload"]["price"]
        e["payload"]["price"] = -abs(price) if price else -1.0
    elif kind == "future_timestamp":
        base = _t(e)
        if now is not None and now > base:
            base = now  # history mode: also land in the future of the wall clock
        e["event_time"] = (base + timedelta(seconds=rng.randint(2 * 3600, 6 * 3600))).isoformat()
    elif kind == "late":
        e["event_time"] = (_t(e) - timedelta(seconds=rng.randint(10 * 60, 120 * 60))).isoformat()
    elif kind == "very_late":
        e["event_time"] = (_t(e) - timedelta(seconds=rng.randint(86400, 5 * 86400))).isoformat()
    elif kind == "tz_offset":
        e["event_time"] = _t(e).astimezone(timezone(timedelta(hours=2))).isoformat()
    elif kind == "null_product_id":
        e["payload"]["product_id"] = None
    elif kind == "unknown_product_id":
        while True:
            pid = "prod_%04d" % rng.randint(9000, 9999)
            if pid not in catalog_ids:
                break
        e["payload"]["product_id"] = pid
    elif kind == "dirty_text":
        keys = [k for k in ("city", "device") if isinstance(e.get(k), str) and e.get(k)]
        k = keys[rng.randrange(len(keys))]
        table = _CITY_DIRTY if k == "city" else _DEVICE_DIRTY
        variants = [v for v in table.get(e[k], []) if v != e[k]] or [e[k] + " "]
        e[k] = variants[rng.randrange(len(variants))]
    elif kind == "dirty_district":
        variants = _district_variants(e["city"], e["district"])
        e["district"] = variants[rng.randrange(len(variants))]
    elif kind == "dirty_utm":
        e["payload"]["page_url"] = _utm_dirty(e["payload"]["page_url"], rng.randrange(2))
    elif kind == "numeric_string":
        slots = _numeric_slots(e)
        c, k = slots[rng.randrange(len(slots))]
        c[k] = str(c[k])
    return e


def _malform(e, rng):
    text = json.dumps(e)
    cut = rng.randint(max(1, len(text) // 4), len(text) - 1)
    return text[:cut]  # any proper prefix of a JSON object is invalid JSON


def _bot_events(first, rng, n=150, span=5.0):
    t0 = _t(first)
    out = []
    for i in range(n):
        ev = copy.deepcopy(first)
        ev["event_id"] = _new_id(rng)
        ev["event_type"] = "page_view"
        ev["event_version"] = 1
        ev["event_time"] = (t0 + timedelta(seconds=span * i / n)).isoformat()
        ev["payload"] = {"page": "home"}
        out.append(("app", ev))
    return out


def _apply(stream, rng, config, catalog_ids, counter, now=None):
    """Core generator yielding (topic_hint, payload, pace_time). pace_time is the original
    event_time of the source message (used for live pacing)."""
    rates = {k: getattr(config, k) for k in KINDS}
    side = random.Random(repr(rng.getstate()))  # does not consume the main rng
    seen_sessions = set()
    pending = None  # (hint, event, pace) held for an out_of_order_status swap

    def out(hint, payload, pace):
        counter.total_messages += 1
        return hint, payload, pace

    for hint, e in stream:
        pace = _t(e)
        if pending is not None:
            ph, pe, ppace = pending
            pending = None
            if hint == "order" and e.get("order_id") == pe.get("order_id"):
                a, b = copy.deepcopy(pe), copy.deepcopy(e)
                a["event_time"], b["event_time"] = e["event_time"], pe["event_time"]
                counter.counts["out_of_order_status"] += 1
                yield out(ph, a, ppace)
                yield out(hint, b, pace)
                continue
            yield out(ph, pe, ppace)  # last event of its order: nothing to swap with

        new_session = False
        sid = e.get("session_id")
        if sid and sid not in seen_sessions:
            seen_sessions.add(sid)
            new_session = rates["bot_session"] > 0 and rng.random() < rates["bot_session"]

        chosen = None
        for kind in MESSAGE_KINDS:
            r = rates[kind]
            if r > 0 and _eligible(kind, e) and (side if kind in SIDE_KINDS else rng).random() < r:
                chosen = kind
                break

        if chosen is None:
            yield out(hint, e, pace)
        elif chosen == "duplicate":
            counter.counts[chosen] += 1
            yield out(hint, e, pace)
            yield out(hint, copy.deepcopy(e), pace)
        elif chosen == "malformed_json":
            counter.counts[chosen] += 1
            yield out(hint, _malform(e, rng), pace)
        elif chosen == "cancel_after_ship":
            yield out(hint, e, pace)
            ct = _t(e) + timedelta(seconds=rng.randint(60, 1800))
            if now is not None and ct > now:
                ct = now  # never put the extra in the future of the wall clock
            if now is None or _t(e) < now:  # shipped at/after now: no extra, not counted
                counter.counts[chosen] += 1
                c = copy.deepcopy(e)
                c["event_id"] = _new_id(rng)
                c["event_type"] = "order_cancelled"
                c["event_time"] = ct.isoformat()
                c["payload"] = {"reason": "customer_request"}
                yield out(hint, c, pace)
        elif chosen == "out_of_order_status":
            pending = (hint, e, pace)  # counted when the successor arrives
        else:
            counter.counts[chosen] += 1
            yield out(hint, _mutate(chosen, e, side if chosen in SIDE_KINDS else rng, catalog_ids, now), pace)

        if new_session:
            counter.counts["bot_session"] += 1
            for bh, be in _bot_events(e, rng):
                yield out(bh, be, pace)

    if pending is not None:
        yield out(*pending)


def apply_noise(stream, rng, config, catalog_ids, counter, now=None):
    """(topic_hint, event) stream in, (topic_hint, dict | broken-JSON str) stream out.

    out_of_order_status is simulated by swapping event_time between an order event and its
    successor in the same order (stream order kept), so the later status carries the earlier
    time. The pair is counted once, when the successor arrives; if the held event was the last
    of its order it is released unchanged and not counted.
    `now` (optional) makes future_timestamp events land after the wall clock even in history mode.
    """
    for hint, payload, _ in _apply(stream, rng, config, catalog_ids, counter, now):
        yield hint, payload


def apply_noise_timed(stream, rng, config, catalog_ids, counter, now=None):
    """Like apply_noise but yields (hint, payload, pace_time) for live pacing."""
    return _apply(stream, rng, config, catalog_ids, counter, now)


def noisy_export(rows, rng, dup_rate=0.02, null_city_rate=0.02):
    """Catalog/customer export noise. Returns (new_rows, counts); input is not mutated."""
    out = []
    counts = {"duplicate": 0, "null_city": 0}
    for row in rows:
        r = copy.deepcopy(row)
        if "city" in r and rng.random() < null_city_rate:
            r["city"] = None
            counts["null_city"] += 1
        out.append(r)
        if rng.random() < dup_rate:
            out.append(copy.deepcopy(r))
            counts["duplicate"] += 1
    return out, counts
