import argparse
import json
import os
import random
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from simulator.campaigns import CAMPAIGNS
from simulator.catalog import make_catalog, make_customers
from simulator.core import journey
from simulator.noise import NoiseConfig, NoiseCounter, apply_noise, apply_noise_timed
from simulator.orders import make_order
from simulator.publish import Publisher, route

AVG_EVENTS_PER_SESSION = 12  # measured: 12.0 to 12.3 (app + order events) per session over seeds 3 and 11


def iter_batch(rng, customers, catalog, start, sessions=None, max_events=None, mean_gap=None, starts=None):
    """Yield (topic_hint, event) pairs session by session without touching Kafka.

    topic_hint is 'app' or 'order'. Stops after `sessions` journeys or once `max_events`
    events were yielded (whichever first); at least one limit is required. max_events is
    approximate: output can exceed it by up to one journey plus its order events (a
    journey and its order lifecycle are never truncated). mean_gap (seconds) switches
    session spacing to exponential with that mean; default is 1-3 s uniform.
    """
    if sessions is None and max_events is None:
        raise ValueError("sessions or max_events required")
    t = start
    n_sessions = 0
    n_events = 0
    while (sessions is None or n_sessions < sessions) and (max_events is None or n_events < max_events):
        n_sessions += 1
        if mean_gap is None:
            t += timedelta(seconds=rng.randint(1, 3))
        else:
            t += timedelta(seconds=rng.expovariate(1.0 / mean_gap))
        cust = rng.choice(customers) if rng.random() < 0.85 else None
        camp = rng.choice(CAMPAIGNS) if rng.random() < 0.4 else None
        events = journey(rng, cust, catalog, camp, t)
        if starts is not None:
            starts[events[0]["session_id"]] = t
        click_id = ("clk_" + events[0]["session_id"][5:]) if camp else None
        for e in events:
            if camp and "utm_campaign=" in e["payload"].get("page_url", ""):
                e["payload"]["click_id"] = click_id
            n_events += 1
            yield "app", e
        purchase = next((e for e in events if e["event_type"] == "purchase"), None)
        if purchase is None or cust is None:
            continue
        order = make_order(rng, cust, catalog, camp, datetime.fromisoformat(purchase["event_time"]),
                           purchase["payload"]["order_id"])
        for _, oe in order:
            if oe["event_type"] == "order_created":
                oe["payload"]["session_id"] = purchase["session_id"]
                oe["payload"]["click_id"] = click_id
                if purchase.get("platform") is not None:
                    oe["payload"]["platform"] = purchase["platform"]
            n_events += 1
            yield "order", oe


def build_batch(rng, customers, catalog, start, sessions=None, max_events=None, mean_gap=None):
    return list(iter_batch(rng, customers, catalog, start, sessions, max_events, mean_gap))


def parse_start(value):
    """ISO string to an aware datetime; naive values are treated as UTC."""
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def resolve_start(start_arg, live, now):
    if live:
        return now
    return parse_start(start_arg) if start_arg else now - timedelta(days=7)


def guard_past(it, now, margin=timedelta(minutes=1)):
    """History mode: raise if any event would land later than now - margin."""
    limit = now - margin
    for hint, e in it:
        if datetime.fromisoformat(e["event_time"]) > limit:
            raise ValueError("event_time %s is later than %s: lower --events or use an earlier --start"
                             % (e["event_time"], limit.isoformat()))
        yield hint, e


def pace_live(batch, emit, now_fn=lambda: datetime.now(timezone.utc), sleep_fn=time.sleep):
    """Emit events in event_time order, never before their event_time on the wall clock."""
    for hint, e in sorted(batch, key=lambda he: datetime.fromisoformat(he[1]["event_time"])):
        wait = (datetime.fromisoformat(e["event_time"]) - now_fn()).total_seconds()
        if wait > 0:
            sleep_fn(wait)
        emit(hint, e)


def compress_orders(batch, time_scale):
    """Live mode: shrink every order lifecycle offset from order_created by time_scale."""
    starts = {e["order_id"]: datetime.fromisoformat(e["event_time"])
              for hint, e in batch if e["event_type"] == "order_created"}
    for hint, e in batch:
        if hint == "order" and e["order_id"] in starts:
            s = starts[e["order_id"]]
            orig = datetime.fromisoformat(e["event_time"])
            e["event_time"] = (s + (orig - s) / time_scale).isoformat()
    return batch


def compress_sessions(batch, session_scale, starts=None):
    """Live mode: shrink the time spread inside each session by session_scale.

    A journey lasts minutes (median 131 s, max 300 s), so without this the last sessions keep emitting for
    minutes after the last session starts and the effective rate falls far below --rate. Offsets from the
    session start (first event if starts is not given) are divided by session_scale (monotone, so order is kept). The order_created
    event follows its purchase; other events of that order keep their offset from order_created (shrunk
    afterwards by compress_orders).
    """
    if not session_scale or session_scale == 1:
        return batch
    anchors = dict(starts or {})
    for hint, e in batch:
        if hint == "app" and e["session_id"] not in anchors:
            t = datetime.fromisoformat(e["event_time"])
            sid = e["session_id"]
            if sid not in anchors or t < anchors[sid]:
                anchors[sid] = t

    def mapped(sid, t):
        a = anchors[sid]
        return a + (t - a) / session_scale

    shift = {}
    for hint, e in batch:
        if hint == "order" and e["event_type"] == "order_created":
            sid = e["payload"].get("session_id")
            if sid in anchors:
                t = datetime.fromisoformat(e["event_time"])
                shift[e["order_id"]] = mapped(sid, t) - t
    for hint, e in batch:
        t = datetime.fromisoformat(e["event_time"])
        if hint == "app":
            e["event_time"] = mapped(e["session_id"], t).isoformat()
        elif e["order_id"] in shift:
            e["event_time"] = (t + shift[e["order_id"]]).isoformat()
    return batch


def cap_order_tail(batch, max_tail):
    """Live mode: scale every order's offset from order_created so the longest is at most max_tail seconds."""
    starts = {e["order_id"]: datetime.fromisoformat(e["event_time"])
              for hint, e in batch if e["event_type"] == "order_created"}
    longest = max((datetime.fromisoformat(e["event_time"]) - starts[e["order_id"]]).total_seconds()
                  for hint, e in batch if hint == "order" and e["order_id"] in starts) if starts else 0
    if not max_tail or longest <= max_tail:
        return batch
    f = max_tail / longest
    for hint, e in batch:
        if hint == "order" and e["order_id"] in starts:
            s = starts[e["order_id"]]
            e["event_time"] = (s + (datetime.fromisoformat(e["event_time"]) - s) * f).isoformat()
    return batch


def pace_stream(timed, emit, now_fn=lambda: datetime.now(timezone.utc), sleep_fn=time.sleep):
    """Like pace_live for a (hint, payload, pace_time) stream: never emit before pace_time."""
    for hint, payload, t in timed:
        wait = (t - now_fn()).total_seconds()
        if wait > 0:
            sleep_fn(wait)
        emit(hint, payload)


def build_parser():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=100_000, help="approximate event count")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--start", default=None, help="ISO start time; default now-7d (history mode)")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--rate", type=float, default=200.0, help="live: events per second")
    ap.add_argument("--time-scale", type=float, default=21600.0,
                    help="live: order lifecycle compression (21600: 6 hours become 1 second)")
    ap.add_argument("--session-scale", type=float, default=60.0,
                    help="live: compress the time spread inside a session (a 5 minute session takes 5 s)")
    ap.add_argument("--max-tail-seconds", type=float, default=60.0,
                    help="live: longest delay of an order lifecycle event after its order_created")
    ap.add_argument("--noise-off", action="store_true", help="publish clean events only")
    ap.add_argument("--noise-scale", type=float, default=1.0, help="multiply every noise rate")
    ap.add_argument("--manifest", default="noise_manifest.json", help="where to write the noise manifest")
    return ap


def run_pipeline(*, seed, events, start, now, live, pub, config, manifest_path, rate=200.0,
                 time_scale=21600.0, session_scale=60.0, max_tail=60.0, now_fn=lambda: datetime.now(timezone.utc), sleep_fn=time.sleep):
    """iter_batch -> (history guard) -> apply_noise -> publish; writes the noise manifest.

    The history guard runs BEFORE noise, so future_timestamp noise events (intentionally in
    the future) never trip it. Noise uses its own rng (derived from the seed) so the clean
    stream is identical with noise on or off. Returns the per-topic message counts.
    """
    rng = random.Random(seed)
    noise_rng = random.Random(seed * 1_000_003 + 7)
    customers, catalog = make_customers(seed, 5_000), make_catalog(seed)
    catalog_ids = {p["product_id"] for p in catalog}
    counter = NoiseCounter()
    counts = Counter()

    def emit(hint, payload):
        topic, key = route(hint, payload)
        pub.send(topic, key, payload)
        counts[topic] += 1

    if live:
        mean_gap = AVG_EVENTS_PER_SESSION / rate
        starts = {}
        batch = list(iter_batch(rng, customers, catalog, start, max_events=events, mean_gap=mean_gap,
                                starts=starts))
        compress_sessions(batch, session_scale, starts)
        compress_orders(batch, time_scale)
        cap_order_tail(batch, max_tail)
        batch.sort(key=lambda he: datetime.fromisoformat(he[1]["event_time"]))
        timed = apply_noise_timed(iter(batch), noise_rng, config, catalog_ids, counter, now=now)
        pace_stream(timed, emit, now_fn, sleep_fn)
    else:
        stream = guard_past(iter_batch(rng, customers, catalog, start, max_events=events), now)
        for hint, payload in apply_noise(stream, noise_rng, config, catalog_ids, counter, now=now):
            emit(hint, payload)
    pub.flush()
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(counter.to_manifest(), f, indent=2)
    return counts


def main():
    args = build_parser().parse_args()
    now = datetime.now(timezone.utc)
    start = resolve_start(args.start, args.live, now)
    config = NoiseConfig.off() if args.noise_off else NoiseConfig().scaled(args.noise_scale)
    pub = Publisher(args.bootstrap)
    counts = run_pipeline(seed=args.seed, events=args.events, start=start, now=now, live=args.live, pub=pub,
                          config=config, manifest_path=args.manifest, rate=args.rate, time_scale=args.time_scale,
                          session_scale=args.session_scale, max_tail=args.max_tail_seconds)
    print(dict(counts), "total", sum(counts.values()))


if __name__ == "__main__":
    main()
