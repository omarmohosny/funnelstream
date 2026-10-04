import argparse
import random
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

from simulator.campaigns import CAMPAIGNS
from simulator.catalog import make_catalog, make_customers
from simulator.core import journey
from simulator.orders import make_order
from simulator.publish import Publisher, route


def build_batch(rng, customers, catalog, start, sessions=None, max_events=None):
    """Generate (topic_hint, event) pairs without touching Kafka.

    topic_hint is 'app' or 'order'. Stops after `sessions` journeys or once
    `max_events` events exist (whichever comes first); at least one limit is required.
    """
    if sessions is None and max_events is None:
        raise ValueError("sessions or max_events required")
    out = []
    t = start
    n_sessions = 0
    while (sessions is None or n_sessions < sessions) and (max_events is None or len(out) < max_events):
        n_sessions += 1
        t += timedelta(seconds=rng.randint(1, 3))
        cust = rng.choice(customers) if rng.random() < 0.85 else None
        camp = rng.choice(CAMPAIGNS) if rng.random() < 0.4 else None
        events = journey(rng, cust, catalog, camp, t)
        click_id = ("clk_" + events[0]["session_id"][5:]) if camp else None
        for e in events:
            if "utm_campaign" in e["payload"]:
                e["payload"]["click_id"] = click_id
            out.append(("app", e))
        purchase = next((e for e in events if e["event_type"] == "purchase"), None)
        if purchase is None or cust is None:
            continue
        order = make_order(rng, cust, catalog, camp, datetime.fromisoformat(purchase["event_time"]),
                           purchase["payload"]["order_id"])
        for _, oe in order:
            if oe["event_type"] == "order_created":
                oe["payload"]["session_id"] = purchase["session_id"]
                oe["payload"]["click_id"] = click_id
            out.append(("order", oe))
    return out


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=100_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--start", default=None, help="ISO start time; default now-7d (history mode)")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--rate", type=float, default=200.0, help="live: events per second")
    ap.add_argument("--time-scale", type=float, default=3600.0, help="live: order lifecycle compression")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    if args.live:
        start = now
    elif args.start:
        start = datetime.fromisoformat(args.start)
    else:
        start = now - timedelta(days=7)

    rng = random.Random(args.seed)
    batch = build_batch(rng, make_customers(args.seed, 5_000), make_catalog(args.seed), start,
                        max_events=args.events)
    if args.live:
        batch = compress_orders(batch, args.time_scale)
        batch.sort(key=lambda he: he[1]["event_time"])

    pub = Publisher(args.bootstrap)
    counts = Counter()
    t0 = time.monotonic()
    for i, (hint, e) in enumerate(batch):
        topic, key = route(hint, e)
        pub.send(topic, key, e)
        counts[topic] += 1
        if args.live:
            delay = (i + 1) / args.rate - (time.monotonic() - t0)
            if delay > 0:
                time.sleep(delay)
    pub.flush()
    print(dict(counts), "total", len(batch))


if __name__ == "__main__":
    main()
