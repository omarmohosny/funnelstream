import argparse
import json
import random
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

from simulator.campaigns import CAMPAIGNS
from simulator.catalog import make_catalog, make_customers
from simulator.core import journey
from simulator.noise import NoiseConfig, NoiseCounter, apply_noise, apply_noise_timed
from simulator.orders import make_order
from simulator.publish import Publisher, route

AVG_EVENTS_PER_SESSION = 12  # rough mean (app events + order events) per session


def iter_batch(rng, customers, catalog, start, sessions=None, max_events=None, mean_gap=None):
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
        click_id = ("clk_" + events[0]["session_id"][5:]) if camp else None
        for e in events:
            if "utm_campaign" in e["payload"]:
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
    ap.add_argument("--time-scale", type=float, default=3600.0, help="live: order lifecycle compression")
    ap.add_argument("--noise-off", action="store_true", help="publish clean events only")
    ap.add_argument("--noise-scale", type=float, default=1.0, help="multiply every noise rate")
    ap.add_argument("--manifest", default="noise_manifest.json", help="where to write the noise manifest")
    return ap


def run_pipeline(*, seed, events, start, now, live, pub, config, manifest_path, rate=200.0,
                 time_scale=3600.0, now_fn=lambda: datetime.now(timezone.utc), sleep_fn=time.sleep):
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
        batch = list(iter_batch(rng, customers, catalog, start, max_events=events, mean_gap=mean_gap))
        compress_orders(batch, time_scale)
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
                          config=config, manifest_path=args.manifest, rate=args.rate, time_scale=args.time_scale)
    print(dict(counts), "total", sum(counts.values()))


if __name__ == "__main__":
    main()
