"""Streaming-side data for the assistant: the events the generator sends to Kafka.

Two ways to get the same events, both landing in DuckDB views the LLM can query:
  - consume_kafka(): read app.events / orders / delivery.events from a real broker
  - simulate():      run the generator's own session code in memory (no Docker needed)

Stage covered: generator -> Kafka -> (here). No Bronze/Silver/Delta involved.
"""
from __future__ import annotations

import json
import random
import sys
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import yaml

import data_layer as dl


def _topics() -> dict:
    return {"app_events": "app.events", "orders": "orders", "delivery": "delivery.events"}


def consume_kafka(bootstrap: str = "localhost:9092", idle_ms: int = 5000, max_messages: int = 500_000) -> dict:
    """Read every message currently in the three topics (earliest -> end), then stop."""
    from kafka import KafkaConsumer

    topics = _topics()
    consumer = KafkaConsumer(
        *topics.values(), bootstrap_servers=bootstrap, auto_offset_reset="earliest",
        enable_auto_commit=False, group_id=None, consumer_timeout_ms=idle_ms,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    out = {t: [] for t in topics.values()}
    try:
        for n, msg in enumerate(consumer):
            if isinstance(msg.value, dict):
                out[msg.topic].append(msg.value)
            if n + 1 >= max_messages:
                break
    finally:
        consumer.close()
    return out


class _CapturingProducer:
    """Stands in for KafkaProducer so the generator's real session code can run offline."""

    def __init__(self):
        self.messages: dict[str, list] = {}

    def send(self, topic, key=None, value=None):
        self.messages.setdefault(topic, []).append(value)


def simulate(scale: str = "small", sessions: int = 2000, seed: int = 42) -> dict:
    """Run this repository's simulator in memory and capture its normal Kafka topics."""
    from scripts.run_simulator import iter_batch
    from simulator.catalog import make_catalog, make_customers
    from simulator.publish import route

    if scale not in {"small", "medium", "large"}:
        raise ValueError(f"Unknown dataset scale {scale!r}")
    customers = make_customers(seed, 5_000)
    products = make_catalog(seed)
    out = {topic: [] for topic in _topics().values()}
    rng = random.Random(seed)
    start = datetime(2026, 10, 1, tzinfo=timezone.utc)
    for hint, event in iter_batch(rng, customers, products, start, sessions=sessions):
        topic, _key = route(hint, event)
        out.setdefault(topic, []).append(event)
    return out


EVENT_VIEWS_SQL = """
-- Deduplicated on event_id, wrong event_version and orphan/invalid ids removed.
CREATE VIEW app_events AS
  SELECT * REPLACE (CAST(event_time AS TIMESTAMP) AS event_time)
  FROM raw_app
  WHERE event_version = 1 AND coalesce(product_id,'') NOT LIKE '%INVALID%'
    AND coalesce(customer_id,'') NOT LIKE '%INVALID%'
  QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY event_time) = 1;

CREATE VIEW order_events AS
  SELECT * REPLACE (CAST(event_time AS TIMESTAMP) AS event_time)
  FROM raw_orders
  WHERE coalesce(customer_id,'') NOT LIKE '%INVALID%'
  QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY event_time) = 1;

CREATE VIEW delivery_events AS
  SELECT d.* REPLACE (CAST(d.event_time AS TIMESTAMP) AS event_time)
  FROM raw_delivery d
  WHERE d.order_id IN (SELECT order_id FROM raw_orders) AND coalesce(d.attempt, 1) >= 1
  QUALIFY row_number() OVER (PARTITION BY d.event_id ORDER BY d.event_time) = 1;

-- One row per order (from order_created). Orders with a negative total or bad product ids are dropped.
CREATE VIEW orders AS
  SELECT order_id, customer_id, session_id, channel, utm_source, utm_campaign, click_id,
         payment_method, city, district, country, order_total, event_time AS order_created_at,
         len(items) AS item_count
  FROM order_events
  WHERE event_type = 'order_created' AND order_total >= 0
    AND list_bool_and(list_transform(items, i -> i.product_id NOT LIKE '%INVALID%'));

CREATE VIEW orders_with_items AS
  SELECT o.order_id, e.items FROM orders o JOIN order_events e
  ON e.order_id = o.order_id AND e.event_type = 'order_created';

CREATE VIEW order_items AS
  SELECT order_id, row_number() OVER (PARTITION BY order_id) AS line_no,
         i.product_id AS product_id, i.quantity AS quantity, i.unit_price AS unit_price, i.line_total AS line_total
  FROM (SELECT order_id, unnest(items) AS i FROM orders_with_items);

-- Status timeline across orders + delivery topics; the current status is the HIGHEST rank, tie -> latest time.
CREATE VIEW order_status_timeline AS
  SELECT order_id, event_type AS status, event_time, carrier, reason,
    CASE event_type WHEN 'order_created' THEN 1 WHEN 'payment_authorised' THEN 2 WHEN 'order_packed' THEN 3
      WHEN 'order_shipped' THEN 4 WHEN 'out_for_delivery' THEN 5 WHEN 'delivery_failed' THEN 5
      WHEN 'delivered' THEN 6 WHEN 'order_cancelled' THEN 2
      WHEN 'order_completed' THEN 7 WHEN 'order_returned' THEN 7 END AS status_rank
  FROM (
    SELECT order_id, event_type, event_time, carrier, reason FROM delivery_events
    UNION ALL
    SELECT order_id, event_type, event_time, NULL, reason FROM order_events
  ) WHERE order_id IN (SELECT order_id FROM orders);

CREATE VIEW order_current AS
  SELECT o.*, s.status AS current_status, s.status_rank,
         coalesce(f.failed_attempts, 0) AS delivery_failed_count,
         s.carrier,
         s.status = 'delivered' AS is_delivered
  FROM orders o
  LEFT JOIN (SELECT * FROM order_status_timeline
             QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY status_rank DESC, event_time DESC) = 1) s
    USING (order_id)
  LEFT JOIN (SELECT order_id, count(*) AS failed_attempts FROM delivery_events
             WHERE event_type = 'delivery_failed' GROUP BY 1) f USING (order_id);

-- Funnel per session: which steps were reached.
CREATE VIEW session_funnel AS
  SELECT session_id, any_value(channel) AS channel, any_value(city) AS city, any_value(device) AS device,
         any_value(utm_campaign) AS utm_campaign,
         bool_or(event_type = 'product_view') AS viewed, bool_or(event_type = 'add_to_cart') AS carted,
         bool_or(event_type = 'checkout_started') AS checkout, bool_or(event_type = 'purchase') AS purchased
  FROM app_events GROUP BY session_id;
"""


def load_events(con, events: dict) -> dict:
    """Create raw tables from the three topics plus the cleaned event views. Returns row counts."""
    t = _topics()
    names = {"app_events": "raw_app", "orders": "raw_orders", "delivery": "raw_delivery"}
    counts = {}
    empty_schemas = {
        "app_events": "event_id VARCHAR, event_type VARCHAR, event_version INTEGER, event_time TIMESTAMP, session_id VARCHAR, device VARCHAR, channel VARCHAR, country VARCHAR, city VARCHAR, district VARCHAR, platform VARCHAR, customer_id VARCHAR, product_id VARCHAR, order_id VARCHAR, utm_source VARCHAR, utm_medium VARCHAR, utm_campaign VARCHAR",
        "orders": "event_id VARCHAR, event_type VARCHAR, event_version INTEGER, event_time TIMESTAMP, session_id VARCHAR, order_id VARCHAR, customer_id VARCHAR, country VARCHAR, city VARCHAR, district VARCHAR, payment_method VARCHAR, items STRUCT(product_id VARCHAR, quantity BIGINT, unit_price DOUBLE, line_total DOUBLE)[], order_total DOUBLE, channel VARCHAR, utm_source VARCHAR, utm_medium VARCHAR, utm_campaign VARCHAR, click_id VARCHAR, carrier VARCHAR, reason VARCHAR, attempts BIGINT",
        "delivery": "event_id VARCHAR, event_type VARCHAR, event_version INTEGER, event_time TIMESTAMP, session_id VARCHAR, order_id VARCHAR, customer_id VARCHAR, country VARCHAR, city VARCHAR, district VARCHAR, carrier VARCHAR, attempt BIGINT, reason VARCHAR",
    }
    scratch = dl.ROOT / "data" / ".tmp"  # keep scratch files on the project drive, not the system temp
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=scratch) as tmp:
        for key, table in names.items():
            # The simulator publishes the stable envelope {metadata, payload}; SQL views
            # use the same flattened shape as the Silver tables.
            rows = []
            for event in events.get(t[key], []):
                if isinstance(event, dict):
                    body = event.get("payload") or {}
                    page_url = body.get("page_url") or ""
                    utm = {k: v[0] for k, v in parse_qs(urlparse(page_url).query).items()
                           if k in {"utm_source", "utm_medium", "utm_campaign"} and v}
                    rows.append({**event, **body, **utm, "payload": body})
            counts[t[key]] = len(rows)
            if not rows:
                con.execute(f"CREATE TABLE {table} ({empty_schemas[key]})")
                continue
            path = Path(tmp) / f"{table}.ndjson"
            path.write_text("\n".join(json.dumps(r) for r in rows) or "{}", encoding="utf-8")
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM read_json_auto(?, format='nd', sample_size=-1)",
                        [path.as_posix()])
        con.execute(EVENT_VIEWS_SQL)
    return counts


def connect_events(source: str, scale: str = "small", bootstrap: str = "localhost:9092",
                   sessions: int = 2000, seed: int = 42):
    """CSV dimensions/ads + the event views. source: 'kafka' or 'sim'."""
    con = dl.connect(scale)
    events = consume_kafka(bootstrap) if source == "kafka" else simulate(scale, sessions, seed)
    counts = load_events(con, events)
    return con, counts
