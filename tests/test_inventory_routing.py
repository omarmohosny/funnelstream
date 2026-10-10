import random

import yaml
from pathlib import Path

from simulator.noise import NoiseConfig, NoiseCounter, apply_noise
from simulator.publish import route
from streaming.jobs.bronze_ingest import BRONZE_TABLES, bronze_path

TOPICS = yaml.safe_load((Path(__file__).parent.parent / "contracts" / "topics.yaml").read_text())["topics"]


def stock_event(i):
    return {"event_id": "evt_%d" % i, "event_type": "stock_movement", "event_version": 1,
            "event_time": "2026-10-10T11:00:%02d+00:00" % (i % 60), "store_id": "store_cairo_maadi",
            "product_id": "prod_0001", "payload": {"qty_delta": -1, "reason": "sold", "order_id": "ord_%d" % i}}


def test_inventory_events_go_to_their_topic_keyed_by_store():
    assert route("inventory", stock_event(1)) == ("inventory.events", "store_cairo_maadi")
    assert route("inventory", {"event_type": "stock_snapshot"}) == ("inventory.events", "unparsable")


def test_every_topic_has_a_bronze_table_and_the_new_one_is_named_as_agreed():
    assert set(BRONZE_TABLES) == set(TOPICS)
    assert BRONZE_TABLES["inventory.events"] == "bronze_inventory_events"
    assert bronze_path("lake", "inventory.events").endswith("bronze_inventory_events")


def test_noise_never_touches_inventory_events():
    events = [("inventory", stock_event(i)) for i in range(400)]
    before = [dict(e) for _, e in events]
    counter = NoiseCounter()
    out = list(apply_noise(iter(events), random.Random(3), NoiseConfig().scaled(20), set(), counter))
    assert [h for h, _ in out] == ["inventory"] * 400
    assert [p for _, p in out] == before  # same events, same order, nothing changed or dropped or duplicated
    assert sum(counter.counts.values()) == 0
    assert counter.total_messages == 400
