import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.run_simulator import build_parser, format_event, run_pipeline
from simulator.noise import NoiseConfig

NOW = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)


class FakePub:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, event):
        self.sent.append((topic, key, event))

    def flush(self):
        pass


def run(tmp_path, **kw):
    pub = FakePub()
    counts = run_pipeline(seed=5, events=400, start=NOW - timedelta(days=3), now=NOW, live=kw.pop("live", False), pub=pub,
                          config=NoiseConfig.off(), manifest_path=str(tmp_path / "m.json"),
                          sleep_fn=lambda s: None, **kw)
    return counts, pub.sent


def test_the_flags_exist_and_default_to_off():
    args = build_parser().parse_args([])
    assert args.inventory is False and args.scenario is None
    args = build_parser().parse_args(["--inventory", "--scenario", "hero-stockout"])
    assert args.inventory is True and args.scenario == "hero-stockout"


def test_without_the_flag_no_inventory_topic_is_used(tmp_path):
    counts, sent = run(tmp_path)
    assert "inventory.events" not in counts and {t for t, _, _ in sent} <= {"app.events", "orders", "delivery.events"}


def test_history_mode_publishes_opening_snapshots_first_and_keys_by_store(tmp_path):
    counts, sent = run(tmp_path, inventory=True)
    stock = [(t, k, e) for t, k, e in sent if t == "inventory.events"]
    assert counts["inventory.events"] == len(stock) > 1000  # 8 stores x active products of opening snapshots
    assert all(k == e["store_id"] for _, k, e in stock)
    assert stock[0][2]["event_type"] == "stock_snapshot"
    assert any(e["event_type"] == "stock_movement" and e["payload"]["reason"] == "sold" for _, _, e in stock)


def test_live_mode_adds_the_same_kind_of_events_in_time_order(tmp_path):
    live_now = datetime.now(timezone.utc)
    pub = FakePub()
    run_pipeline(seed=5, events=300, start=live_now, now=live_now, live=True, pub=pub, config=NoiseConfig.off(),
                 manifest_path=str(tmp_path / "m.json"), rate=50.0, sleep_fn=lambda s: None,
                 now_fn=lambda: live_now + timedelta(hours=1), inventory=True, scenario="hero-stockout")
    stock = [e for t, _, e in pub.sent if t == "inventory.events"]
    assert stock and stock[0]["event_type"] == "stock_snapshot"
    times = [e["event_time"] for e in stock]
    assert times == sorted(times)
    assert any(e["event_type"] == "stock_movement" and e["payload"]["reason"] == "sold" for e in stock)


def test_show_prints_the_store_and_quantity_of_a_stock_event():
    line = format_event("inventory.events", {"event_type": "stock_movement", "event_time": "2026-10-10T11:00:05+00:00",
                                              "store_id": "store_cairo_maadi", "product_id": "prod_0001",
                                              "payload": {"qty_delta": -2, "reason": "sold"}})
    assert "store=" in line and "prod_0001" in line and "qty_delta=-2" in line


def test_the_demo_script_has_an_inventory_switch():
    text = (Path(__file__).parent.parent / "scripts" / "watch_demo.ps1").read_text(encoding="utf-8")
    assert "[switch]$Inventory" in text and "--inventory --scenario hero-stockout" in text
    assert "[int]$ExpressPromiseMinutes" in text and "[int]$LateExpressEvery" in text
    assert "--express-promise-minutes $ExpressPromiseMinutes" in text and "--late-express-every $LateExpressEvery" in text
    assert "-ExpressPromiseMinutes 2 -LateExpressEvery 15" in text  # the documented rehearsal
