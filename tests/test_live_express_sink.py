import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from warehouse import live_store
from warehouse.live_tables import LIVE_TABLE_NAMES, LIVE_TABLES
from test_live_stock_sink import StockStore  # MemStore that also answers the out-of-stock query

NOW = datetime(2026, 10, 10, 12, 0, 5, tzinfo=timezone.utc)
DDL = (Path(__file__).parent.parent / "warehouse" / "live_ddl.sql").read_text(encoding="utf-8")
ROW = dict(order_id="ord_1", store_id="store_cairo_maadi", promised_minutes=20, created_at=NOW - timedelta(minutes=5),
           delivered_at=None, closed_without_delivery=False)


def test_the_express_table_is_specified_created_and_granted():
    assert "live_express_orders" in LIVE_TABLE_NAMES and len(LIVE_TABLES) == len(LIVE_TABLE_NAMES) == 15
    spec = {t.name: t for t in LIVE_TABLES}["live_express_orders"]
    assert spec.keys == ("order_id",)
    assert spec.column_names() == ["order_id", "store_id", "promised_minutes", "created_at", "delivered_at",
                                   "closed_without_delivery"]
    body = re.search(r"CREATE TABLE IF NOT EXISTS live_express_orders \((.*?)\n\);", DDL, re.S).group(1)
    assert re.search(r"order_id\s+text\s+PRIMARY KEY", body)
    assert re.search(r"delivered_at\s+timestamptz(?!\s+NOT NULL)", body)  # nullable
    assert re.search(r"closed_without_delivery\s+boolean\s+NOT NULL DEFAULT false", body)
    assert "live_express_orders" in DDL.split("GRANT SELECT ON")[1].split("TO grafana_ro")[0]


def stub(monkeypatch, express_rows=None, fail_express=None):
    calls = {"express": [], "stock": []}

    def fake_metrics(spark, root, minutes, bot_lookback_hours=6):
        return []

    def fake_health(spark, root, minutes):
        return {m: dict(events=0, late_events=0, quarantined=0) for m in live_store.minute_epochs(minutes)}

    def fake_stock(spark, root, pairs, now):
        calls["stock"].append(sorted(pairs))
        return []

    def fake_demand(spark, root, minutes):
        return []

    def fake_express(spark, root, since):
        calls["express"].append(since)
        if fail_express:
            raise fail_express
        return express_rows or []

    monkeypatch.setattr(live_store, "compute_live_metric_rows", fake_metrics)
    monkeypatch.setattr(live_store, "compute_health_counts", fake_health)
    monkeypatch.setattr(live_store, "compute_stock_rows", fake_stock)
    monkeypatch.setattr(live_store, "compute_demand_rows", fake_demand)
    monkeypatch.setattr(live_store, "compute_express_rows", fake_express)
    return calls


def test_an_express_batch_computes_since_three_hours_and_upserts_by_order_id(monkeypatch):
    calls = stub(monkeypatch, [ROW])
    store = StockStore()
    sink = live_store.LiveSink("lake", store=store, job_run_id="run-1")
    assert sink.on_batch(None, 1, [1_000_000_020], [], [], now=NOW, express_seen=True) is not None
    assert calls["express"] == [NOW - timedelta(hours=3)]
    assert store.tables["live_express_orders"][("ord_1",)]["promised_minutes"] == 20
    assert ("live_express_orders", [ROW], ["order_id"]) in store.calls


def test_a_later_batch_without_new_express_rows_still_refreshes_the_open_orders(monkeypatch):
    calls = stub(monkeypatch, [ROW])
    sink = live_store.LiveSink("lake", store=StockStore(), job_run_id="run-1")
    sink.on_batch(None, 1, [], [], [], now=NOW, express_seen=True)
    sink.on_batch(None, 2, [], [], [], now=NOW + timedelta(seconds=10))  # express was seen before: keep refreshing
    assert len(calls["express"]) == 2


def test_stock_activity_also_runs_the_express_compute(monkeypatch):
    calls = stub(monkeypatch)
    sink = live_store.LiveSink("lake", store=StockStore(), job_run_id="run-1")
    sink.on_batch(None, 1, [], [], [], now=NOW, stock_pairs=[("store_a", "prod_1")])
    assert len(calls["express"]) == 1


def test_when_no_express_order_was_ever_seen_the_express_compute_is_skipped(monkeypatch):
    calls = stub(monkeypatch, [ROW])
    store = StockStore()
    sink = live_store.LiveSink("lake", store=store, job_run_id="run-1")
    assert sink.on_batch(None, 1, [1_000_000_020], [], [], now=NOW) is not None
    assert calls["express"] == [] and "live_express_orders" not in store.tables


def test_a_failing_express_compute_returns_none_and_the_work_stays_pending(monkeypatch, caplog):
    calls = stub(monkeypatch, [ROW], fail_express=RuntimeError("read failed"))
    store = StockStore()
    sink = live_store.LiveSink("lake", store=store, job_run_id="run-1", retry_after=0.0)
    with caplog.at_level(logging.WARNING, logger="funnelstream.live_store"):
        assert sink.on_batch(None, 1, [], [], [], now=NOW, express_seen=True) is None  # never raises
    assert "not written to Postgres" in caplog.text and len(calls["express"]) == 1
    stub(monkeypatch, [ROW])  # the problem is gone; the next batch has no express news of its own
    assert sink.on_batch(None, 2, [], [], [], now=NOW) is not None
    assert "live_express_orders" in store.tables


def test_express_times_leave_spark_as_epoch_seconds_and_are_converted_with_utc():
    """M1: a naive datetime from collect() is in the driver's zone; the other computes use epoch seconds + _utc."""
    from pathlib import Path
    text = (Path(__file__).resolve().parents[1] / "warehouse" / "live_express.py").read_text(encoding="utf-8")
    assert text.count("F.unix_timestamp(") >= 2 and text.count("_utc(") >= 2
    assert 'created_at=r["created_at"]' not in text and 'delivered_at=r["delivered_at"]' not in text
