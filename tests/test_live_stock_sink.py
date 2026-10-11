import logging
import re
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from scripts import apply_live_ddl
from warehouse import live_store
from warehouse.live_tables import LIVE_TABLE_NAMES, LIVE_TABLES
from test_g1_live_store import MemStore  # records upserts by primary key, records every call

NOW = datetime(2026, 10, 10, 12, 0, 5, tzinfo=timezone.utc)
DDL = (Path(__file__).parent.parent / "warehouse" / "live_ddl.sql").read_text(encoding="utf-8")
NEW = ["dim_store", "dim_store_district", "dim_product_current", "live_stock_now", "live_oos_demand_minute"]


class StockStore(MemStore):
    """MemStore that also answers the out-of-stock query of write_oos_demand."""

    def __init__(self, oos=(), **kw):
        super().__init__(**kw)
        self.oos = list(oos)

    def query(self, sql, params=None, retry=True):
        if self.fail:
            raise self.fail
        if "live_stock_now" in sql:
            return list(self.oos)
        return []


def test_the_new_tables_are_specified_created_and_granted():
    assert set(NEW) <= set(LIVE_TABLE_NAMES) and len(LIVE_TABLES) == len(LIVE_TABLE_NAMES) == 15
    for name in NEW:
        assert re.search(r"CREATE TABLE IF NOT EXISTS %s \(" % name, DDL), name
        assert name in DDL.split("GRANT SELECT ON")[1].split("TO grafana_ro")[0], name
    spec = {t.name: t for t in LIVE_TABLES}
    assert spec["live_stock_now"].keys == ("store_id", "product_id")
    assert spec["live_oos_demand_minute"].keys == ("minute_start", "store_id", "product_id", "campaign_id")
    assert spec["dim_product_current"].keys == ("product_id",)
    flag = [c for c in spec["live_oos_demand_minute"].columns if c.name == "oos_at_write"]
    assert flag and flag[0].type == "boolean" and re.search(r"oos_at_write\s+boolean", DDL)


def test_apply_live_ddl_seeds_the_store_dimensions():
    store = StockStore()
    apply_live_ddl.main(["--role", "grafana_scratch"], env={"GRAFANA_DB_PASSWORD": "x"}, store=store)
    assert len(store.tables["dim_store"]) == 8
    assert len(store.tables["dim_store_district"]) == 37  # Cairo 15, Giza 8, Alexandria 10, Mansoura 2, Tanta 2
    again = {k: dict(v) for k, v in store.tables["dim_store"].items()}
    apply_live_ddl.main(["--role", "grafana_scratch"], env={"GRAFANA_DB_PASSWORD": "x"}, store=store)
    assert store.tables["dim_store"] == again  # running it twice changes nothing


def test_apply_live_ddl_fills_dim_product_current_from_the_given_rows_and_is_idempotent():
    store = StockStore()
    rows = [dict(product_id="p1", name="Milk 1L", category="dairy", price=Decimal("32.50")),
            dict(product_id="p2", name="Rice 1kg", category="grocery", price=None)]
    env = {"GRAFANA_DB_PASSWORD": "x"}
    apply_live_ddl.main(["--role", "grafana_scratch"], env=env, store=store, product_rows=rows)
    assert sorted(r["name"] for r in store.tables["dim_product_current"].values()) == ["Milk 1L", "Rice 1kg"]
    rows[0] = dict(rows[0], name="Milk 1L Full Fat")  # the catalog changed: the same call updates the row
    apply_live_ddl.main(["--role", "grafana_scratch"], env=env, store=store, product_rows=rows)
    assert sorted(r["name"] for r in store.tables["dim_product_current"].values()) == ["Milk 1L Full Fat", "Rice 1kg"]


def stub(monkeypatch, stock_rows=None, demand_rows=None, fail_stock=None):
    calls = {"stock": [], "demand": []}

    def fake_metrics(spark, root, minutes, bot_lookback_hours=6):
        return []

    def fake_health(spark, root, minutes):
        return {m: dict(events=0, late_events=0, quarantined=0) for m in live_store.minute_epochs(minutes)}

    def fake_stock(spark, root, pairs, now):
        calls["stock"].append(sorted(pairs))
        if fail_stock:
            raise fail_stock
        return stock_rows or []

    def fake_demand(spark, root, minutes):
        calls["demand"].append(sorted(minutes))
        return demand_rows or []

    monkeypatch.setattr(live_store, "compute_live_metric_rows", fake_metrics)
    monkeypatch.setattr(live_store, "compute_health_counts", fake_health)
    monkeypatch.setattr(live_store, "compute_stock_rows", fake_stock)
    monkeypatch.setattr(live_store, "compute_demand_rows", fake_demand)
    monkeypatch.setattr(live_store, "compute_express_rows", lambda spark, root, since: [])  # stock runs it too
    return calls


K = ("store_a", "prod_1")
STOCK = [dict(store_id="store_a", product_id="prod_1", on_hand=0, reserved_qty=0, available=0, reorder_point=10,
              updated_at=NOW)]
DEMAND = [dict(minute_start=NOW, store_id="store_a", product_id="prod_1", campaign_id="camp_201", utm_campaign="eid_sale_1",
               views=3, add_to_cart=2, landing_clicks=1),
          dict(minute_start=NOW, store_id="store_a", product_id="prod_2", campaign_id="direct", utm_campaign="", views=9,
               add_to_cart=1, landing_clicks=0)]


def test_stock_rows_are_written_before_the_demand_rows_and_all_served_demand_is_kept_with_the_flag(monkeypatch):
    stub(monkeypatch, STOCK, DEMAND)
    store = StockStore(oos=[K])
    sink = live_store.LiveSink("lake", store=store, job_run_id="run-1")
    assert sink.on_batch(None, 1, [1_000_000_020], [], [], now=NOW, stock_pairs=[K]) is not None
    tables = [c[0] for c in store.calls]
    assert tables.index("live_stock_now") < tables.index("live_oos_demand_minute")
    rows = {key[2]: row for key, row in store.tables["live_oos_demand_minute"].items()}  # key[2] is the product
    assert set(rows) == {"prod_1", "prod_2"}  # nothing is filtered out at write time
    assert rows["prod_1"]["oos_at_write"] is True and rows["prod_2"]["oos_at_write"] is False
    assert (rows["prod_1"]["views"], rows["prod_1"]["add_to_cart"], rows["prod_1"]["landing_clicks"]) == (3, 2, 1)
    assert rows["prod_1"]["campaign_id"] == "camp_201" and rows["prod_2"]["campaign_id"] == "direct"


def test_a_rewritten_minute_takes_its_flag_from_the_stock_of_that_moment(monkeypatch):
    stub(monkeypatch, STOCK, DEMAND)
    store = StockStore(oos=[K])
    sink = live_store.LiveSink("lake", store=store, job_run_id="run-1")
    sink.on_batch(None, 1, [1_000_000_020], [], [], now=NOW, stock_pairs=[K])

    def flags():
        return {key[2]: row["oos_at_write"] for key, row in store.tables["live_oos_demand_minute"].items()}

    assert flags() == {"prod_1": True, "prod_2": False}
    store.oos = []  # restocked, then the same minute is written again (a late event or a replay)
    sink.on_batch(None, 2, [1_000_000_020], [], [], now=NOW, stock_pairs=[K])
    assert flags() == {"prod_1": False, "prod_2": False}  # known limitation: the flag is the stock when written


def test_without_stock_events_the_inventory_work_is_skipped(monkeypatch):
    calls = stub(monkeypatch, STOCK, DEMAND)
    store = StockStore()
    sink = live_store.LiveSink("lake", store=store, job_run_id="run-1")
    assert sink.on_batch(None, 1, [1_000_000_020], [], [], now=NOW) is not None
    assert calls == {"stock": [], "demand": []}
    assert "live_stock_now" not in store.tables


def test_a_failing_stock_compute_returns_none_and_keeps_the_pairs_pending(monkeypatch, caplog):
    calls = stub(monkeypatch, STOCK, DEMAND, fail_stock=RuntimeError("read failed"))
    store = StockStore(oos=[K])
    sink = live_store.LiveSink("lake", store=store, job_run_id="run-1", retry_after=0.0)
    with caplog.at_level(logging.WARNING, logger="funnelstream.live_store"):
        assert sink.on_batch(None, 1, [1_000_000_020], [], [], now=NOW, stock_pairs=[K]) is None  # never raises
    assert "not written to Postgres" in caplog.text
    first_pairs = calls["stock"][0]
    first_calls = len(calls["stock"])
    stub(monkeypatch, STOCK, DEMAND)  # the problem is gone
    assert sink.on_batch(None, 2, [], [], [], now=NOW) is not None
    assert "live_stock_now" in store.tables and first_pairs == [K] and first_calls == 1
