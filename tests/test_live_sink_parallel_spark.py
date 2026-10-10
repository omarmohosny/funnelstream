"""Spark tests (run in Docker: RUN_SPARK_TESTS=1) for the speed of the live sink.

After each Silver batch the sink recomputes two sets of totals from Silver (live metrics and health). They read
different things and only meet at Postgres, so their Spark work should run at the same time. The Postgres writes
stay one after the other on the single connection."""
import time

import pytest

pytestmark = pytest.mark.spark

from warehouse import live_store  # noqa: E402
from test_g1_live_spark import MemStore, NOW, builder_totals, dataset, store_live, write_events  # noqa: E402


def _minutes(rows):
    return [r["event_time"] for r in rows]


def test_metrics_and_health_read_silver_events_at_the_same_time(spark, tmp_path, monkeypatch):
    lake = str(tmp_path / "lake")
    rows = dataset()
    write_events(spark, lake, rows)
    spans = []
    real = live_store._read

    def slow(spark_, root, table):
        started = time.monotonic()
        time.sleep(0.8)  # long enough that two sequential reads cannot overlap
        try:
            return real(spark_, root, table)
        finally:
            spans.append((table, started, time.monotonic()))

    monkeypatch.setattr(live_store, "_read", slow)
    sink = live_store.LiveSink(lake, store=MemStore(), job_run_id="run-1")
    assert sink.on_batch(spark, 1, _minutes(rows), _minutes(rows), [], now=NOW) is not None

    events = sorted((s for s in spans if s[0] == "silver_events"), key=lambda s: s[1])
    assert len(events) == 2, spans  # one read for the live metrics, one for the health counts
    (_, _, first_end), (_, second_start, _) = events
    assert second_start < first_end, spans


def test_a_failing_read_returns_none_and_the_retry_writes_everything(spark, tmp_path, monkeypatch):
    lake = str(tmp_path / "lake")
    rows = dataset()
    write_events(spark, lake, rows)
    real = live_store._read
    state = {"fail": True}

    def flaky(spark_, root, table):
        if state["fail"] and table == "silver_quarantine":
            raise RuntimeError("read failed")
        return real(spark_, root, table)

    monkeypatch.setattr(live_store, "_read", flaky)
    store = MemStore()
    sink = live_store.LiveSink(lake, store=store, job_run_id="run-1", retry_after=0.0)
    assert sink.on_batch(spark, 1, _minutes(rows), _minutes(rows), [], now=NOW) is None  # never raises

    state["fail"] = False
    assert sink.on_batch(spark, 2, [], [], [], now=NOW) is not None  # the minutes stayed pending
    assert store_live(store) == builder_totals(spark, lake)
    assert store.snapshot("pipeline_health_minute"), "health rows were not written by the retry"


def test_the_sink_writes_the_same_live_totals_as_the_builder(spark, tmp_path):
    lake = str(tmp_path / "lake")
    rows = dataset()
    write_events(spark, lake, rows)
    store = MemStore()
    sink = live_store.LiveSink(lake, store=store, job_run_id="run-1")
    assert sink.on_batch(spark, 1, _minutes(rows), _minutes(rows), [], now=NOW) is not None
    assert store_live(store) == builder_totals(spark, lake)


def test_sink_threads_run_under_the_callers_job_group(spark, tmp_path):
    """All Spark work of the sink runs in its two pool threads; they must belong to the caller's job group."""
    lake = str(tmp_path / "lake")
    rows = dataset()
    write_events(spark, lake, rows)
    sc = spark.sparkContext
    sink = live_store.LiveSink(lake, store=MemStore(), job_run_id="run-1")
    sc.setJobGroup("sink-test-group", "silver batch", True)
    try:
        assert sink.on_batch(spark, 1, _minutes(rows), _minutes(rows), [], now=NOW) is not None
    finally:
        sc.setLocalProperty("spark.jobGroup.id", None)  # PySpark has no clearJobGroup
    in_group = len(sc.statusTracker().getJobIdsForGroup("sink-test-group"))
    print("sink jobs in the caller's group: %d" % in_group)
    assert in_group >= 5, in_group
