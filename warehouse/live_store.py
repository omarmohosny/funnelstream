"""Live and health tables in Postgres, written by the Silver job (spec 17.3, 17.17; contract g-shared-contract.md).

Every measure is a FULL recalculated total for the minutes a batch touched, read from the local Silver tables, and
written with INSERT ... ON CONFLICT DO UPDATE SET <measure> = EXCLUDED.<measure> (store.upsert). Nothing here
increments, so running a batch twice, or replaying it, gives the same rows. Rows are minutes x countries, so they are
collected to the driver and sent through the store (no JDBC).

Silver is the source of truth: LiveSink.on_batch never raises. A database outage logs a warning, the batch still
succeeds, and the minutes that could not be written are retried with the next batch.
"""
import logging
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F

from streaming.common.spark import inherit_job_group
from streaming.gold.live import live_metrics_for_minutes, minute_epochs
from warehouse.live_common import _read, _utc
from warehouse.live_express import compute_express_rows
from warehouse.live_stock import compute_demand_rows, compute_stock_rows, write_oos_demand, write_stock_history
from warehouse.live_tables import HEALTH_COLUMNS, LIVE_METRIC_COLUMNS

log = logging.getLogger("funnelstream.live_store")

LIVE_KEYS = ["minute_start", "country"]
CONNECT_TIMEOUT = 3  # seconds
EXPRESS_WINDOW = timedelta(hours=3)  # an open express order is refreshed until it is delivered or older than this
LIVENESS_INTERVAL = 15.0  # seconds between "alive" heartbeats of the Silver job


def compute_live_metric_rows(spark, silver_root, minutes, bot_lookback_hours=6) -> list:
    """The Spark part of refresh_live_metrics: the live_metrics_minute rows for `minutes`, recomputed from
    silver_events (full totals, never increments). No database is touched, so it can run beside other Spark work.

    silver_root is the lake root (Silver tables live in <silver_root>/silver/<table>)."""
    epochs = minute_epochs(minutes)
    if not epochs:
        return []
    events = _read(spark, silver_root, "silver_events")
    if events is None:
        return []
    df = live_metrics_for_minutes(events, epochs, bot_lookback_hours)
    df = df.select(F.unix_timestamp("minute_start").alias("m"), "country", *LIVE_METRIC_COLUMNS)
    return [dict(minute_start=_utc(r["m"]), country=r["country"], **{c: int(r[c] or 0) for c in LIVE_METRIC_COLUMNS})
            for r in df.collect()]


def refresh_live_metrics(spark, silver_root, minutes, store, bot_lookback_hours=6) -> int:
    """Recompute live_metrics_minute for `minutes` from silver_events and upsert the totals. Returns rows written."""
    rows = compute_live_metric_rows(spark, silver_root, minutes, bot_lookback_hours)
    return store.upsert("live_metrics_minute", rows, LIVE_KEYS) if rows else 0


def _by_minute(df, ts_col, aggs):
    sec = F.unix_timestamp(ts_col)
    return df.groupBy((sec - (sec % 60)).alias("m")).agg(*aggs)


def compute_health_counts(spark, silver_root, minutes) -> dict:
    """The Spark part of refresh_health: {minute epoch: {events, late_events, quarantined}} for `minutes`.

    events and late_events: silver_events by the minute of kafka_timestamp (arrival). quarantined: silver_quarantine
    by received_at. No database is touched, so it can run beside other Spark work."""
    epochs = minute_epochs(minutes)
    counts = {m: dict(events=0, late_events=0, quarantined=0) for m in epochs}
    if not epochs:
        return counts
    lo, hi = epochs[0], epochs[-1] + 60

    def inside(ts_col):
        sec = F.unix_timestamp(ts_col)
        return (sec >= lo) & (sec < hi) & (sec - (sec % 60)).isin(epochs)

    ev = _read(spark, silver_root, "silver_events")
    if ev is not None:
        for r in _by_minute(ev.where(inside("kafka_timestamp")), "kafka_timestamp", [
                F.count(F.lit(1)).alias("n"),
                F.sum(F.coalesce(F.col("is_late"), F.lit(False)).cast("int")).alias("late")]).collect():
            counts[r["m"]]["events"] = int(r["n"])
            counts[r["m"]]["late_events"] = int(r["late"] or 0)
    q = _read(spark, silver_root, "silver_quarantine")
    if q is not None:
        for r in _by_minute(q.where(inside("received_at")), "received_at", [F.count(F.lit(1)).alias("n")]).collect():
            counts[r["m"]]["quarantined"] = int(r["n"])
    return counts


def write_health(store, minutes, counts) -> int:
    """The Postgres part of refresh_health: add silver_batches (pipeline_batch_log rows finished in the minute) to
    the Spark counts and upsert pipeline_health_minute. Returns rows written (one per minute)."""
    epochs = minute_epochs(minutes)
    if not epochs:
        return 0
    lo, hi = epochs[0], epochs[-1] + 60
    totals = {m: dict.fromkeys(HEALTH_COLUMNS, 0) for m in epochs}
    for m in epochs:
        totals[m].update(counts.get(m, {}))
    for (finished,) in store.query("SELECT finished_at FROM pipeline_batch_log WHERE finished_at >= %s AND "
                                   "finished_at < %s", (_utc(lo), _utc(hi))):
        m = int(finished.timestamp()) // 60 * 60
        if m in totals:
            totals[m]["silver_batches"] += 1
    rows = [dict(minute_start=_utc(m), **totals[m]) for m in epochs]
    return store.upsert("pipeline_health_minute", rows, ["minute_start"])


def refresh_health(spark, silver_root, minutes, store) -> int:
    """Recompute pipeline_health_minute for `minutes` and upsert it. Returns rows written (one per minute)."""
    epochs = minute_epochs(minutes)
    if not epochs:
        return 0
    return write_health(store, epochs, compute_health_counts(spark, silver_root, epochs))


def log_batch(store, job_run_id, batch_id, finished_at=None):
    """Insert (or, for a repeated batch, refresh) the batch-log row that silver_batches is counted from."""
    finished_at = finished_at or datetime.now(timezone.utc)
    return store.upsert("pipeline_batch_log", [dict(job_run_id=str(job_run_id), batch_id=int(batch_id),
                                                    finished_at=finished_at)], ["job_run_id", "batch_id"])


def heartbeat(store, component, details=None, now=None):
    """Best effort: True if written, False (logged) if the store failed. Never raises."""
    try:
        store.upsert("pipeline_heartbeat", [dict(component=component, last_seen_at=now or datetime.now(timezone.utc),
                                                 details=details)], ["component"])
        return True
    except Exception as e:
        log.warning("heartbeat %s not written: %s", component, type(e).__name__)
        return False


class LiveSink:
    """What the Silver job calls after each finished batch. on_batch never raises."""

    def __init__(self, silver_root, dsn=None, store=None, job_run_id=None, clock=time.time, retry_after=30.0):
        self.silver_root = silver_root
        self._dsn = dsn if dsn is not None else os.environ.get("PG_DSN")
        self._store = store
        self._own_store = store is None
        self.job_run_id = job_run_id or str(uuid.uuid4())
        self._clock = clock
        self._retry_after = retry_after
        self._down_until = 0.0
        self._pending_metrics, self._pending_health = set(), set()
        self._pending_stock, self._stock_seen = set(), False
        self._express_seen = False
        self._stop_liveness = threading.Event()

    def _get_store(self):
        if self._store is None:
            from warehouse.store import PostgresStore  # psycopg is imported on the first connect only
            self._store = PostgresStore.bounded(self._dsn)
        return self._store

    def _drop_store(self):
        store, self._store = self._store, None
        if store is not None and not self._own_store:
            self._store = store  # a store handed in by the caller is kept (and closed by the caller)
        elif store is not None:
            try:
                store.close()
            except Exception:
                pass

    def close(self):
        self._drop_store()

    def _liveness_beat(self):
        """One 'alive' heartbeat on its own short-lived connection (never the batch thread's), best effort."""
        if self._own_store:
            if self._dsn:
                from warehouse.heartbeat import beat
                beat("silver_job", "alive", env={"PG_DSN": self._dsn})
        else:
            heartbeat(self._store, "silver_job", "alive")

    def start_liveness(self, query, interval=None):
        """Daemon thread: a silver_job heartbeat every `interval` seconds while query.isActive, so the heartbeat
        age means "the job is alive" and not "data arrived" (Spark runs no micro-batch on an idle source)."""
        interval = LIVENESS_INTERVAL if interval is None else interval

        def loop():
            while not self._stop_liveness.wait(interval):
                try:
                    if not query.isActive:
                        return
                    self._liveness_beat()
                except Exception as e:
                    log.warning("liveness heartbeat failed: %s", type(e).__name__)

        self._stop_liveness = threading.Event()
        t = threading.Thread(target=loop, name="silver-liveness", daemon=True)
        t.start()
        return t

    def on_batch(self, spark, batch_id, event_minutes, arrival_minutes=(), quarantine_minutes=(), now=None,
                 stock_pairs=(), express_seen=False):
        """Write the live and health rows of one finished Silver batch. Returns (metric minutes, health minutes)
        or None when Postgres could not be written (the minutes stay pending for the next batch)."""
        now = now or datetime.now(timezone.utc)
        self._pending_metrics |= set(minute_epochs(event_minutes))
        self._pending_health |= set(minute_epochs(list(arrival_minutes) + list(quarantine_minutes) + [now]))
        self._pending_stock |= {tuple(p) for p in stock_pairs}
        self._stock_seen = self._stock_seen or bool(self._pending_stock)
        self._express_seen = self._express_seen or bool(express_seen) or self._stock_seen
        n_metrics, n_health = len(self._pending_metrics), len(self._pending_health)
        if self._clock() < self._down_until:
            log.warning("live_store: Postgres unavailable, %d metric minutes and %d health minutes wait for the "
                        "next batch", n_metrics, n_health)
            return None
        try:
            store = self._get_store()
            metric_minutes, health_minutes = set(self._pending_metrics), set(self._pending_health)
            stock_pairs_now = set(self._pending_stock)
            # The two recomputes only read Silver and meet at Postgres, so their Spark work runs side by side; the
            # writes below stay one after the other on the single connection.
            in_job_group = inherit_job_group(spark)  # so query.stop() also cancels the sink's Spark jobs
            with ThreadPoolExecutor(max_workers=5, thread_name_prefix="live-sink") as pool:
                metric_job = pool.submit(in_job_group(compute_live_metric_rows), spark, self.silver_root,
                                         metric_minutes)
                health_job = pool.submit(in_job_group(compute_health_counts), spark, self.silver_root,
                                         health_minutes)
                stock_job = (pool.submit(in_job_group(compute_stock_rows), spark, self.silver_root,
                                         sorted(stock_pairs_now), now) if stock_pairs_now else None)
                demand_job = (pool.submit(in_job_group(compute_demand_rows), spark, self.silver_root, metric_minutes)
                              if self._stock_seen else None)
                express_job = (pool.submit(in_job_group(compute_express_rows), spark, self.silver_root,
                                           now - EXPRESS_WINDOW) if self._express_seen else None)
            metric_rows, health_counts = metric_job.result(), health_job.result()  # raises the first failure
            stock_rows = stock_job.result() if stock_job else []
            demand_rows = demand_job.result() if demand_job else []
            express_rows = express_job.result() if express_job else []
            if metric_rows:
                store.upsert("live_metrics_minute", metric_rows, LIVE_KEYS)
            log_batch(store, self.job_run_id, batch_id, now)
            write_health(store, health_minutes, health_counts)
            if stock_rows:  # stock first, so the demand flag below sees the new stock
                store.upsert("live_stock_now", stock_rows, ["store_id", "product_id"])
                write_stock_history(store)
            write_oos_demand(store, demand_rows)
            if express_rows:
                store.upsert("live_express_orders", express_rows, ["order_id"])
            heartbeat(store, "silver_job", "batch %s" % batch_id, now)
        except Exception as e:  # Silver is the source of truth; the next batch repairs these minutes
            text = " ".join(str(e).split())[:200]
            log.warning("live_store: batch %s not written to Postgres (%s: %s); retrying with the next batch",
                        batch_id, type(e).__name__, text)
            self._drop_store()
            self._down_until = self._clock() + self._retry_after
            return None
        self._pending_metrics, self._pending_health = set(), set()
        self._pending_stock = set()
        log.info("live_store: %d metric minutes, %d health minutes", n_metrics, n_health)
        return n_metrics, n_health
