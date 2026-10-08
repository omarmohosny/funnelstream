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
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from streaming.gold.live import live_metrics_for_minutes, minute_epochs
from streaming.silver.writer import silver_path
from warehouse.live_tables import HEALTH_COLUMNS, LIVE_METRIC_COLUMNS

log = logging.getLogger("funnelstream.live_store")

LIVE_KEYS = ["minute_start", "country"]
CONNECT_TIMEOUT = 3  # seconds
LIVENESS_INTERVAL = 15.0  # seconds between "alive" heartbeats of the Silver job


def _utc(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def _read(spark, silver_root, table):
    """The Silver Delta table, or None when it does not exist yet (Silver only creates a table on its first row)."""
    path = silver_path(silver_root, table)
    return spark.read.format("delta").load(path) if DeltaTable.isDeltaTable(spark, path) else None


def refresh_live_metrics(spark, silver_root, minutes, store, bot_lookback_hours=6) -> int:
    """Recompute live_metrics_minute for `minutes` from silver_events and upsert the totals. Returns rows written.

    silver_root is the lake root (Silver tables live in <silver_root>/silver/<table>)."""
    epochs = minute_epochs(minutes)
    if not epochs:
        return 0
    events = _read(spark, silver_root, "silver_events")
    if events is None:
        return 0
    df = live_metrics_for_minutes(events, epochs, bot_lookback_hours)
    df = df.select(F.unix_timestamp("minute_start").alias("m"), "country", *LIVE_METRIC_COLUMNS)
    rows = [dict(minute_start=_utc(r["m"]), country=r["country"], **{c: int(r[c] or 0) for c in LIVE_METRIC_COLUMNS})
            for r in df.collect()]
    return store.upsert("live_metrics_minute", rows, LIVE_KEYS) if rows else 0


def _by_minute(df, ts_col, aggs):
    sec = F.unix_timestamp(ts_col)
    return df.groupBy((sec - (sec % 60)).alias("m")).agg(*aggs)


def refresh_health(spark, silver_root, minutes, store) -> int:
    """Recompute pipeline_health_minute for `minutes` and upsert it. Returns rows written (one per minute).

    events and late_events: silver_events by the minute of kafka_timestamp (arrival). quarantined: silver_quarantine
    by received_at. silver_batches: pipeline_batch_log rows whose finished_at is in the minute."""
    epochs = minute_epochs(minutes)
    if not epochs:
        return 0
    lo, hi = epochs[0], epochs[-1] + 60
    totals = {m: dict.fromkeys(HEALTH_COLUMNS, 0) for m in epochs}

    def inside(ts_col):
        sec = F.unix_timestamp(ts_col)
        return (sec >= lo) & (sec < hi) & (sec - (sec % 60)).isin(epochs)

    ev = _read(spark, silver_root, "silver_events")
    if ev is not None:
        for r in _by_minute(ev.where(inside("kafka_timestamp")), "kafka_timestamp", [
                F.count(F.lit(1)).alias("n"),
                F.sum(F.coalesce(F.col("is_late"), F.lit(False)).cast("int")).alias("late")]).collect():
            totals[r["m"]]["events"] = int(r["n"])
            totals[r["m"]]["late_events"] = int(r["late"] or 0)
    q = _read(spark, silver_root, "silver_quarantine")
    if q is not None:
        for r in _by_minute(q.where(inside("received_at")), "received_at", [F.count(F.lit(1)).alias("n")]).collect():
            totals[r["m"]]["quarantined"] = int(r["n"])
    for (finished,) in store.query("SELECT finished_at FROM pipeline_batch_log WHERE finished_at >= %s AND "
                                   "finished_at < %s", (_utc(lo), _utc(hi))):
        m = int(finished.timestamp()) // 60 * 60
        if m in totals:
            totals[m]["silver_batches"] += 1
    rows = [dict(minute_start=_utc(m), **totals[m]) for m in epochs]
    return store.upsert("pipeline_health_minute", rows, ["minute_start"])


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

    def on_batch(self, spark, batch_id, event_minutes, arrival_minutes=(), quarantine_minutes=(), now=None):
        """Write the live and health rows of one finished Silver batch. Returns (metric minutes, health minutes)
        or None when Postgres could not be written (the minutes stay pending for the next batch)."""
        now = now or datetime.now(timezone.utc)
        self._pending_metrics |= set(minute_epochs(event_minutes))
        self._pending_health |= set(minute_epochs(list(arrival_minutes) + list(quarantine_minutes) + [now]))
        n_metrics, n_health = len(self._pending_metrics), len(self._pending_health)
        if self._clock() < self._down_until:
            log.warning("live_store: Postgres unavailable, %d metric minutes and %d health minutes wait for the "
                        "next batch", n_metrics, n_health)
            return None
        try:
            store = self._get_store()
            refresh_live_metrics(spark, self.silver_root, self._pending_metrics, store)
            log_batch(store, self.job_run_id, batch_id, now)
            refresh_health(spark, self.silver_root, self._pending_health, store)
            heartbeat(store, "silver_job", "batch %s" % batch_id, now)
        except Exception as e:  # Silver is the source of truth; the next batch repairs these minutes
            text = " ".join(str(e).split())[:200]
            log.warning("live_store: batch %s not written to Postgres (%s: %s); retrying with the next batch",
                        batch_id, type(e).__name__, text)
            self._drop_store()
            self._down_until = self._clock() + self._retry_after
            return None
        self._pending_metrics, self._pending_health = set(), set()
        log.info("live_store: %d metric minutes, %d health minutes", n_metrics, n_health)
        return n_metrics, n_health
