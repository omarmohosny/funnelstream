"""Silver job: Bronze Delta tables to validated, typed, de-duplicated Silver Delta tables.

One streaming query reads the union of the three Bronze tables and calls process_batch per
micro-batch. A single query (not one per Bronze table) keeps every Silver table down to one MERGE
writer at a time. Every write is a MERGE that inserts unmatched keys only, so replays (a restart, a
deleted checkpoint) never change the Silver row counts.

Rules per Bronze row: transform_message (pure Python, runs in the executors) returns rows for the Silver
tables or one quarantine row. Quarantine ids are a hash of topic and payload, so a replayed bad message
produces the same quarantine row.

Cancel after shipment: for the batch's order_cancelled rows, the order's order_shipped rows already in
Silver are read and combined with the batch rows, then split_cancel_after_ship runs on the driver
(chunked by order_id if more than 200,000 rows would be combined). Quarantined cancels go to
silver_quarantine instead of silver_order_status. Only cancels of the current batch are routed, rows
already in Silver are never touched. LIMITATION: a cancel that arrives (is written to Silver) BEFORE its
shipped event stays in silver_order_status; the Gold status rank resolves it.

Timezone: the session zone is UTC (see spark_conf). Naive datetimes that reach the Python workers are
read as UTC explicitly (as_utc); the driver never converts timestamps through Python datetimes (status
times are fetched as ISO strings and quarantine times are cast in Spark), so the driver's TZ is irrelevant.

Cancels already present in silver_order_status are treated as decided: never quarantined or re-inserted,
so a full replay leaves every count unchanged.

process_batch returns, per Silver table, the number of rows in this batch after de-duplication on the
table key (rows offered to the MERGE, not rows newly inserted).

District hierarchy: a district that does not belong to the row's city is kept as 'unknown' (never
quarantined). The number of such events and orders in a batch is counted in an accumulator and written to
the batch log line as district_violations (process_batch fills stats["district_violations"]).
"""
import json
import logging
import os
from datetime import datetime, timezone
from functools import partial

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from streaming.common.spark import get_spark
from streaming.jobs.bronze_ingest import BRONZE_TABLES, await_all, bronze_path
from streaming.silver.orders import split_cancel_after_ship
from streaming.silver.schemas import TABLE_COLUMNS
from streaming.silver.spark_schemas import SCHEMAS, coerce_row
from streaming.silver.transform import count_district_violations, transform_message
from streaming.silver.writer import PARTITION_COLS, SILVER_KEYS, merge_insert, silver_path

log = logging.getLogger("silver_clean")

CANCEL_CHUNK_CAP = 200_000
SILVER_TABLES = list(TABLE_COLUMNS)
_warned_no_products = False


def as_utc(value):
    """A datetime received from Spark (naive, UTC session zone) as an aware UTC datetime."""
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _violations_in(raw):
    """1 if a raw message that became a Silver event or order row has a district outside its city."""
    try:
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
        if not isinstance(text, str) or '"district"' not in text:
            return 0
        e = json.loads(text)
        return count_district_violations([e]) if isinstance(e, dict) else 0
    except Exception:
        return 0


def _transform_partition(rows, known_bc, now, violations=None):
    """Executor side: Bronze rows to (table, tuple-in-schema-order) pairs."""
    known = known_bc.value if known_bc is not None else None
    for r in rows:
        received = as_utc(r["ingest_time"])
        kafka_ts = as_utc(r["kafka_timestamp"]) or received
        out = transform_message(r["raw_value"], r["kafka_topic"], kafka_ts, received, known, now)
        if violations is not None and any(t in ("silver_events", "silver_orders") for t, _ in out):
            violations.add(_violations_in(r["raw_value"]))
        for table, row in out:
            yield table, tuple(coerce_row(table, row).values())


def _is_table(table, pair):
    return pair[0] == table


def _values(pair):
    return pair[1]


def load_known_products(lake_root, spark=None):
    """Set of product_ids from silver_products, or None (check skipped, warned once) if it is missing."""
    global _warned_no_products
    spark = spark or SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    path = silver_path(lake_root, "silver_products")
    if not DeltaTable.isDeltaTable(spark, path):
        if not _warned_no_products:
            log.warning("silver_products not found at %s: product id check is skipped", path)
            _warned_no_products = True
        return None
    return {r["product_id"] for r in spark.read.format("delta").load(path).select("product_id").collect()}


_ISO = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSxxx"


def _status_dicts(df):
    """Rows as dicts with status_time as an ISO string (formatted by Spark in the UTC session zone)."""
    df = df.withColumn("status_time", F.date_format("status_time", _ISO))
    return [r.asDict() for r in df.collect()]


def _split_chunked(rows, now):
    """split_cancel_after_ship, in chunks of whole orders when rows exceed CANCEL_CHUNK_CAP."""
    if len(rows) <= CANCEL_CHUNK_CAP:
        return split_cancel_after_ship(rows, now=now)
    log.warning("%d combined status rows exceed %d: processing by order_id chunks", len(rows), CANCEL_CHUNK_CAP)
    by_order = {}
    for r in rows:
        by_order.setdefault(r["order_id"], []).append(r)
    kept, quarantined, chunk = [], [], []
    for group in by_order.values():
        chunk.extend(group)
        if len(chunk) >= CANCEL_CHUNK_CAP:
            k, q = split_cancel_after_ship(chunk, now=now)
            kept += k
            quarantined += q
            chunk = []
    if chunk:
        k, q = split_cancel_after_ship(chunk, now=now)
        kept += k
        quarantined += q
    return kept, quarantined


_QUARANTINE_STR = StructType([
    StructField(f.name, StringType() if f.name == "received_at" else f.dataType, True)
    for f in SCHEMAS["silver_quarantine"].fields])


def _cancel_after_ship(spark, status_df, lake_root, now):
    """Return (status_df without quarantined cancels, list of quarantine row dicts)."""
    rows = _status_dicts(status_df)
    cancel_orders = {r["order_id"] for r in rows if r["status"] == "order_cancelled"}
    if not cancel_orders:
        return status_df, []
    batch_ids = {r["event_id"] for r in rows}
    existing, decided = [], set()
    path = silver_path(lake_root, "silver_order_status")
    if DeltaTable.isDeltaTable(spark, path):
        ids_df = spark.createDataFrame([(o,) for o in cancel_orders], "order_id string")
        existing = _status_dicts(spark.read.format("delta").load(path)
                                 .join(F.broadcast(ids_df), "order_id", "left_semi"))
        decided = {r["event_id"] for r in existing
                   if r["status"] == "order_cancelled" and r["event_id"] in batch_ids}
    relevant = [r for r in rows if r["order_id"] in cancel_orders and r["event_id"] not in decided]
    relevant += [r for r in existing if r["status"] == "order_shipped" and r["event_id"] not in batch_ids]
    _, quarantined = _split_chunked(relevant, now)
    quarantined = [q for q in quarantined if json.loads(q["raw_payload"])["event_id"] in batch_ids]
    if not quarantined:
        return status_df, []
    bad_ids = [json.loads(q["raw_payload"])["event_id"] for q in quarantined]
    return status_df.where(~F.col("event_id").isin(bad_ids)), quarantined


def _minutes(df, col):
    """Distinct minute starts (epoch seconds, UTC) of a timestamp column; no Python datetimes are involved."""
    sec = F.unix_timestamp(col)
    return sorted(r["m"] for r in df.where(F.col(col).isNotNull()).select((sec - (sec % 60)).alias("m"))
                  .distinct().collect())


def process_batch(spark, bronze_df, lake_root, known_products, now=None, stats=None):
    """Bronze rows to the Silver tables (MERGE insert). Returns {table: rows in batch after key dedupe}.

    If stats (a dict) is given, stats["district_violations"] is set to the number of events and orders of
    the batch whose district was outside their city (reported, not an error). With stats["collect_minutes"] true
    it also gets stats["event_minutes"] (minutes of event_time of the batch's Silver events), ["arrival_minutes"]
    (minutes of kafka_timestamp) and ["quarantine_minutes"] (minutes of received_at), as epoch seconds."""
    now = now or datetime.now(timezone.utc)
    cols = ["raw_value", "kafka_topic", "kafka_timestamp", "ingest_time"]
    known_bc = spark.sparkContext.broadcast(set(known_products)) if known_products is not None else None
    violations = spark.sparkContext.accumulator(0)
    pairs = (bronze_df.select(*cols).rdd
             .mapPartitions(partial(_transform_partition, known_bc=known_bc, now=now, violations=violations))
             .persist(StorageLevel.MEMORY_AND_DISK))
    counts = {t: 0 for t in SILVER_TABLES}
    try:
        frames = {}
        for table in SILVER_TABLES:
            rdd = pairs.filter(partial(_is_table, table)).map(_values)
            frames[table] = (spark.createDataFrame(rdd, SCHEMAS[table])
                             .dropDuplicates(SILVER_KEYS[table]))
        frames["silver_order_status"], cancelled = _cancel_after_ship(
            spark, frames["silver_order_status"], lake_root, now)
        if cancelled:
            extra = (spark.createDataFrame(
                [tuple(q["received_at"].isoformat() if c == "received_at" else q[c]
                       for c in TABLE_COLUMNS["silver_quarantine"]) for q in cancelled], _QUARANTINE_STR)
                .withColumn("received_at", F.col("received_at").cast("timestamp"))
                .select(*TABLE_COLUMNS["silver_quarantine"]))
            frames["silver_quarantine"] = frames["silver_quarantine"].unionByName(extra)
        want_minutes = stats is not None and stats.get("collect_minutes")
        for table in SILVER_TABLES:
            df = frames[table].dropDuplicates(SILVER_KEYS[table])
            n = df.count()
            counts[table] = n
            if want_minutes and n and table == "silver_events":
                stats["event_minutes"] = _minutes(df, "event_time")
                stats["arrival_minutes"] = _minutes(df, "kafka_timestamp")
            if want_minutes and n and table == "silver_quarantine":
                stats["quarantine_minutes"] = _minutes(df, "received_at")
            if n:
                merge_insert(spark, silver_path(lake_root, table), df, SILVER_KEYS[table],
                             PARTITION_COLS.get(table))
        if stats is not None:
            stats["district_violations"] = int(violations.value)
    finally:
        pairs.unpersist()
        if known_bc is not None:
            known_bc.unpersist()
    return counts


SHOW_TABLES = [("silver_events", "event_time", "event_type", "district", "platform", "utm_source", "page_url"),
               ("silver_orders", "order_created_at", "order_id", "city", "district", "payment_method", "order_total"),
               ("silver_order_status", "status_time", "order_id", "status"),
               ("silver_quarantine", "received_at", "error_code", "source_topic", "error_detail")]


def _say(text):
    print(text, flush=True)  # flush: the line must appear at once even when stdout is a pipe or a file


def show_batch(spark, lake_root, batch_id, counts, stats, sample=3, out=_say):
    """Terminal view of one Silver batch: the counts and the newest `sample` rows of each table that grew."""
    out("[silver] batch %s  %s  district_violations=%d" % (
        batch_id, "  ".join("%s=%d" % (t.replace("silver_", ""), n) for t, n in counts.items()),
        stats.get("district_violations", 0)))
    for table, order_col, *cols in SHOW_TABLES:
        if not counts.get(table) or not sample:
            continue
        try:
            rows = (spark.read.format("delta").load(silver_path(lake_root, table))
                    .orderBy(F.col(order_col).desc()).select(order_col, *cols).limit(sample).collect())
        except Exception as e:  # keep the stream alive if a sample read fails
            out("          (%s sample unavailable: %s)" % (table, type(e).__name__))
            continue
        for r in rows:
            out("          %-19s %s" % (table.replace("silver_", ""), "  ".join(str(v)[:60] for v in r)))


def live_store_enabled(flag=False, env=None):
    """The live and health tables are written when --live-store is given or PG_DSN is set."""
    env = os.environ if env is None else env
    return bool(flag or env.get("PG_DSN"))


def run(lake_root="data/lake", checkpoint_root="data/checkpoints", once=False, show=0, live_store=False, sink=None):
    lake_root, checkpoint_root = os.path.abspath(lake_root), os.path.abspath(checkpoint_root)
    if sink is None and live_store_enabled(live_store):
        from warehouse.live_store import LiveSink  # psycopg is only imported on the first connect
        sink = LiveSink(lake_root)
    spark = get_spark("silver_clean")
    frames = [spark.readStream.format("delta").option("maxBytesPerTrigger", "64m")
              .load(bronze_path(lake_root, topic)) for topic in BRONZE_TABLES]
    union = frames[0]
    for f in frames[1:]:
        union = union.unionByName(f)

    def handle(df: DataFrame, batch_id):
        sess = df.sparkSession
        stats = {"collect_minutes": sink is not None}
        counts = process_batch(sess, df, lake_root, load_known_products(lake_root, sess), stats=stats)
        log.info("batch %s: %s district_violations=%d", batch_id, counts, stats.get("district_violations", 0))
        if sink is not None:
            sink.on_batch(sess, batch_id, stats.get("event_minutes", ()), stats.get("arrival_minutes", ()),
                          stats.get("quarantine_minutes", ()))  # never raises
        if show:
            show_batch(sess, lake_root, batch_id, counts, stats, sample=show)

    writer = (union.writeStream.foreachBatch(handle)
              .option("checkpointLocation", os.path.join(checkpoint_root, "silver_clean"))
              .queryName("silver_clean"))
    writer = writer.trigger(availableNow=True) if once else writer.trigger(processingTime="10 seconds")
    q = writer.start()
    if sink is not None and not once:
        sink.start_liveness(q)  # silver_job heartbeat every 15 s while the query is active
    return q


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--lake", default="data/lake")
    ap.add_argument("--checkpoints", default="data/checkpoints")
    ap.add_argument("--show", type=int, nargs="?", const=3, default=0, metavar="N",
                    help="print each batch's counts and the newest N rows of every table that grew (default 3)")
    ap.add_argument("--live-store", action="store_true",
                    help="write live_metrics_minute and the health tables to Postgres (also on when PG_DSN is set)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO)
    q = run(args.lake, args.checkpoints, once=args.once, show=args.show, live_store=args.live_store)
    await_all([q], SparkSession.getActiveSession())
