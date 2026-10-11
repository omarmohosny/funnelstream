"""Live express delivery SLA rows (inventory design, express delivery).

One Spark compute, run in the live sink's thread pool beside the others. Postgres only stores the facts (when the order
was created, when it was first delivered, whether it was closed without a delivery); Grafana computes elapsed time and
the SLA state from them with now(), so the state moves even between Silver batches.
"""
from pyspark.sql import Window, functions as F

from warehouse.live_common import _read, _utc

CLOSED_STATUSES = ("order_cancelled", "order_returned")


def _epoch_or_none(value):
    return None if value is None else _utc(value)


def compute_express_rows(spark, silver_root, since):
    """live_express_orders rows for every express order created at or after `since` ([] without the express table).

    delivered_at: the earliest `delivered` status_time of the order (None until then; a replayed or duplicated status
    row cannot change it). closed_without_delivery: the order's latest status (greatest status_time, ties by the
    greater status_rank) is order_cancelled or order_returned and the order has no delivered_at.
    Times leave Spark as epoch seconds and become aware UTC datetimes, so the driver's time zone does not matter."""
    orders = _read(spark, silver_root, "silver_express_orders")
    if orders is None:
        return []
    orders = orders.where(F.col("created_at") >= F.lit(since)).select(
        "order_id", "store_id", "promised_minutes", F.unix_timestamp("created_at").alias("created_at"))
    status = _read(spark, silver_root, "silver_order_status")
    if status is None:
        return [dict(order_id=r["order_id"], store_id=r["store_id"], promised_minutes=int(r["promised_minutes"]),
                     created_at=_utc(r["created_at"]), delivered_at=None, closed_without_delivery=False)
                for r in orders.collect()]
    status = status.join(orders.select("order_id"), "order_id", "left_semi")
    delivered = (status.where(F.col("status") == "delivered").groupBy("order_id")
                 .agg(F.unix_timestamp(F.min("status_time")).alias("delivered_at")))
    window = Window.partitionBy("order_id").orderBy(F.col("status_time").desc(), F.col("status_rank").desc())
    latest = (status.withColumn("_rn", F.row_number().over(window)).where("_rn = 1")
              .select("order_id", F.col("status").alias("latest_status")))
    df = (orders.join(delivered, "order_id", "left").join(latest, "order_id", "left")
          .withColumn("closed", F.col("delivered_at").isNull() & F.coalesce(F.col("latest_status").isin(*CLOSED_STATUSES),
                                                                            F.lit(False))))
    return [dict(order_id=r["order_id"], store_id=r["store_id"], promised_minutes=int(r["promised_minutes"]),
                 created_at=_utc(r["created_at"]), delivered_at=_epoch_or_none(r["delivered_at"]),
                 closed_without_delivery=bool(r["closed"]))
            for r in df.collect()]
