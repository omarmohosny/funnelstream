"""Spark tests (Docker, RUN_SPARK_TESTS=1): express orders through process_batch."""
import json
from datetime import date, datetime, timezone

import pytest

pytestmark = pytest.mark.spark

from streaming.jobs.silver_clean import process_batch  # noqa: E402
from streaming.silver.spark_schemas import coerce_row  # noqa: E402
from test_silver_clean_spark import NOW, bronze_df, silver, silver_counts  # noqa: E402

T = datetime(2026, 10, 1, 11, 0, tzinfo=timezone.utc)  # before the module's NOW (4 Oct) so nothing is "future"


def created(i, district="Maadi", city="Cairo", **payload):
    body = {"order_id": "ord_%d" % i, "payment_method": "card", "order_total": 10.0,
            "items": [{"product_id": "prod_1", "quantity": 1, "unit_price": 10.0, "line_total": 10.0}]}
    body.update(payload)
    return {"event_id": "evt_o%d" % i, "event_type": "order_created", "event_version": 1,
            "event_time": T.isoformat(), "order_id": "ord_%d" % i, "customer_id": "cust_1", "city": city,
            "district": district, "payload": body}


def messages(events):
    kafka_ts = T.replace(tzinfo=None)
    return [("orders", json.dumps(e), kafka_ts) for e in events]


EVENTS = [created(1, service="express", promised_minutes=20),
          created(2, district="Dokki", city="Giza", service="express", promised_minutes=2),
          created(3),  # standard
          created(4, service="standard")]


def test_express_orders_get_one_express_row_each_and_standard_orders_none(spark, tmp_path):
    lake = str(tmp_path / "lake")
    counts = process_batch(spark, bronze_df(spark, messages(EVENTS)), lake, None, now=NOW)
    assert counts["silver_express_orders"] == 2 and counts["silver_orders"] == 4
    assert silver_counts(spark, lake)["silver_express_orders"] == 2
    rows = {r["order_id"]: r for r in silver(spark, lake, "silver_express_orders").collect()}
    assert set(rows) == {"ord_1", "ord_2"}
    assert (rows["ord_1"]["store_id"], rows["ord_1"]["promised_minutes"]) == ("store_cairo_maadi", 20)
    assert (rows["ord_2"]["store_id"], rows["ord_2"]["promised_minutes"]) == ("store_giza_dokki", 2)
    assert rows["ord_1"]["event_date"] == date(2026, 10, 1)
    assert rows["ord_1"]["created_at"] == T.replace(tzinfo=None)


def test_a_replay_and_a_duplicate_change_nothing(spark, tmp_path):
    lake = str(tmp_path / "lake")
    df = bronze_df(spark, messages(EVENTS + [EVENTS[0]]))  # one order delivered twice in the same batch
    process_batch(spark, df, lake, None, now=NOW)
    first = silver_counts(spark, lake)
    process_batch(spark, df, lake, None, now=NOW)  # the batch replayed
    assert silver_counts(spark, lake) == first
    assert first["silver_express_orders"] == 2


def test_an_express_order_outside_a_served_district_goes_to_quarantine(spark, tmp_path):
    lake = str(tmp_path / "lake")
    bad = created(9, district="Assiut", city="Assiut", service="express", promised_minutes=20)
    counts = process_batch(spark, bronze_df(spark, messages([EVENTS[0], bad])), lake, None, now=NOW)
    assert counts["silver_express_orders"] == 1 and counts["silver_orders"] == 1
    assert counts["silver_quarantine"] == 1
    q = silver(spark, lake, "silver_quarantine").first()
    assert q["error_code"] == "BAD_VALUE" and q["error_detail"] == "express order outside a served district"


def test_the_row_coerces_to_the_spark_types():
    out = coerce_row("silver_express_orders", {"order_id": "ord_1", "store_id": "s", "promised_minutes": "20",
                                                "created_at": "2026-10-01T11:00:00+00:00",
                                                "event_date": "2026-10-01"})
    assert out["promised_minutes"] == 20 and type(out["promised_minutes"]) is int
    assert out["created_at"] == datetime(2026, 10, 1, 11, 0) and out["event_date"] == date(2026, 10, 1)
