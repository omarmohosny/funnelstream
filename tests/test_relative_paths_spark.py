"""Relative lake paths must work for every Delta entry point (Docker: RUN_SPARK_TESTS=1)."""
import os
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.spark

from pyspark.sql import functions as F  # noqa: E402

from scripts.seed_silver_reference import seed  # noqa: E402
from simulator.catalog import make_catalog, make_customers  # noqa: E402
from streaming.gold.build import build_all  # noqa: E402
from streaming.gold.live import live_path, main as live_main  # noqa: E402
from streaming.jobs.bronze_ingest import BRONZE_TABLES, bronze_path  # noqa: E402
from streaming.jobs.silver_clean import SILVER_TABLES, run  # noqa: E402
from streaming.silver.writer import silver_path  # noqa: E402
from test_silver_clean_spark import bronze_df, simulated  # noqa: E402

UPDATED = datetime(2026, 1, 1, tzinfo=timezone.utc)
GOLD = ["fact_orders", "fact_order_lines", "agg_orders_daily", "agg_funnel_daily"]


def _count(spark, path):
    return spark.read.format("delta").load(path).count()


def pipeline(spark, lake, ckpt, out):
    """Seed, Bronze fixture, Silver --once, Gold build, all with the given (maybe relative) paths."""
    msgs, _ = simulated(4_000)
    seed(spark, lake, UPDATED, make_catalog(11), make_customers(11, 300))
    df = bronze_df(spark, msgs)
    for topic in BRONZE_TABLES:
        (df.where(F.col("kafka_topic") == topic).write.format("delta").partitionBy("ingest_date")
         .mode("overwrite").save(bronze_path(lake, topic)))
    run(lake, ckpt, once=True).awaitTermination()
    gold = build_all(spark, lake, out)
    return {
        "silver": {t: _count(spark, silver_path(lake, t)) for t in SILVER_TABLES},
        "reference": {t: _count(spark, silver_path(lake, t)) for t in ("silver_products", "silver_customers")},
        "gold": {t: gold[t].count() for t in GOLD},
    }


@pytest.fixture(scope="module")
def absolute_result(spark, tmp_path_factory):
    base = tmp_path_factory.mktemp("abs")
    return pipeline(spark, str(base / "data/lake"), str(base / "data/checkpoints"), str(base / "data/lake/gold_ref"))


def test_relative_paths_match_absolute_run(spark, tmp_path, monkeypatch, absolute_result):
    monkeypatch.chdir(tmp_path)
    result = pipeline(spark, "data/lake", "data/checkpoints", "data/lake/gold_ref")
    print("\nrelative:", result, "\nabsolute:", absolute_result)
    assert result == absolute_result
    assert result["silver"]["silver_events"] > 0 and result["reference"]["silver_products"] == 200
    assert os.path.isdir(tmp_path / "data" / "lake" / "silver" / "silver_products")
    assert all(n > 0 for n in result["gold"].values())


def test_live_cli_relative_lake(spark, tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    pipeline(spark, "data/lake", "data/checkpoints", "data/lake/gold_ref")
    code = live_main(["--lake", "data/lake", "--minutes", "1000000", "--lookback-hours", "1"])
    out = capsys.readouterr().out
    assert code == 0
    path = live_path("data/lake")
    assert path in out and "minute rows" in out
    assert os.path.isdir(path)
    n = int(out.split("(")[1].split(" ")[0])
    assert n == _count(spark, path)
