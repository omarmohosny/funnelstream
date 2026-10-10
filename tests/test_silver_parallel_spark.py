"""Spark tests (run in Docker: RUN_SPARK_TESTS=1) for the speed of one Silver batch.

A Silver batch took about 60 s on the demo laptop because the five table MERGEs ran one after another and
each one cost several small Spark jobs. These tests pin the three properties of the fix: the MERGEs of one batch
run at the same time, a failing MERGE still fails the batch (and a replay repairs it), and a batch uses a bounded
number of Spark jobs."""
import os
import time

import pytest

pytestmark = pytest.mark.spark

import streaming.jobs.silver_clean as silver_clean  # noqa: E402
from streaming.jobs.silver_clean import process_batch  # noqa: E402
from test_silver_clean_spark import NOW, bronze_df, silver_counts, simulated  # noqa: E402

JOB_BUDGET = 64  # Spark jobs for one warm batch of the 6,000-event dataset (67 before the fix: 3 extra distinct jobs)


def _jobs_started(spark, work):
    """Number of Spark jobs the work started, from any thread (jobs without a job group are all counted)."""
    tracker = spark.sparkContext.statusTracker()
    before = set(tracker.getJobIdsForGroup(None))
    work()
    return len(set(tracker.getJobIdsForGroup(None)) - before)


def test_table_merges_of_one_batch_overlap_in_time(spark, tmp_path, monkeypatch):
    real = silver_clean.merge_insert
    spans = {}

    def slow(spark_, path, df, key_cols, partition_col=None, partition_values=None):
        started = time.monotonic()
        time.sleep(0.6)  # long enough that sequential MERGEs cannot overlap
        try:
            return real(spark_, path, df, key_cols, partition_col, partition_values)
        finally:
            spans[os.path.basename(path)] = (started, time.monotonic())

    monkeypatch.setattr(silver_clean, "merge_insert", slow)
    msgs, _ = simulated(6_000)
    process_batch(spark, bronze_df(spark, msgs), str(tmp_path / "lake"), None, now=NOW)

    assert len(spans) == 5, spans
    first_end = min(end for _, end in spans.values())
    started_before_first_end = [t for t, (start, _) in spans.items() if start < first_end]
    assert len(started_before_first_end) >= 2, spans


def test_a_failing_merge_fails_the_batch_and_a_replay_repairs_it(spark, tmp_path, monkeypatch):
    msgs, _ = simulated(6_000)
    df = bronze_df(spark, msgs)
    clean = str(tmp_path / "clean")
    process_batch(spark, df, clean, None, now=NOW)
    expected = silver_counts(spark, clean)

    real = silver_clean.merge_insert

    def boom(spark_, path, d, key_cols, partition_col=None, partition_values=None):
        if path.endswith("silver_orders"):
            raise RuntimeError("disk full")
        return real(spark_, path, d, key_cols, partition_col, partition_values)

    lake = str(tmp_path / "lake")
    monkeypatch.setattr(silver_clean, "merge_insert", boom)
    with pytest.raises(RuntimeError, match="disk full"):
        process_batch(spark, df, lake, None, now=NOW)

    monkeypatch.setattr(silver_clean, "merge_insert", real)
    process_batch(spark, df, lake, None, now=NOW)  # what Spark does when it replays the failed batch
    assert silver_counts(spark, lake) == expected


def test_one_batch_uses_a_bounded_number_of_spark_jobs(spark, tmp_path):
    msgs, _ = simulated(6_000)
    df = bronze_df(spark, msgs)
    lake = str(tmp_path / "lake")
    process_batch(spark, df, lake, None, now=NOW)  # first call creates the tables
    jobs = _jobs_started(spark, lambda: process_batch(spark, df, lake, None, now=NOW))
    print("\nspark jobs for one warm batch: %d (budget %d)" % (jobs, JOB_BUDGET))
    assert jobs <= JOB_BUDGET, jobs


def test_merge_threads_run_under_the_callers_job_group(spark, tmp_path):
    """query.stop() cancels a job group, so the MERGE jobs started in pool threads must belong to the caller's group.
    Without that only the main-thread jobs (about 30 of about 60 to 90) are in the group."""
    msgs, _ = simulated(6_000)
    df = bronze_df(spark, msgs)
    sc = spark.sparkContext
    sc.setJobGroup("silver-test-group", "silver batch", True)
    try:
        process_batch(spark, df, str(tmp_path / "lake"), None, now=NOW)
    finally:
        sc.setLocalProperty("spark.jobGroup.id", None)  # PySpark has no clearJobGroup
    in_group = len(sc.statusTracker().getJobIdsForGroup("silver-test-group"))
    print("jobs in the caller's group: %d" % in_group)
    assert in_group >= 50, in_group
