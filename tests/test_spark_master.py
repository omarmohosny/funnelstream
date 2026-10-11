"""The Spark master (number of local cores) is chosen by one function so a demo can ask for more cores
without touching the code of the jobs; tests and every other job keep two cores by default."""
from streaming.common.spark import spark_master


def test_master_defaults_to_two_cores(monkeypatch):
    monkeypatch.delenv("FS_SPARK_MASTER", raising=False)
    assert spark_master() == "local[2]"


def test_master_can_be_overridden_by_environment(monkeypatch):
    monkeypatch.setenv("FS_SPARK_MASTER", "local[4]")
    assert spark_master() == "local[4]"


def test_blank_override_falls_back_to_the_default(monkeypatch):
    monkeypatch.setenv("FS_SPARK_MASTER", "  ")
    assert spark_master() == "local[2]"


def test_delta_log_replay_uses_few_tasks_by_default(monkeypatch):
    """Delta replays its log with 50 tasks by default (snapshotPartitions); on local[2] that is pure overhead
    for every snapshot a stream or MERGE loads, so the session asks for 4. The tests already used 2."""
    from streaming.common.spark import spark_conf
    monkeypatch.delenv("FS_DELTA_SNAPSHOT_PARTITIONS", raising=False)
    assert spark_conf()["spark.databricks.delta.snapshotPartitions"] == "4"


def test_delta_snapshot_partitions_can_be_overridden_by_environment(monkeypatch):
    from streaming.common.spark import spark_conf
    monkeypatch.setenv("FS_DELTA_SNAPSHOT_PARTITIONS", "50")
    assert spark_conf()["spark.databricks.delta.snapshotPartitions"] == "50"
