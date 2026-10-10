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
