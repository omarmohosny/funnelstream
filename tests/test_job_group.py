"""A new Python thread is a new JVM thread in PySpark 3.5 and starts without the caller's Spark job group.
Work handed to a thread pool must therefore re-apply the group, or query.stop() cannot cancel it and its jobs
lose the streaming query's name."""
import threading
from concurrent.futures import ThreadPoolExecutor

from streaming.common.spark import inherit_job_group


class FakeContext:
    def __init__(self, props):
        self._props = props
        self.set_calls = []  # (thread name, group, description, interrupt)

    def getLocalProperty(self, key):
        return self._props.get(key)

    def setJobGroup(self, group, description, interruptOnCancel=False):
        self.set_calls.append((threading.current_thread().name, group, description, interruptOnCancel))


class FakeSpark:
    def __init__(self, props):
        self.sparkContext = FakeContext(props)


def test_wrapped_work_runs_in_the_callers_job_group_on_another_thread():
    spark = FakeSpark({"spark.jobGroup.id": "query-1", "spark.job.description": "silver batch",
                       "spark.job.interruptOnCancel": "true"})
    wrap = inherit_job_group(spark)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="worker") as pool:
        result = pool.submit(wrap(lambda a, b: a + b), 2, 3).result()
    assert result == 5
    [(thread, group, description, interrupt)] = spark.sparkContext.set_calls
    assert thread.startswith("worker") and group == "query-1"
    assert description == "silver batch" and interrupt is True


def test_without_a_job_group_the_work_runs_unchanged():
    spark = FakeSpark({})
    wrap = inherit_job_group(spark)
    assert wrap(lambda: "done")() == "done"
    assert spark.sparkContext.set_calls == []


def test_an_error_in_the_wrapped_work_still_propagates():
    spark = FakeSpark({"spark.jobGroup.id": "query-1"})
    wrap = inherit_job_group(spark)

    def boom():
        raise ValueError("merge failed")

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(wrap(boom))
    try:
        future.result()
    except ValueError as e:
        assert "merge failed" in str(e)
    else:
        raise AssertionError("the error was swallowed")


def test_without_a_spark_session_the_work_runs_unchanged():
    wrap = inherit_job_group(None)
    assert wrap(lambda: "done")() == "done"
