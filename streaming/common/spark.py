import glob
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"


def _ensure_java() -> None:
    if os.name != "nt" or os.environ.get("JAVA_HOME"):
        return
    candidates = sorted(glob.glob(r"C:\Program Files\Eclipse Adoptium\jdk-17*"))
    if candidates:
        home = candidates[-1]
        os.environ["JAVA_HOME"] = home
        os.environ["PATH"] = os.path.join(home, "bin") + os.pathsep + os.environ.get("PATH", "")


def spark_conf() -> dict:
    """Session settings. Everything runs in UTC: session zone, driver JVM, executor JVMs and the
    Python workers (executorEnv TZ), so Spark timestamps never depend on the machine's local zone."""
    return {
        "spark.driver.memory": os.environ.get("FS_SPARK_DRIVER_MEMORY", "3g"),
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.shuffle.partitions": "8",
        "spark.databricks.delta.snapshotPartitions": os.environ.get("FS_DELTA_SNAPSHOT_PARTITIONS", "4"),
        "spark.sql.session.timeZone": "UTC",
        "spark.executorEnv.TZ": "UTC",
        "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
        "spark.executor.extraJavaOptions": "-Duser.timezone=UTC",
        "spark.ui.enabled": "false",
    }


def spark_master() -> str:
    """local[2] unless FS_SPARK_MASTER asks for more cores (the demo pipeline container does)."""
    return (os.environ.get("FS_SPARK_MASTER") or "").strip() or "local[2]"


def inherit_job_group(spark):
    """Return wrap(fn): fn runs under the Spark job group the caller has now.

    A new Python thread is a new JVM thread in PySpark 3.5 and starts without the caller's job group, so work given
    to a thread pool would not be cancelled by query.stop() and would lose the streaming query's name."""
    sc = getattr(spark, "sparkContext", None)
    if sc is None:  # no Spark session (host tests with a stubbed sink): nothing to inherit
        return lambda fn: fn
    group = sc.getLocalProperty("spark.jobGroup.id")
    description = sc.getLocalProperty("spark.job.description")
    interrupt = sc.getLocalProperty("spark.job.interruptOnCancel") == "true"

    def wrap(fn):
        def run(*args, **kwargs):
            if group:
                sc.setJobGroup(group, description or "", interrupt)
            return fn(*args, **kwargs)
        return run
    return wrap


def get_spark(app_name: str, with_kafka: bool = False) -> SparkSession:
    _ensure_java()
    builder = SparkSession.builder.appName(app_name).master(spark_master())
    for key, value in spark_conf().items():
        builder = builder.config(key, value)
    extra = [KAFKA_PACKAGE] if with_kafka else []
    return configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()
