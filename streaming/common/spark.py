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
        "spark.sql.session.timeZone": "UTC",
        "spark.executorEnv.TZ": "UTC",
        "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
        "spark.executor.extraJavaOptions": "-Duser.timezone=UTC",
        "spark.ui.enabled": "false",
    }


def get_spark(app_name: str, with_kafka: bool = False) -> SparkSession:
    _ensure_java()
    builder = SparkSession.builder.appName(app_name).master("local[2]")
    for key, value in spark_conf().items():
        builder = builder.config(key, value)
    extra = [KAFKA_PACKAGE] if with_kafka else []
    return configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()
