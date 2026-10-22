"""Daily DAG: yesterday's ad report. Runs at 03:00 Cairo time, when the Cairo day that just ended is complete.

First run of a day pulls the mock ad API and stores the report; any later run the same day returns the stored
report without pulling (batch.ads_report.run). Trigger with config {"report_date": "YYYY-MM-DD"} to backfill a day,
or {"force": true} to pull again.
"""
import os
from datetime import date, datetime, timedelta, timezone

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

from batch.ads_report import format_report, run
from warehouse.store import get_store


def ads_report(**context):
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    report_date = date.fromisoformat(conf["report_date"]) if conf.get("report_date") else None
    with get_store("databricks") as store:
        result = run(store, os.environ.get("AD_API_URL", "http://host.docker.internal:8000"),
                     now=datetime.now(timezone.utc), report_date=report_date, force=bool(conf.get("force")))
    print(format_report(result))
    return {"report_date": result["report_date"].isoformat(), "status": result["status"],
            "campaigns": len(result["rows"]), "rejected": result["rejected"], "file": result["path"]}


with DAG(
    dag_id="ads_yesterday_report",
    description="Yesterday's ad report: pull once per day, serve the stored copy on reruns",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Africa/Cairo"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["funnelstream", "ads"],
) as dag:
    PythonOperator(task_id="ads_yesterday_report", python_callable=ads_report)
