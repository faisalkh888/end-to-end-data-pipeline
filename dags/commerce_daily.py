from datetime import timedelta
import logging
import os
import sys

import pendulum
from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow")
from app.pipeline import run_pipeline


def alert_failure(context):
    # Structured log is visible in Airflow task logs and can be forwarded by a log collector.
    logging.getLogger("airflow.task").error("PIPELINE_ALERT dag=%s task=%s run=%s error=%s",
        context["dag"].dag_id, context["task_instance"].task_id,
        context["run_id"], context.get("exception"))


@dag(
    dag_id="commerce_daily_pipeline",
    schedule="15 2 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 3, "retry_delay": timedelta(minutes=2),
                  "execution_timeout": timedelta(minutes=30), "on_failure_callback": alert_failure},
    tags=["commerce", "bronze-silver-gold"],
)
def commerce_daily_pipeline():
    @task
    def extract_validate_load():
        from datetime import date
        from airflow.operators.python import get_current_context
        return run_pipeline(date.fromisoformat(get_current_context()["ds"]))

    @task
    def report(result: dict):
        logging.info("Pipeline completed: %s", result)
        return result

    report(extract_validate_load())


commerce_daily_pipeline()
