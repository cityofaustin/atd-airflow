from __future__ import annotations

from os import getenv

import pendulum

from airflow.sdk import dag, get_current_context, task
from airflow.utils.state import DagRunState

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")


@dag(
    dag_id=f"log_last_run_time_{DEPLOYMENT_ENVIRONMENT}",
    schedule=None,
    start_date=pendulum.datetime(2015, 12, 1, tz="America/Chicago"),
    catchup=False,
    tags=["repo:atd-airflow"],
    default_args={
        "owner": "airflow",
        "retries": 0,
        "execution_timeout": pendulum.duration(minutes=5),
    },
    doc_md="Log the most recent successful DAG run end time",
)
def log_last_run_time():
    """Log the last successful DAG run end time."""

    @task(task_id="log_last_run_end")
    def log_last_run_end():
        context = get_current_context()
        last_success_end = context.get("prev_end_date_success")

        if last_success_end is None:
            ti = context["ti"]
            prev_run = ti.get_previous_dagrun(state=DagRunState.SUCCESS)
            last_success_end = prev_run.end_date if prev_run else None

        print(f"Last successful DAG run ended at: {last_success_end}")

    log_last_run_end()


dag_instance = log_last_run_time()
