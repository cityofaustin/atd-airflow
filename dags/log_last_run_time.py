from __future__ import annotations

from os import getenv

import pendulum

from airflow.sdk import dag, task

from utils.time import (
    get_current_time,
    get_previous_success_end_time,
    get_previous_success_start_time,
)

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

    @task
    def log_prev_success_age(
        prev_success_start: str | None,
        prev_success_end: str | None,
        now: str,
    ) -> None:
        now_dt = pendulum.parse(now)

        if not prev_success_start and not prev_success_end:
            print(f"No previous successful run found. Now: {now_dt.to_iso8601_string()}")
            return

        if prev_success_start:
            prev_start_dt = pendulum.parse(prev_success_start)
            delta_start = now_dt - prev_start_dt
            print(f"Previous successful run start: {prev_start_dt.to_iso8601_string()}")
            print(f"Start was: {delta_start.in_words()} ago ({delta_start.total_seconds():.0f}s)")

        if prev_success_end:
            prev_end_dt = pendulum.parse(prev_success_end)
            delta_end = now_dt - prev_end_dt
            print(f"Previous successful run end: {prev_end_dt.to_iso8601_string()}")
            print(f"End was: {delta_end.in_words()} ago ({delta_end.total_seconds():.0f}s)")

        print(f"Now: {now_dt.to_iso8601_string()}")

    # TaskFlow return values are passed via XCom automatically.
    prev_success_start = get_previous_success_end_time()
    prev_success_end = get_previous_success_start_time()
    now_cst = get_current_time("America/Chicago")
    log_prev_success_age(prev_success_start, prev_success_end, now_cst)

dag_instance = log_last_run_time()
