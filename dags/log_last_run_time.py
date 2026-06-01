from __future__ import annotations

from os import getenv

import pendulum

from airflow.sdk import dag, task

from utils.knack import get_date_filter_arg
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

    @task(task_id="log_previous_success_age")
    def log_prev_success_age(
        prev_success_start: str | None,
        prev_success_end: str | None,
        date_filter_default: str | None,
        date_filter_monthly: str | None,
        now: str,
    ) -> None:
        now_dt = pendulum.parse(now)
        now_str = now_dt.to_iso8601_string()

        def log_date_filter_arg(label: str, date_filter_arg: str | None) -> None:
            if not date_filter_arg:
                print(f"{label}: no date filter provided (empty string)")
                return

            cleaned = date_filter_arg.strip()
            if cleaned.startswith("-d"):
                cleaned = cleaned[2:].strip()

            try:
                filter_dt = pendulum.parse(cleaned)
            except Exception:
                print(f"{label}: unparseable value '{date_filter_arg}'")
                return

            delta_filter = now_dt - filter_dt
            print(
                f"{label}: {filter_dt.to_iso8601_string()} | "
                f"{delta_filter.in_words()} ago ({delta_filter.total_seconds():.0f}s)"
            )

        if not prev_success_start and not prev_success_end:
            print(f"Current time (from get_current_time): {now_str}")
            print("No previous successful run found in task context.")
            log_date_filter_arg(
                "Date filter default (from get_date_filter_arg)",
                date_filter_default,
            )
            log_date_filter_arg(
                "Date filter monthly replace (from get_date_filter_arg)",
                date_filter_monthly,
            )
            return

        if prev_success_start:
            prev_start_dt = pendulum.parse(prev_success_start)
            delta_start = now_dt - prev_start_dt
            print(
                "Previous success start (from prev_success_start): "
                f"{prev_start_dt.to_iso8601_string()} | "
                f"{delta_start.in_words()} ago ({delta_start.total_seconds():.0f}s)"
            )
        else:
            print("Previous success start (from prev_success_start): not available")

        if prev_success_end:
            prev_end_dt = pendulum.parse(prev_success_end)
            delta_end = now_dt - prev_end_dt
            print(
                "Previous success end (from prev_success_end): "
                f"{prev_end_dt.to_iso8601_string()} | "
                f"{delta_end.in_words()} ago ({delta_end.total_seconds():.0f}s)"
            )
        else:
            print("Previous success end (from prev_success_end): not available")

        log_date_filter_arg(
            "Knack Date filter default (from get_date_filter_arg)",
            date_filter_default,
        )
        log_date_filter_arg(
            "Knack Date filter monthly replace (from get_date_filter_arg)",
            date_filter_monthly,
        )
        print(f"Current time (from get_current_time): {now_str}")

    # TaskFlow return values are passed via XCom automatically.
    prev_success_start = get_previous_success_end_time()
    prev_success_end = get_previous_success_start_time()
    now_cst = get_current_time("America/Chicago")

    date_filter_default = get_date_filter_arg.override(
        task_id="get_date_filter_default",
    )()
    date_filter_monthly = get_date_filter_arg.override(
        task_id="get_date_filter_monthly_replace",
    )(should_replace_monthly=True)

    log_prev_success_age(
        prev_success_start,
        prev_success_end,
        date_filter_default,
        date_filter_monthly,
        now_cst,
    )


dag_instance = log_last_run_time()
