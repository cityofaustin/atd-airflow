# test locally with: docker compose run --rm airflow-cli dags test dts_open_311_scrape

from os import getenv

from datetime import timedelta

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert
from utils.time import get_previous_run_date

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "SO_WEB": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.endpoint",
    },
    "SO_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
    "SO_SECRET": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SO_KEY": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "REALTIME_DATASET": {
        "opitem": "Executive Dashboard",
        "opfield": "datasets.Realtime",
    },
    "OPEN_311_API_KEY": {
        "opitem": "Austin Open 311 API key",
        "opfield": "production.API key",
    },
    "OPEN_311_API_BASE_URL": {
        "opitem": "Austin Open 311 API key",
        "opfield": "production.Base URL",
    },
}


with DAG(
    dag_id="dts_open_311_scrape",
    description="Downloads CSRs from Open311 API and publishes them in a Socrata dataset",
    default_args=default_args,
    schedule_interval=(
        "*/5 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None
    ),
    dagrun_timeout=timedelta(minutes=5),
    tags=["repo:dts-311-reporting", "socrata", "csr", "open311"],
    catchup=False,
) as dag:
    docker_image = "atddocker/dts-311-reporting:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)
    date_arg = get_previous_run_date(fallback_date="2025-10-13")

    t1 = DockerOperator(
        task_id="open311_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command=f"python -m etl.open311.open311_to_socrata -d {date_arg["last_run_datetime_iso"]}",
        environment=env_vars,
        tty=True,
        force_pull=True,
    )

    t1
