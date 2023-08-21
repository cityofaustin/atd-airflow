# test locally with: docker compose run --rm airflow-cli dags test atd_trail_counters

import os

from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": timedelta(minutes=60),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-trail-counters:production"

REQUIRED_SECRETS = {
    "SO_WEB": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.endpoint",
    },
    "SO_PASS": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SO_USER": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "COUNTERS_DATASET": {
        "opitem": "Trail Counters",
        "opfield": "production.Dataset ID",
    },
    "DEVICE_DATASET": {
        "opitem": "Trail Counters",
        "opfield": "production.Device ID",
    },
}

with DAG(
    dag_id="atd_trail_counters",
    description="Scrapes trail counter data from the public eco-counter website and publishes it in Socrata",
    default_args=default_args,
    schedule_interval="00 8 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-trail-counter-data", "socrata"],
    catchup=False,
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    start = "{{ (prev_start_date_success - macros.timedelta(days=7)).strftime('%Y-%m-%d') if prev_start_date_success else '1970-01-01'}}"
    t1 = DockerOperator(
        task_id="trail_counter_data_publish",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"python counter_data.py --start {start}",
        environment=env_vars,
        tty=True,
        force_pull=True,
    )

    t1
