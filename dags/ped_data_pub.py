# test locally with: docker compose run --rm airflow-cli dags test atd_trail_counters

import os
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from utils.slack_operator import task_fail_slack_alert
from utils.onepassword import load_dict

from onepasswordconnectsdk.client import Client, new_client
import onepasswordconnectsdk

default_args = {
    "owner": "airflow",
    "description": "Scrapes trail counter data from the public eco-counter website and publishes it in Socrata",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-trail-counters:latest"

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
}

env_vars = load_dict(REQUIRED_SECRETS)
env_vars["COUNTERS_DATASET"] = "26tt-cp67"

with DAG(
    dag_id="atd_trail_counters",
    default_args=default_args,
    schedule_interval="00 8 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["repo:atd-trail-counter-data", "Socrata"],
    catchup=False,
) as dag:
    t1 = DockerOperator(
        task_id="trail_counter_data_publish",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="python counter_data.py",
        environment=env_vars,
        tty=True,
    )

    t1
