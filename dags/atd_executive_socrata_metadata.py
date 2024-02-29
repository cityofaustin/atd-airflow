# test locally with: docker compose run --rm airflow-cli dags test atd_executive_socrata_metadata

import os

from datetime import timedelta

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-executive-dashboard:production"

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
}


with DAG(
    dag_id="atd_executive_socrata_metadata",
    description="Gathers metadata from all TPW assets on Socrata and stores it in a dataset.",
    default_args=default_args,
    schedule_interval="00 4 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=timedelta(minutes=60),
    tags=["repo:atd-executive-dashboard", "socrata"],
    catchup=False,
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="socrata_metadata_pub",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove=True,
        command="python socrata-metadata/socrata_metadata_pub.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
    )

    t1
