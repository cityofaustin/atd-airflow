# Test locally with: docker compose run --rm airflow-cli dags test atd_metrobike_trips

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
    "depend_on_past": False,
    "start_date": datetime(2020, 12, 31, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": task_fail_slack_alert,
}

# assemble env vars

REQUIRED_SECRETS = {
    "SOCRATA_API_KEY_SECRET": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SOCRATA_API_KEY_ID": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SOCRATA_APP_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
    "METROBIKE_DROPBOX_TOKEN": {
        "opitem": "Metrobike",
        "opfield": "production.Dropbox Token",
    },
}

# runs weekly in prod environment at 1:33am Monday
with DAG(
    dag_id="atd_metrobike_trips",
    description="Fetch metrobike trip data from dropbox and publish to Socrata",
    default_args=default_args,
    schedule_interval="33 1 * * 1" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-metrobike", "metrobike", "socrata"],
    catchup=False,
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="atd_metrobike_trips_socrata",
        image="atddocker/atd-metrobike:production",
        docker_conn_id="docker_default",
        auto_remove=True,
        command="python publish_trips.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
    )

    t1
