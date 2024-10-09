# test locally with: docker compose run --rm airflow-cli dags test dts_work_zone_data_feed

import os

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration, now

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=10),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/dts-work-zone-data-feed:production"

REQUIRED_SECRETS = {
    # Socrata
    "SO_USER": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SO_PASS": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SO_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
    "SO_WEB": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.endpoint",
    },
    "FEED_DATASET": {
        "opitem": "Work Zone Data Feed",
        "opfield": "production.feed dataset ID",
    },
    "FLAT_DATASET": {
        "opitem": "Work Zone Data Feed",
        "opfield": "production.flat dataset ID",
    },
    # AMANDA
    "HOST": {
        "opitem": "Amanda Read-Only (RO) replica database",
        "opfield": "production.host",
    },
    "PORT": {
        "opitem": "Amanda Read-Only (RO) replica database",
        "opfield": "production.port",
    },
    "SERVICE_NAME": {
        "opitem": "Amanda Read-Only (RO) replica database",
        "opfield": "production.service",
    },
    "DB_USER": {
        "opitem": "Amanda Read-Only (RO) replica database",
        "opfield": "production.username",
    },
    "DB_PASS": {
        "opitem": "Amanda Read-Only (RO) replica database",
        "opfield": "production.password",
    },
    # Contact Email
    "CONTACT_EMAIL": {
        "opitem": "Work Zone Data Feed",
        "opfield": "production.contact email",
    },
}

with DAG(
    dag_id="dts_work_zone_data_feed",
    description="Publishing AMANDA work zone data to Socrata.",
    default_args=DEFAULT_ARGS,
    schedule_interval="30 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:dts-work-zone-data-feed", "amanda", "socrata", "work zone", "wzdx"],
    catchup=False,
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="amanda_applications_received",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"python data_sources/amanda_closure_publishing.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
    )

    t1 
