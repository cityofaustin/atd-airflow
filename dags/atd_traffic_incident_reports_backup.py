# test locally with: docker compose run --rm airflow-cli dags test atd_traffic_incident_reports_backup

import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration

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
    "execution_timeout": duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "SOCRATA_API_KEY_ID": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SOCRATA_API_KEY_SECRET": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SOCRATA_APP_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
    "AWS_ACCESS_ID": {
        "opitem": "Socrata Dataset Backups S3 Bucket",
        "opfield": "production.AWS Access Key",
    },
    "AWS_SECRET_ACCESS_KEY": {
        "opitem": "Socrata Dataset Backups S3 Bucket",
        "opfield": "production.AWS Secret Access Key",
    },
    "BUCKET": {
        "opitem": "Socrata Dataset Backups S3 Bucket",
        "opfield": "production.Bucket",
    },
}


with DAG(
    dag_id="atd_traffic_incident_reports_backup",
    description="Daily backup of the traffic incidents dataset from Socrata to S3.",
    default_args=DEFAULT_ARGS,
    schedule_interval="05 00 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "socrata"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-knack-services:production"
    dataset = "dx9v-zd7x"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="atd_knack_inventory_items_nightly_snapshot_socrata_backup",
        image=docker_image,
        auto_remove=True,
        command=f"./atd-knack-services/services/backup_socrata.py --dataset {dataset}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    t1
