# test locally with: docker compose run --rm airflow-cli dags test atd_executive_dashboard_row_active_permits_logging

import os

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration, now

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert
from utils.knack import get_date_filter_arg

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
    # Socrata
    "SO_KEY": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SO_SECRET": {
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
    "WEEK_DATASET": {
        "opitem": "atd-executive-dashboard",
        "opfield": "production.Weekly Dataset ID",
    },
    "ACTIVE_DATASET": {
        "opitem": "atd-executive-dashboard",
        "opfield": "production.Active Permits Dataset ID",
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
    # S3
    "BUCKET_NAME": {
        "opitem": "atd-executive-dashboard",
        "opfield": "production.Bucket",
    },
    "EXEC_DASH_PASS": {
        "opitem": "atd-executive-dashboard",
        "opfield": "production.AWS Secret Access Key",
    },
    "EXEC_DASH_ACCESS_ID": {
        "opitem": "atd-executive-dashboard",
        "opfield": "production.AWS Access ID",
    },
    # Smartsheet
    "SMARTSHEET_ACCESS_TOKEN": {
        "opitem": "atd-executive-dashboard",
        "opfield": "production.Smartsheet API Key",
    },
}

SECRETS_SOCRATA_BACKUP = {
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

@task
def get_dataset_id(env_vars):
    return env_vars['ACTIVE_DATASET']

with DAG(
    dag_id="atd_executive_dashboard_row_active_permits_logging",
    description="Stores the current number of active ROW permits in a Socrata dataset.",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 8 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-executive-dashboard", "amanda", "socrata"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-executive-dashboard:production"
    docker_image_2 = "atddocker/atd-knack-services:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)
    env_vars_2 = get_env_vars_task(SECRETS_SOCRATA_BACKUP)
    dataset_id = get_dataset_id(env_vars)

    t1 = DockerOperator(
        task_id="active_permits_logging",
        image=docker_image,
        auto_remove=True,
        command=f"python active_permits_logging.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
    )

    t2 = DockerOperator(
        task_id="backup_active_permits",
        image=docker_image_2,
        auto_remove=True,
        command=f"./atd-knack-services/services/backup_socrata.py --dataset {dataset_id}",
        environment=env_vars_2,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )


    t1 >> t2
