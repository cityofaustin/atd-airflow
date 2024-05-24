# test locally with: docker compose run --rm airflow-cli dags test atd_executive_dashboard_row_weekly_summary

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
    "execution_timeout": duration(minutes=30),
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
    dag_id="atd_executive_dashboard_row_weekly_summary",
    description="Downloads ROW data from AMANDA and Smartsheet and publishes the weekly summary results in a Socrata Dataset.",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-executive-dashboard", "amanda", "socrata", "smartsheet"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-executive-dashboard:production"
    knack_services_image = "atddocker/atd-knack-services:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)
    env_vars_knack_services = get_env_vars_task(SECRETS_SOCRATA_BACKUP)

    # pulling out dataset identifier for command arg
    dataset_id = get_dataset_id(env_vars)

    t1 = DockerOperator(
        task_id="amanda_applications_received",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"python AMANDA/amanda_to_s3.py --query applications_received",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
        trigger_rule="all_done",
    )

    t2 = DockerOperator(
        task_id="amanda_active_permits",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"python AMANDA/amanda_to_s3.py --query active_permits",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
        trigger_rule="all_done",
    )

    t3 = DockerOperator(
        task_id="amanda_issued_permits",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"python AMANDA/amanda_to_s3.py --query issued_permits",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
        trigger_rule="all_done",
    )

    t4 = DockerOperator(
        task_id="smartsheet_to_s3",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"python smartsheet/smartsheet_to_s3.py",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
        trigger_rule="all_done",
    )

    t5 = DockerOperator(
        task_id="row_data_summary",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"python row_data_summary.py",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
        trigger_rule="all_done",
    )

    t6 = DockerOperator(
        task_id="amanda_review_time",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"python AMANDA/amanda_to_s3.py --query review_time",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
        trigger_rule="all_done",
    )

    t7 = DockerOperator(
        task_id="ex_permits_issued",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"python AMANDA/amanda_to_s3.py --query ex_permits_issued",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
        trigger_rule="all_done",
    )

    t8 = DockerOperator(
        task_id="active_permits_logging",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"python active_permits_logging.py",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
        trigger_rule="all_done",
    )

    t9 = DockerOperator(
        task_id="backup_active_permits",
        image=knack_services_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"./atd-knack-services/services/backup_socrata.py --dataset {dataset_id}",
        environment=env_vars_knack_services,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        trigger_rule="all_done",
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9
