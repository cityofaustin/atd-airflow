# test locally with: docker compose run --rm airflow-cli dags test dts_row_reporting

from os import getenv

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.helpers import chain
from pendulum import datetime, duration, now

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

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
    return env_vars["ACTIVE_DATASET"]


def knack_services_task_template(task_id, image, command, env_vars, pull=False):
    return DockerOperator(
        task_id=task_id,
        image=image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=command,
        environment=env_vars,
        tty=True,
        force_pull=pull,
        mount_tmp_dir=False,
        trigger_rule="all_done",
        retries=3,
        retry_delay=duration(seconds=60),
    )


with DAG(
    dag_id="dts_row_reporting",
    description="Downloads ROW data from AMANDA and Smartsheet and publishes the weekly summary results in a Socrata Dataset.",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:dts-right-of-way-reporting", "amanda", "socrata", "smartsheet"],
    catchup=False,
) as dag:
    docker_image = "atddocker/dts-right-of-way-reporting:production"
    knack_services_image = "atddocker/atd-knack-services:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)
    env_vars_knack_services = get_env_vars_task(SECRETS_SOCRATA_BACKUP)

    # pulling out dataset identifier for command arg
    dataset_id = get_dataset_id(env_vars)

    commands = [
        {
            "task_id": "amanda_applications_received",
            "command": "python amanda/amanda_to_s3.py --query applications_received",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "amanda_active_permits",
            "command": "python amanda/amanda_to_s3.py --query active_permits",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "amanda_issued_permits",
            "command": "python amanda/amanda_to_s3.py --query issued_permits",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "amanda_license_agreements_timeline",
            "command": "python amanda/amanda_to_s3.py --query license_agreements_timeline",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "smartsheet_to_s3",
            "command": "python smartsheet/smartsheet_to_s3.py",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "row_data_summary",
            "command": "python metrics/row_data_summary.py",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "amanda_review_time",
            "command": "python amanda/amanda_to_s3.py --query review_time",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "ex_permits_issued",
            "command": "python amanda/amanda_to_s3.py --query ex_permits_issued",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "active_permits_logging",
            "command": "python metrics/active_permits_logging.py",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "backup_active_permits",
            "command": f"./atd-knack-services/services/backup_socrata.py --dataset {dataset_id}",
            "image": knack_services_image,
            "env": env_vars_knack_services,
        },
        {
            "task_id": "license_agreements_socrata",
            "command": "python metrics/s3_to_socrata.py --dataset license_agreements_timeline",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "lde_site_plan_revisions_s3",
            "command": "python amanda/amanda_to_s3.py --query lde_site_plan_revisions",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "lde_site_plan_revisions_socrata",
            "command": "python metrics/s3_to_socrata.py --dataset lde_site_plan_revisions",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "tds_cases_s3",
            "command": "python amanda/amanda_to_s3.py --query tds_cases",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "tds_cases_socrata",
            "command": "python metrics/s3_to_socrata.py --dataset tds_cases",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "tds_sif_map_s3",
            "command": "python amanda/amanda_to_s3.py --query tds_asmd_map",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "tds_sif_map_socrata",
            "command": "python metrics/s3_to_socrata.py --dataset tds_asmd_map",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "sif_payment_details_s3",
            "command": "python amanda/amanda_to_s3.py --query sif_payment_details",
            "image": docker_image,
            "env": env_vars,
        },
        {
            "task_id": "sif_payment_details_socrata",
            "command": "python metrics/s3_to_socrata.py --dataset sif_payment_details",
            "image": docker_image,
            "env": env_vars,
        },
    ]

    tasks = []

    for cmd in commands:
        # We want the first task to pull the latest docker image
        if len(tasks) == 0:
            pull = True
        else:
            pull = False
        tasks.append(
            knack_services_task_template(
                task_id=cmd["task_id"],
                image=cmd["image"],
                command=cmd["command"],
                env_vars=cmd["env"],
                pull=pull,
            )
        )

    chain(*tasks)
