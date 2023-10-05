# test locally with: docker compose run --rm airflow-cli dags test atd_executive_csr_data

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
    "EXP_DATASET": {
        "opitem": "Executive Dashboard",
        "opfield": "datasets.Expenses",
    },
    "REV_DATASET": {
        "opitem": "Executive Dashboard",
        "opfield": "datasets.Revenue",
    },
    "CSR_DATASET": {
        "opitem": "Executive Dashboard",
        "opfield": "datasets.CSR",
    },
    "BUCKET_NAME": {
        "opitem": "Executive Dashboard",
        "opfield": "s3.Bucket",
    },
    "AWS_ACCESS_KEY": {
        "opitem": "Executive Dashboard",
        "opfield": "s3.AWS Access Key",
    },
    "AWS_SECRET_ACCESS_KEY": {
        "opitem": "Executive Dashboard",
        "opfield": "s3.AWS Secret Access Key",
    },
    "CSR_ENDPOINT": {
        "opitem": "Executive Dashboard",
        "opfield": "csr.Report Endpoint",
    },
    "BASE_URL": {
        "opitem": "Microstrategy API",
        "opfield": "shared.Base URL",
    },
    "PROJECT_ID": {
        "opitem": "Microstrategy API",
        "opfield": "shared.Project ID",
    },
    "MSTRO_USERNAME": {
        "opitem": "Microstrategy API",
        "opfield": "shared.Username",
    },
    "MSTRO_PASSWORD": {
        "opitem": "Microstrategy API",
        "opfield": "shared.Password",
    },
}

with DAG(
    dag_id="atd_executive_csr_data",
    description="Downloads a report of 311 service requests for TPW and publishes it in a Socrata dataset.",
    default_args=default_args,
    schedule_interval="36 9 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=timedelta(minutes=20),
    tags=["repo:atd-executive-dashboard", "socrata", "csr"],
    catchup=False,
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="csr_report_to_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"python csr/csr_to_socrata.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
    )

    t1
