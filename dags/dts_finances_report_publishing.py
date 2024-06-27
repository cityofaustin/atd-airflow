# test locally with: docker compose run --rm airflow-cli dags test dts_finances_report_publishing

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
    dag_id="dts_finances_report_publishing",
    description="Downloads two Microstrategy Reports for Expenses and Revenue. \
    Places the results as a CSV in a S3 bucket. \
    Then publishes the data to a Socrata dataset",
    default_args=default_args,
    schedule_interval="00 11 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=timedelta(minutes=120),
    tags=["repo:dts-finance-reporting", "socrata", "microstrategy"],
    catchup=False,
) as dag:
    docker_image = "atddocker/dts-finance-reporting:production"
    
    env_vars = get_env_vars_task(REQUIRED_SECRETS)


    t1 = DockerOperator(
        task_id="download_microstrategy_reports",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"python etl/rev_exp_report_to_s3.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
    )

    t2 = DockerOperator(
        task_id="update_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"python etl/mstro_reports_to_socrata.py",
        environment=env_vars,
        tty=True,
        force_pull=False,
    )

    t1 >> t2
