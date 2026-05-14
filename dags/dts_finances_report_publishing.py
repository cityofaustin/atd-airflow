from os import getenv

from datetime import timedelta

from airflow.sdk import task, DAG
from utils.docker_operator import DockerOperatorWithFallback
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Finances Report Publishing

This DAG runs a microstrategy report for department finances and places the results in a socrata dataset.

## Troubleshooting

VPN access should not be required for running this DAG locally.

This DAG supplies data for the TPW Finances dashboard in Power BI.

It is not critical this is run daily, but long term or repeated issues should be investigated.

Contact ATS for issues with microstrategy or the microstrategy users group in Teams.

"""
DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

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
    doc_md=doc_md,
    default_args=default_args,
    schedule="00 11 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=timedelta(minutes=120),
    tags=["repo:dts-finance-reporting", "socrata", "microstrategy"],
    catchup=False,
) as dag:
    docker_image = "atddocker/dts-finance-reporting:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    download_microstrategy_reports = DockerOperatorWithFallback(
        force_pull=True,
        task_id="download_microstrategy_reports",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command=f"python etl/rev_exp_report_to_s3.py",
        environment=env_vars,
        tty=True,
    )

    update_socrata = DockerOperatorWithFallback(
        task_id="update_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command=f"python etl/mstro_reports_to_socrata.py",
        environment=env_vars,
        tty=True,
    )

    download_microstrategy_reports >> update_socrata
