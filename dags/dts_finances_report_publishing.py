from os import getenv

from datetime import timedelta

from airflow.sdk import task, DAG, Param
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Finances Report Publishing

This DAG runs microstrategy reports for department finances and places the results in a socrata dataset.

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
    "SOCRATA_ENDPOINT": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.endpoint",
    },
    "SOCRATA_APP_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
    "SOCRATA_KEY_SECRET": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SOCRATA_KEY_ID": {
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
    description="Downloads Microstrategy Reports and publishes them on Socrata for TPW finances reporting.",
    doc_md=doc_md,
    default_args=default_args,
    schedule="00 11 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=timedelta(minutes=10),
    tags=["repo:dts-finance-reporting", "socrata", "microstrategy"],
    catchup=False,
    params={
        "docker_image_tag": Param(
            "production",
            type="string",
            title="Docker image tag",
            description="Tag of atddocker/dts-finance-reporting to run. Use 'local' for a locally built image.",
        ),
    },
) as dag:
    docker_image = "atddocker/dts-finance-reporting:{{ params.docker_image_tag }}"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="download_expenses_revenue_reports",
        image=docker_image,
        api_version="auto",
        auto_remove="force",
        command="python etl/revenue_expenses/rev_exp_report_to_s3.py",
        environment=env_vars,
        tty=True,
    )

    t2 = DockerOperator(
        task_id="update_expenses_revenue_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove="force",
        command="python etl/revenue_expenses/rev_exp_reports_to_socrata.py",
        environment=env_vars,
        tty=True,
    )

    t3 = DockerOperator(
        task_id="sif_spent_to_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove="force",
        command="python etl/microstrategy_to_socrata.py --report sif_spent",
        environment=env_vars,
        tty=True,
    )

    t1 >> t2 >> t3
