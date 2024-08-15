# test locally with: docker compose run --rm airflow-cli dags test dts_csr_report_publishing

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

OTHER_SECRETS = {
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
    "FLEX_NOTES_DATASET": {
        "opitem": "Executive Dashboard",
        "opfield": "datasets.Flex Notes",
    },
    "ACTIVITIES_DATASET": {
        "opitem": "Executive Dashboard",
        "opfield": "datasets.Activities",
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

CUR_YEAR_SECRETS = {
    "CSR_ENDPOINT": {
        "opitem": "Executive Dashboard",
        "opfield": "csr.Current FY Endpoint",
    },
    "FLEX_NOTE_ENDPOINT": {
        "opitem": "Executive Dashboard",
        "opfield": "flex_notes.Current FY Endpoint",
    },
    "ACTIVITIES_ENDPOINT": {
        "opitem": "Executive Dashboard",
        "opfield": "activities.Current FY Endpoint",
    },
}

PREV_YEAR_SECRETS = {
    "CSR_ENDPOINT": {
        "opitem": "Executive Dashboard",
        "opfield": "csr.Previous FY Endpoint",
    },
    "FLEX_NOTE_ENDPOINT": {
        "opitem": "Executive Dashboard",
        "opfield": "flex_notes.Previous FY Endpoint",
    },
    "ACTIVITIES_ENDPOINT": {
        "opitem": "Executive Dashboard",
        "opfield": "activities.Previous FY Endpoint",
    },
}

TWO_YEARS_AGO_SECRETS = {
    "CSR_ENDPOINT": {
        "opitem": "Executive Dashboard",
        "opfield": "csr.Two Years Ago FY Endpoint",
    },
    "FLEX_NOTE_ENDPOINT": {
        "opitem": "Executive Dashboard",
        "opfield": "flex_notes.Two Years Ago FY Endpoint",
    },
    "ACTIVITIES_ENDPOINT": {
        "opitem": "Executive Dashboard",
        "opfield": "activities.Two Years Ago FY Endpoint",
    },
}

# Combine env vars to create one for each report
CUR_YEAR_SECRETS.update(OTHER_SECRETS)
PREV_YEAR_SECRETS.update(OTHER_SECRETS)
TWO_YEARS_AGO_SECRETS.update(OTHER_SECRETS)

with DAG(
    dag_id="dts_csr_report_publishing",
    description="Downloads reports of 311 service requests for TPW and publishes it in a Socrata dataset.",
    default_args=default_args,
    schedule_interval="36 9 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=timedelta(minutes=60),
    tags=["repo:dts-311-reporting", "socrata", "csr"],
    catchup=False,
) as dag:
    docker_image = "atddocker/dts-311-reporting:production"

    cur_year_env = get_env_vars_task(CUR_YEAR_SECRETS)
    prev_year_env = get_env_vars_task(PREV_YEAR_SECRETS)
    two_years_env = get_env_vars_task(TWO_YEARS_AGO_SECRETS)

    t1 = DockerOperator(
        task_id="cur_year_csr_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove=True,
        command=f"python etl/csr_to_socrata.py",
        environment=cur_year_env,
        tty=True,
        force_pull=True,
    )

    t2 = DockerOperator(
        task_id="cur_year_flex_note_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove=True,
        command=f"python etl/flex_notes_to_socrata.py",
        environment=cur_year_env,
        tty=True,
    )

    t3 = DockerOperator(
        task_id="cur_year_activities_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove=True,
        command=f"python etl/activities_to_socrata.py",
        environment=cur_year_env,
        tty=True,
    )

    t4 = DockerOperator(
        task_id="prev_year_csr_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove=True,
        command=f"python etl/csr_to_socrata.py",
        environment=prev_year_env,
        tty=True,
    )

    t5 = DockerOperator(
        task_id="prev_year_flex_note_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove=True,
        command=f"python etl/flex_notes_to_socrata.py",
        environment=prev_year_env,
        tty=True,
    )

    t6 = DockerOperator(
        task_id="prev_year_activities_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove=True,
        command=f"python etl/activities_to_socrata.py",
        environment=prev_year_env,
        tty=True,
    )

    t7 = DockerOperator(
        task_id="two_years_ago_csr_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove=True,
        command=f"python etl/csr_to_socrata.py",
        environment=two_years_env,
        tty=True,
    )

    t8 = DockerOperator(
        task_id="two_years_ago_flex_note_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove=True,
        command=f"python etl/flex_notes_to_socrata.py",
        environment=two_years_env,
        tty=True,
    )

    t9 = DockerOperator(
        task_id="two_years_ago_activities_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove=True,
        command=f"python etl/activities_to_socrata.py",
        environment=two_years_env,
        tty=True,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9
