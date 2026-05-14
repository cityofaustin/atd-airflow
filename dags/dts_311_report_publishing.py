from os import getenv

from datetime import timedelta

from airflow.sdk import task, DAG
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## 311 Report Publishing ETL

This DAG downloads a bunch of csv reports from an endpoint set up by Austin 311. 
We process these reports then publish them to socrata datasets.

## Troubleshooting

You must be on the city VPN in order to run these scripts. You will see this error if you aren't on the city network:

> "Unexpected file type returned from the report endpoint. Check that you are on the city network. 
> It's likely that your request is getting flagged as a bot by the web app firewall."

It is not critical these scripts are running flawlessly. We will get errors from time to time that are out of DTS's control.

These datasets are the back end of 311 dashboard in Power BI along with biweekly emails that are emailed to department leadership. 
So, long term outages should be investigated with help from 311.

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
    "REQUESTS_DATASET": {
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
    "REQUESTS_ENDPOINT": {
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
    "REQUESTS_ENDPOINT": {
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
    "REQUESTS_ENDPOINT": {
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
    dag_id="dts_311_report_publishing",
    description="Downloads reports of 311 service requests for TPW and publishes it in a Socrata dataset.",
    doc_md=doc_md,
    default_args=default_args,
    schedule=("36 2,13 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None),
    dagrun_timeout=timedelta(minutes=60),
    tags=["repo:dts-311-reporting", "socrata", "311"],
    catchup=False,
) as dag:
    docker_image = "atddocker/dts-311-reporting:production"

    cur_year_env = get_env_vars_task(CUR_YEAR_SECRETS)
    prev_year_env = get_env_vars_task(PREV_YEAR_SECRETS)
    two_years_env = get_env_vars_task(TWO_YEARS_AGO_SECRETS)

    t1 = DockerOperator(
        task_id="cur_year_requests_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command="python -m etl.csv_reporting.requests_to_socrata",
        environment=cur_year_env,
        tty=True,
        force_pull=True,
    )

    t2 = DockerOperator(
        task_id="cur_year_flex_note_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command="python -m etl.csv_reporting.flex_notes_to_socrata",
        environment=cur_year_env,
        tty=True,
    )

    t3 = DockerOperator(
        task_id="cur_year_activities_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command="python -m etl.csv_reporting.activities_to_socrata",
        environment=cur_year_env,
        tty=True,
    )

    t4 = DockerOperator(
        task_id="prev_year_requests_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command="python -m etl.csv_reporting.requests_to_socrata",
        environment=prev_year_env,
        tty=True,
    )

    t5 = DockerOperator(
        task_id="prev_year_flex_note_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command="python -m etl.csv_reporting.flex_notes_to_socrata",
        environment=prev_year_env,
        tty=True,
    )

    t6 = DockerOperator(
        task_id="prev_year_activities_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command="python -m etl.csv_reporting.activities_to_socrata",
        environment=prev_year_env,
        tty=True,
    )

    t7 = DockerOperator(
        task_id="two_years_ago_requests_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command="python -m etl.csv_reporting.requests_to_socrata",
        environment=two_years_env,
        tty=True,
    )

    t8 = DockerOperator(
        task_id="two_years_ago_flex_note_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command="python -m etl.csv_reporting.flex_notes_to_socrata",
        environment=two_years_env,
        tty=True,
    )

    t9 = DockerOperator(
        task_id="two_years_ago_activities_report_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command="python -m etl.csv_reporting.activities_to_socrata",
        environment=two_years_env,
        tty=True,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9
