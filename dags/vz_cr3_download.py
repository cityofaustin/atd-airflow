import os

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
from pendulum import datetime, duration, now, parse

from utils.onepassword import get_env_vars_task, get_item_last_update_date
from utils.slack_operator import task_fail_slack_alert, task_success_slack_alert
from utils.time import get_previous_run_date

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
}

COOKIE_1PW_ENTRY = "CRIS CR3 Download Cookie"

REQUIRED_SECRETS = {
    "HASURA_ENDPOINT": {
        "opitem": "Vision Zero CRIS Import",
        "opfield": "production.GraphQL Endpoint",
    },
    "HASURA_ADMIN_KEY": {
        "opitem": "Vision Zero CRIS Import",
        "opfield": "production.GraphQL Endpoint key",
    },
    "AWS_ACCESS_KEY_ID": {
        "opitem": "CR3 Download IAM Access Key and Secret",
        "opfield": "production.accessKeyId",
    },
    "AWS_SECRET_ACCESS_KEY": {
        "opitem": "CR3 Download IAM Access Key and Secret",
        "opfield": "production.accessSecret",
    },
    "AWS_DEFAULT_REGION": {
        "opitem": "CR3 Download IAM Access Key and Secret",
        "opfield": "production.awsDefaultRegion",
    },
    "ATD_CRIS_CR3_URL": {
        "opitem": "CRIS CR3 Download",
        "opfield": "production.ATD_CRIS_CR3_URL",
    },
    "AWS_CRIS_CR3_BUCKET_NAME": {
        "opitem": "CRIS CR3 Download",
        "opfield": "production.AWS_CRIS_CR3_BUCKET_NAME",
    },
    "CRIS_CR3_DOWNLOAD_COOKIE": {
        "opitem": COOKIE_1PW_ENTRY,
        "opfield": "production.COOKIE",
    },
}


with DAG(
    dag_id="vz-cr3-download",
    description="Downloads CR3 pdfs from CRIS and uploads to S3",
    default_args=default_args,
    schedule_interval="*/5 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=duration(minutes=5),
    tags=["repo:atd-vz-data", "cris", "s3", "cr3"],
    catchup=False,
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    last_run = get_previous_run_date()
    updated_at = get_item_last_update_date(COOKIE_1PW_ENTRY)

    branch_true = DockerOperator(
        task_id="vz_cr3_download",
        image=f"atddocker/vz-cr3-download:{DEPLOYMENT_ENVIRONMENT}",
        api_version="auto",
        auto_remove=True,
        command="python cr3_download.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        on_success_callback=task_success_slack_alert,
    )

    branch_false = DummyOperator(task_id="skip_vz_cr3_download")

    @task.branch(task_id="choose_branch")
    def choose_branch(updated_at, last_run):
        if updated_at > last_run:
            print("Downloading CR3 pdfs")
            return ["vz_cr3_download"]
        else:
            print("Cookie entry not updated - skipping CR3 pdf downloads")
            return ["skip_vz_cr3_download"]

    choose_branch = choose_branch(
        updated_at=updated_at["updated_at_datetime"],
        last_run=last_run["last_run_datetime"],
    )

    choose_branch >> [branch_true, branch_false]
