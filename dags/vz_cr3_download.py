import os

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
from pendulum import datetime, duration, now, parse

from utils.onepassword import get_env_vars_task, get_item_last_update_date
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "description": "Downloads CR3 pdfs from CRIS and uploads to S3",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
}

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
    "AWS_CRIS_CR3_DOWNLOAD_PATH": {
        "opitem": "CRIS CR3 Download",
        "opfield": "production.AWS_CRIS_CR3_DOWNLOAD_PATH",
    },
    "AWS_CRIS_CR3_BUCKET_NAME": {
        "opitem": "CRIS CR3 Download",
        "opfield": "production.AWS_CRIS_CR3_BUCKET_NAME",
    },
    "CRIS_CR3_DOWNLOAD_COOKIE": {
        "opitem": "CRIS CR3 Download",
        "opfield": "production.CRIS_CR3_DOWNLOAD_COOKIE",
    },
}


with DAG(
    dag_id=f"vz-cr3-download",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=duration(minutes=5),
    tags=["repo:atd-vz-data", "cris", "s3", "cr3"],
    catchup=False,
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    updated_at_dict = get_item_last_update_date("CRIS CR3 Download")

    branch_true = DockerOperator(
        task_id="vz_cr3_download",
        image="atddocker/atd-vz-etl:development",
        api_version="auto",
        auto_remove=True,
        command="python process_cris_cr3.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    branch_false = DummyOperator(task_id="skip_vz_cr3_download")

    @task.branch(task_id="choose_branch")
    def choose_branch(updated_at):
        minutes_ago_utc = now() - duration(minutes=5)
        updated_at_utc = updated_at

        if updated_at_utc > minutes_ago_utc:
            print("Downloading CR3 pdfs")
            print(env_vars)
            return ["vz_cr3_download"]
        else:
            print("Cookie entry not updated - skipping CR3 pdf downloads")
            return ["skip_vz_cr3_download"]

    choose_branch = choose_branch(updated_at=updated_at_dict["updated_at"])

    choose_branch >> [branch_true, branch_false]
