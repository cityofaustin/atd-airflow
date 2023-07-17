import os

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration

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
}

with DAG(
    dag_id=f"vz-cr3-download",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=duration(minutes=5),
    tags=["repo:atd-vz-data", "cris", "s3", "cr3"],
    catchup=False,
) as dag:

    @task(
        task_id="get_env_vars",
        execution_timeout=duration(seconds=30),
    )
    def get_env_vars():
        from utils.onepassword import load_dict

        return load_dict(REQUIRED_SECRETS)

    env_vars = get_env_vars()

    print(env_vars)

    # t1 = DockerOperator(
    #     task_id="vz_cr3_download",
    #     # image=docker_image,
    #     image="vz_etl",
    #     api_version="auto",
    #     auto_remove=True,
    #     command="",
    #     environment=env_vars,
    #     tty=True,
    #     force_pull=True,
    #     mount_tmp_dir=False,
    # )

    # t1
