# test locally with: docker compose run --rm airflow-cli dags test dts_inspector_priority

from os import getenv

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration

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
    "PRIORITY_DATASET": {
        "opitem": "dts-right-of-way-reporting",
        "opfield": "datasets.Priority",
    },
    "SEGMENT_DATASET": {
        "opitem": "dts-right-of-way-reporting",
        "opfield": "datasets.Segments",
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
    # ArcGIS Online
    "AGOL_USERNAME": {
        "opitem": "AGOL Scripts Publisher",
        "opfield": "production.Username",
    },
    "AGOL_PASSWORD": {
        "opitem": "AGOL Scripts Publisher",
        "opfield": "production.Password",
    },
}

with DAG(
    dag_id="dts_inspector_priority",
    description="Downloads permits and road segment data from AMANDA and scores permits based on several metrics",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 3 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:dts-right-of-way-reporting", "amanda", "socrata", "permits"],
    catchup=False,
) as dag:
    docker_image = "atddocker/dts-right-of-way-reporting:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="amanda_row_inspector_permit_list",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python amanda/amanda_to_s3.py --query row_inspector_permit_list",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
        trigger_rule="all_done",
    )

    t2 = DockerOperator(
        task_id="amanda_row_inspector_segment_list",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python amanda/amanda_to_s3.py --query row_inspector_segment_list",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
        trigger_rule="all_done",
    )

    t3 = DockerOperator(
        task_id="agol_street_segment_tagging",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python metrics/roadway_segment_tagging.py",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        trigger_rule="all_done",
    )

    t4 = DockerOperator(
        task_id="inspector_prioritization",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python metrics/inspector_prioritization.py",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        trigger_rule="all_done",
    )

    t1 >> t2 >> t3 >> t4
