from os import getenv

from airflow.sdk import DAG
from utils.docker_operator import DockerOperatorWithFallback
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Inspector Priority DAG

This DAG runs a script which scores active permits in AMANDA for inspectors to prioritize their work.

This is visualized in a Power BI dashboard for the inspectors.

## Troubleshooting

You will need to be on city VPN in order to run this DAG locally.

An outage of this DAG should be investigated and if a fix is not found, make the AMANDA team aware of the outage.

Re-triggering this DAG should be the first step in troubleshooting.

AMANDA connection issues should be investigated with the AMANDA team.

"""

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
        "opitem": "ArcGIS Online (AGOL) Scripts Publisher",
        "opfield": "production.username",
    },
    "AGOL_PASSWORD": {
        "opitem": "ArcGIS Online (AGOL) Scripts Publisher",
        "opfield": "production.password",
    },
}

with DAG(
    dag_id="dts_inspector_priority",
    description="Downloads permits and road segment data from AMANDA and scores permits based on several metrics",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="0 3 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:dts-right-of-way-reporting", "amanda", "socrata", "permits"],
    catchup=False,
) as dag:
    docker_image = "atddocker/dts-right-of-way-reporting:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    amanda_row_inspector_permit_list = DockerOperatorWithFallback(
        force_pull=True,
        task_id="amanda_row_inspector_permit_list",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python amanda/amanda_to_s3.py --query row_inspector_permit_list",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
        trigger_rule="all_done",
    )

    amanda_row_inspector_segment_list = DockerOperatorWithFallback(
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

    agol_street_segment_tagging = DockerOperatorWithFallback(
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

    inspector_prioritization = DockerOperatorWithFallback(
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

    (
        amanda_row_inspector_permit_list
        >> amanda_row_inspector_segment_list
        >> agol_street_segment_tagging
        >> inspector_prioritization
    )
