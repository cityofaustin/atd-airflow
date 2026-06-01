from os import getenv

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag
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
    "execution_timeout": duration(minutes=10),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "KNACK_APP_ID": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"production.apiKey",
    },
    "SOCRATA_API_KEY_ID": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SOCRATA_API_KEY_SECRET": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SOCRATA_APP_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
    "PGREST_ENDPOINT": {
        "opitem": "atd-knack-services PostgREST",
        "opfield": "production.endpoint",
    },
    "PGREST_JWT": {
        "opitem": "atd-knack-services PostgREST",
        "opfield": "production.jwt",
    },
    "AWS_ACCESS_ID": {
        "opitem": "Socrata Dataset Backups S3 Bucket",
        "opfield": "production.AWS Access Key",
    },
    "AWS_SECRET_ACCESS_KEY": {
        "opitem": "Socrata Dataset Backups S3 Bucket",
        "opfield": "production.AWS Secret Access Key",
    },
    "BUCKET": {
        "opitem": "Socrata Dataset Backups S3 Bucket",
        "opfield": "production.Bucket",
    },
}


DAG_DOC_MD = """
### atd_knack_inventory_items_nightly_snapshot
Appends inventory item counts from Data Tracker to Socrata and then backs up the dataset.

This DAG intentionally uses a fixed date filter of 1970-01-01 so each run appends the full view contents.
"""


@dag(
    dag_id="atd_knack_inventory_items_nightly_snapshot",
    description="Appends inventory item counts to running log in Socrata",
    default_args=DEFAULT_ARGS,
    schedule="13 23 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "socrata"],
    catchup=False,
    doc_md=DAG_DOC_MD,
)
def atd_knack_inventory_items_nightly_snapshot():
    docker_image = "atddocker/atd-knack-services:production"
    app_name = "data-tracker"
    container = "view_2863"

    # Always append complete view contents for nightly snapshots.
    date_filter_arg = "-d 1970-01-01"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    load_inventory_items_to_postgrest_task = DockerOperator(
        task_id="atd_knack_inventory_items_nightly_snapshot_to_postgrest",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_postgrest.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    load_inventory_items_to_socrata_task = DockerOperator(
        task_id="atd_knack_inventory_items_nightly_snapshot_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_socrata.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    backup_inventory_items_socrata_task = DockerOperator(
        task_id="atd_knack_inventory_items_nightly_snapshot_socrata_backup",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/backup_socrata.py -a {app_name} -c {container}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    (
        load_inventory_items_to_postgrest_task
        >> load_inventory_items_to_socrata_task
        >> backup_inventory_items_socrata_task
    )


atd_knack_inventory_items_nightly_snapshot()
