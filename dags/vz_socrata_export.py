import os
from pendulum import datetime, duration

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

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
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.GraphQL Endpoint",
    },
    "HASURA_ADMIN_KEY": {
        "opitem": "Vision Zero CRIS Import",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.GraphQL Endpoint key",
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
    "SOCRATA_DATASET_CRASHES": {
        "opitem": "Vision Zero CRIS Import",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.SOCRATA_DATASET_CRASHES",
    },
    "SOCRATA_DATASET_PERSONS": {
        "opitem": "Vision Zero CRIS Import",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.SOCRATA_DATASET_PERSONS",
    },
}

with DAG(
    dag_id=f"vz-cr3-download_{DEPLOYMENT_ENVIRONMENT}",
    default_args=default_args,
    schedule_interval="*/5 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=duration(minutes=5),
    tags=["repo:atd-vz-data", "socrata"],
    catchup=False,
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    DockerOperator(
        task_id="socrata_export",
        image=f"atddocker/vz-socrata-export:{DEPLOYMENT_ENVIRONMENT}",
        api_version="auto",
        auto_remove=True,
        command="python socrata_export.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )
