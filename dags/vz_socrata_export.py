import os
from pendulum import datetime, duration

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from utils.slack_operator import task_fail_slack_alert
from utils.onepassword import get_env_vars_task

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "retries": 2,
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "HASURA_ENDPOINT": {
        "opitem": "Vision Zero graphql-engine Endpoints",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.GraphQL Endpoint",
    },
    "HASURA_ADMIN_KEY": {
        "opitem": "Vision Zero graphql-engine Endpoints",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Admin Key",
    },
    "SOCRATA_KEY_ID": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SOCRATA_KEY_SECRET": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SOCRATA_APP_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
    "SOCRATA_DATASET_CRASHES": {
        "opitem": "Vision Zero Socrata Export",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.SOCRATA_DATASET_CRASHES",
    },
    "SOCRATA_DATASET_PERSONS": {
        "opitem": "Vision Zero Socrata Export",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.SOCRATA_DATASET_PERSONS",
    },
}

with DAG(
    dag_id=f"vz_socrata_export_{DEPLOYMENT_ENVIRONMENT}",
    description="Exports Vision Zero crash and people datasets to Socrata from Vision Zero database.",
    default_args=default_args,
    schedule_interval="0 4 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=duration(minutes=40),
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
