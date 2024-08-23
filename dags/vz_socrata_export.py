"""Download crash and people records and publish to the Open Data Portal.

This is the ETL that keeps the VZV up to date.

The VZD source and the target datasets are controlled by the Airflow `ENVIRONMENT`
env var, which determines which 1pass secrets to apply to the docker runtime env.

Check the 1Pass entry to understand exactly what will happen when you trigger
this DAG in a given context, but the expected behavior is that you may set the 
Airflow `ENVIRONMENT` to `production`, `staging`, or `dev`, with the following
results:
- production: use production VZ db and update production data portal datasets
- staging: use staging VZ db and update staging data portal datasets
- dev: use local VZ db and update staging data portal datasets
"""

import os
from pendulum import datetime, duration

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert


DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT")
secrets_env_prefix = None

if DEPLOYMENT_ENVIRONMENT == "production":
    secrets_env_prefix = "prod"
elif DEPLOYMENT_ENVIRONMENT == "staging":
    secrets_env_prefix = "staging"
else:
    secrets_env_prefix = "dev"


REQUIRED_SECRETS = {
    "SOCRATA_DATASET_CRASHES": {
        "opitem": "Vision Zero Socrata Export v2",
        "opfield": f"{secrets_env_prefix}.SOCRATA_DATASET_CRASHES",
    },
    "SOCRATA_DATASET_PEOPLE": {
        "opitem": "Vision Zero Socrata Export v2",
        "opfield": f"{secrets_env_prefix}.SOCRATA_DATASET_PEOPLE",
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
    "HASURA_GRAPHQL_ENDPOINT": {
        "opitem": "Vision Zero CRIS Import - v2",
        "opfield": f"{secrets_env_prefix}.HASURA_GRAPHQL_ENDPOINT",
    },
    "HASURA_GRAPHQL_ADMIN_SECRET": {
        "opitem": "Vision Zero CRIS Import - v2",
        "opfield": f"{secrets_env_prefix}.HASURA_GRAPHQL_ADMIN_SECRET",
    },
}


docker_image = f"atddocker/vz-socrata-export:{'production' if DEPLOYMENT_ENVIRONMENT == 'production' else 'development'}"


DEFAULT_ARGS = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "execution_timeout": duration(minutes=60),
    "on_failure_callback": task_fail_slack_alert,
}


with DAG(
    catchup=False,
    dag_id="vz-socrata-export",
    description="Exports Vision Zero crash and people datasets to Socrata from Vision Zero database.",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 4 * * *" if DEPLOYMENT_ENVIRONMENT == "prod" else None,
    start_date=datetime(2024, 8, 1, tz="America/Chicago"),
    tags=["vision-zero", "cris", "repo:atd-vz-data", "socrata"],
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    socrata_export_crashes = DockerOperator(
        task_id="socrata_export_crashes",
        docker_conn_id="docker_default",
        image=docker_image,
        command=f"./socrata_export.py --crashes",
        environment=env_vars,
        auto_remove=True,
        tty=True,
        force_pull=True,
    )

    socrata_export_people = DockerOperator(
        task_id="socrata_export_people",
        docker_conn_id="docker_default",
        image=docker_image,
        command=f"./socrata_export.py --people",
        environment=env_vars,
        auto_remove=True,
        tty=True,
        force_pull=True,
        trigger_rule="all_done",  # always run this task regardless of outcome of crashes task
    )

    socrata_export_crashes >> socrata_export_people
