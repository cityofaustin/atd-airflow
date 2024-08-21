import os
from pendulum import datetime, duration

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert


DEPLOYMENT_ENVIRONMENT = "prod" if os.getenv("ENVIRONMENT") == "production" else "dev"

REQUIRED_SECRETS = {
    "SOCRATA_DATASET_CRASHES": {
        "opitem": "Vision Zero Socrata Export v2",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.SOCRATA_DATASET_CRASHES",
    },
    "SOCRATA_DATASET_PEOPLE": {
        "opitem": "Vision Zero Socrata Export v2",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.SOCRATA_DATASET_PEOPLE",
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
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.HASURA_GRAPHQL_ENDPOINT",
    },
    "HASURA_GRAPHQL_ADMIN_SECRET": {
        "opitem": "Vision Zero CRIS Import - v2",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.HASURA_GRAPHQL_ADMIN_SECRET",
    },
}


docker_image = f"atddocker/vz-socrata-export:{'production' if DEPLOYMENT_ENVIRONMENT == 'prod' else 'development'}"


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
        trigger_rule="all_done"  # always run this task regardless of outcome of crashes task
    )

    socrata_export_crashes >> socrata_export_people
