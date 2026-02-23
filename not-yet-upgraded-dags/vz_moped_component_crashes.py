"""Process the Moped-component crashes spatial join, populate the component-crash
lookup table in the VZ database, and then publish that entire table to Socrata.

This is the ETL that manages the Moped x VZ impact analysis data.

The VZD source and the target datasets are controlled by the Airflow `ENVIRONMENT`
env var, which determines which 1pass secrets to apply to the docker runtime env.

Moped prod is always used as the source for Moped data.

Check the 1Pass entry to understand exactly what will happen when you trigger
this DAG in a given context, but the expected behavior is that you may set the
Airflow `ENVIRONMENT` to `production`, `staging`, or `dev`, with the following
results:
- production: use production VZ db and update production data portal datasets
- staging: use staging VZ db and update staging data portal datasets
- dev: use local VZ db and update staging data portal datasets
"""

from os import getenv
from pendulum import datetime, duration

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert


DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT")
secrets_env_prefix = None

if DEPLOYMENT_ENVIRONMENT == "production":
    secrets_env_prefix = "prod"
elif DEPLOYMENT_ENVIRONMENT == "staging":
    secrets_env_prefix = "staging"
else:
    secrets_env_prefix = "dev"


REQUIRED_SECRETS_SOCRATA = {
    "SOCRATA_DATASET_CRASH_COMPONENTS": {
        "opitem": "Vision Zero ETLs",
        "opfield": f"{secrets_env_prefix}.SOCRATA_DATASET_CRASH_COMPONENTS",
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
        "opitem": "Vision Zero ETLs",
        "opfield": f"{secrets_env_prefix}.HASURA_GRAPHQL_ENDPOINT",
    },
    "HASURA_GRAPHQL_ADMIN_SECRET": {
        "opitem": "Vision Zero ETLs",
        "opfield": f"{secrets_env_prefix}.HASURA_GRAPHQL_ADMIN_SECRET",
    },
}

REQUIRED_SECRETS_MOPED_JOIN = {
    "VZ_HASURA_ENDPOINT": {
        "opitem": "Vision Zero ETLs",
        "opfield": f"{secrets_env_prefix}.HASURA_GRAPHQL_ENDPOINT",
    },
    "VZ_HASURA_ADMIN_SECRET": {
        "opitem": "Vision Zero ETLs",
        "opfield": f"{secrets_env_prefix}.HASURA_GRAPHQL_ADMIN_SECRET",
    },
    "MOPED_HASURA_ENDPOINT": {
        "opitem": "Moped Hasura Admin",
        "opfield": "production.Endpoint",
    },
    "MOPED_HASURA_ADMIN_SECRET": {
        "opitem": "Moped Hasura Admin",
        "opfield": "production.Admin Secret",
    },
}


docker_image_vz_moped_join = f"atddocker/vz-moped-join:{'production' if DEPLOYMENT_ENVIRONMENT == 'production' else 'development'}"

docker_image_socrata_export = f"atddocker/vz-socrata-export:{'production' if DEPLOYMENT_ENVIRONMENT == 'production' else 'development'}"


DEFAULT_ARGS = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "execution_timeout": duration(minutes=60),
    "on_failure_callback": task_fail_slack_alert,
}


with DAG(
    catchup=False,
    dag_id="vz-moped-component-crashes",
    description="Populate the Moped - crash lookup table and publish the table to Socrata",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 1 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    start_date=datetime(2024, 8, 1, tz="America/Chicago"),
    tags=["vision-zero", "moped", "repo:atd-vz-data", "socrata"],
) as dag:

    env_vars_moped_join = get_env_vars_task(REQUIRED_SECRETS_MOPED_JOIN)
    env_vars_socrata = get_env_vars_task(REQUIRED_SECRETS_SOCRATA)

    vz_moped_spatial_join = DockerOperator(
        task_id="vz_moped_spatial_join",
        docker_conn_id="docker_default",
        image=docker_image_vz_moped_join,
        command="./moped_project_components_spatial_join.py",
        environment=env_vars_moped_join,
        auto_remove="force",
        tty=True,
        force_pull=True,
    )

    socrata_export_crash_components = DockerOperator(
        task_id="socrata_export_crashes",
        docker_conn_id="docker_default",
        image=docker_image_socrata_export,
        command=f"./socrata_export.py --crash-components",
        environment=env_vars_socrata,
        auto_remove="force",
        tty=True,
        force_pull=True,
    )

    (
        env_vars_moped_join
        >> env_vars_socrata
        >> vz_moped_spatial_join
        >> socrata_export_crash_components
    )
