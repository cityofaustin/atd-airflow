"""
This DAG refreshes the location_crashes_view in the Vision Zero database
"""

from os import getenv
from pendulum import datetime, duration

from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator

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


REQUIRED_SECRETS = {
    "HASURA_GRAPHQL_ENDPOINT": {
        "opitem": "Vision Zero ETLs",
        "opfield": f"{secrets_env_prefix}.HASURA_GRAPHQL_SCHEMA_API_ENDPOINT",
    },
    "HASURA_GRAPHQL_ADMIN_SECRET": {
        "opitem": "Vision Zero ETLs",
        "opfield": f"{secrets_env_prefix}.HASURA_GRAPHQL_ADMIN_SECRET",
    },
}


docker_image = f"atddocker/vz-run-sql:{'production' if DEPLOYMENT_ENVIRONMENT == 'production' else 'development'}"


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
    dag_id="vz-location-crashes-refresh",
    description="Refreshes the materialized view: location_crashes_view ",
    default_args=DEFAULT_ARGS,
    schedule="0 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    start_date=datetime(2024, 8, 1, tz="America/Chicago"),
    tags=["vision-zero", "repo:vision-zero"],
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    refresh_location_crashes = DockerOperator(
        task_id="refresh_location_crashes",
        image=docker_image,
        command=f"./run_sql.py -c refresh_location_crashes",
        environment=env_vars,
        auto_remove="force",
        tty=True,
    )

    refresh_location_crashes
