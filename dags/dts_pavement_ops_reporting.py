from os import getenv

from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
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
    "USERNAME": {
        "opitem": "Pavement ops reporting ETL",
        "opfield": "agileassets prod.agileassets username",
    },
    "PASSWORD": {
        "opitem": "Pavement ops reporting ETL",
        "opfield": "agileassets prod.agileassets password",
    },
    "CLIENT_ID": {
        "opitem": "Pavement ops reporting ETL",
        "opfield": "agileassets prod.Client ID",
    },
    "CLIENT_SECRET": {
        "opitem": "Pavement ops reporting ETL",
        "opfield": "agileassets prod.Client Secret",
    },
    "BASE_URL": {
        "opitem": "Pavement ops reporting ETL",
        "opfield": "agileassets prod.Base URL",
    },
}

with DAG(
    dag_id=f"dts_pavement_ops_reporting",
    description="Uploads reports from agileassets PMIS to socrata.",
    default_args=DEFAULT_ARGS,
    schedule="00 5 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:dts-pavement-ops-reporting", "socrata", "pmis", "pavement"],
    catchup=False,
) as dag:
    docker_image = "atddocker/dts-pavement-ops-reporting:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="VW_UPDATED_PROJECTS_SEGMENTS_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python etl/report_to_socrata.py --report VW_UPDATED_PROJECTS_SEGMENTS",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    t1
