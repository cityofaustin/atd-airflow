# test locally with: docker compose run --rm airflow-cli dags test dts_maximo_reporting_workorders

import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=15),
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
    "MAXIMO_DB_PASS": {
        "opitem": "Maximo Data Warehouse",
        "opfield": "production.db password",
    },
    "MAXIMO_DB_USER": {
        "opitem": "Maximo Data Warehouse",
        "opfield": "production.db username",
    },
    "MAXIMO_SERVICE_NAME": {
        "opitem": "Maximo Data Warehouse",
        "opfield": "production.service name",
    },
    "MAXIMO_HOST": {
        "opitem": "Maximo Data Warehouse",
        "opfield": "production.host",
    },
    "MAXIMO_PORT": {
        "opitem": "Maximo Data Warehouse",
        "opfield": "production.port",
    },
}

with DAG(
    dag_id=f"dts_maximo_reporting_workorders",
    description="Uploads the last 7 days of Maximo work orders to Socrata from the Maximo data warehouse.",
    default_args=DEFAULT_ARGS,
    schedule_interval="00 4 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:dts-maximo-reporting", "socrata", "maximo"],
    catchup=False,
) as dag:
    docker_image = "atddocker/dts-maximo-reporting:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="maximo_workorders_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"python etl/work_orders_to_socrata.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    t1
