# Test locally with: docker compose run --rm airflow-cli dags test atd_moped_components_to_agol

import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.decorators import task
from airflow.models import Param
from pendulum import datetime, duration, parse

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
    "retry_delay": duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "HASURA_ENDPOINT": {
        "opitem": "Moped Hasura Admin",
        "opfield": "production.Endpoint",
    },
    "HASURA_ADMIN_SECRET": {
        "opitem": "Moped Hasura Admin",
        "opfield": "production.Admin Secret",
    },
    "AGOL_USERNAME": {
        "opitem": "AGOL Scripts Publisher",
        "opfield": "production.Username",
    },
    "AGOL_PASSWORD": {
        "opitem": "AGOL Scripts Publisher",
        "opfield": "production.Password",
    },
}


with DAG(
    dag_id="atd_moped_ecapris_status_sync",
    description="sync eCapris statuses to Moped database",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-moped", "moped", "agol"],
    catchup=False,
    params={"full_replace": Param(default=False, type="boolean")},
    max_active_runs=1,  # Block schedule while DAG with params is triggered
) as dag:
    docker_image = "atddocker/atd-moped-etl-ecapris-statuses:production"

    t1 = DockerOperator(
        task_id="ecapris_statues_to_moped",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python3 ecapris_statuses_sync.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        execution_timeout=duration(minutes=30),
    )

    incremental = DockerOperator(
        task_id="moped_components_to_agol_incremental",
        image=docker_image,
        auto_remove="force",
        command=f"python components_to_agol.py {args}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        execution_timeout=duration(minutes=5),
    )

    t1
