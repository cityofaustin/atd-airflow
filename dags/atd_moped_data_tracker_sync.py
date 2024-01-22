# Test locally with: docker compose run --rm airflow-cli dags test atd_moped_data_tracker_sync

import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert
from utils.knack import get_date_filter_arg

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
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Endpoint",
    },
    "HASURA_ADMIN_SECRET": {
        "opitem": "Moped Hasura Admin",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Admin Secret",
    },
    "KNACK_APP_ID": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.apiKey",
    },
}


with DAG(
    dag_id="atd_moped_data_tracker_sync",
    description="sync Moped projects data to Knack Data Tracker projects table",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=duration(minutes=30),
    tags=["repo:atd-moped", "moped", "agol"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-moped-etl-data-tracker-sync:production"

    date_filter_arg = get_date_filter_arg()

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="data_tracker_sync",
        image=docker_image,
        auto_remove=True,
        command=f"python data_tracker_sync.py --start {date_filter_arg}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    t1
