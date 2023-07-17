"""
Prunes docker images by running `docker image prune`.

Dangling docker images are common with our dockerized ETLs, because the top-most image
layers contain ETL code. When that code changes, the previous layer is discarded,
resulting in dangling docker images that can consume signficant disk space. This DAG
removes those dangling images.
"""
import os

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from pendulum import datetime, duration

from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT")

default_args = {
    "owner": "airflow",
    "description": "Prune dangling docker images from system",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
}


with DAG(
    dag_id=f"atd_airflow_docker_image_prune",
    default_args=default_args,
    schedule_interval="1 4 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=duration(minutes=5),
    tags=["repo:atd-airflow"],
    catchup=False,
) as dag:
    t1 = BashOperator(
        task_id="prune_images",
        bash_command="docker image prune -f",
    )
    t1
