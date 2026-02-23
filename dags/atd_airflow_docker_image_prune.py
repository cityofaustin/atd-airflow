"""
Prunes docker images by running `docker image prune`.

Dangling docker images are common with our dockerized ETLs, because the top-most image
layers contain ETL code. When that code changes, the previous layer is discarded,
resulting in dangling docker images that can consume significant disk space. This DAG
removes those dangling images.
"""

from os import getenv

from airflow.decorators import dag, task
from pendulum import datetime, duration

from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT")


@dag(
    dag_id="atd_airflow_docker_image_prune",
    schedule="1 4 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    start_date=datetime(2015, 12, 1, tz="America/Chicago"),
    catchup=False,
    tags=["repo:atd-airflow"],
    default_args={
        "owner": "airflow",
        "retries": 0,
        "on_failure_callback": task_fail_slack_alert,
        "execution_timeout": duration(minutes=5),
    },
    description="Prune dangling docker images from system",
)
def atd_airflow_docker_image_prune():
    """
    Prunes docker images and containers to free up disk space.

    This DAG runs daily in production to clean up dangling docker images
    that accumulate from our dockerized ETLs.
    """

    @task.bash(task_id="prune_images")
    def prune_images():
        """Remove dangling docker images."""
        return "docker image prune -f"

    @task.bash(task_id="prune_containers")
    def prune_containers():
        """Remove stopped docker containers."""
        return "docker container prune -f"

    # Set up task dependencies
    prune_images() >> prune_containers()


# Instantiate the DAG
atd_airflow_docker_image_prune()
