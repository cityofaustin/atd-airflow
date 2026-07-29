"""
Pulls the latest production docker images used by our dockerized ETLs.
"""

import logging
import subprocess
from os import getenv

from airflow.sdk import Param, chain, dag, task
from pendulum import datetime, duration

from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT")
logger = logging.getLogger(__name__)

# Docker images used by our DAGs, kept warm so task runs don't have to pull on demand
DOCKER_IMAGES = [
    "atddocker/atd-airflow:production",
    "atddocker/atd-cost-of-service:production",
    "atddocker/atd-finance-data:production", 
    "atddocker/atd-kits:production",
    "atddocker/atd-knack-311:production",
    "atddocker/atd-knack-banner:production",
    "atddocker/atd-knack-services:production",
    "atddocker/atd-moped-etl-arcgis:production",
    "atddocker/atd-moped-etl-data-tracker-sync:production",
    "atddocker/atd-moped-etl-ecapris-funding:production",
    "atddocker/atd-moped-etl-ecapris-statuses:production",
    "atddocker/atd-parking-data-meters:production",
    "atddocker/atd-road-conditions:production",
    "atddocker/atd-service-bot:production",
    "atddocker/atd-signal-comms:production",
    "atddocker/atd-traffic-incident-reports:production",
    "atddocker/dts-311-reporting:production",
    "atddocker/dts-finance-reporting:production",
    "atddocker/dts-maximo-reporting:production",
    "atddocker/dts-pavement-ops-reporting:production",
    "atddocker/dts-right-of-way-reporting:production",
    "atddocker/dts-traffic-signal-metrics:production",
    "atddocker/dts-work-zone-data-feed:production",
    "atddocker/maximo-geo-emergency-mgmt:production",
    "atddocker/vz-afd-ems-import:production",
    "atddocker/vz-cad-incidents-import:production",
    "atddocker/vz-cris-import:production",
    "atddocker/vz-ems-person-match:production",
    "atddocker/vz-moped-join:production",
    "atddocker/vz-run-sql:production",
    "atddocker/vz-socrata-export:production",
]


@dag(
    dag_id="airflow_docker_image_pull",
    schedule="0 6 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    start_date=datetime(2015, 12, 1, tz="America/Chicago"),
    catchup=False,
    tags=["repo:atd-airflow"],
    default_args={
        "owner": "airflow",
        "retries": 0,
        "on_failure_callback": task_fail_slack_alert,
        "execution_timeout": duration(minutes=30),
    },
    description="Pull docker images used by our ETLs to keep them up to date",
    params={
        "dry_run": Param(
            title="Dry run",
            default=False,
            type="boolean",
            description_md="Log images that would be pulled without running docker pull.",
        ),
    },
)
def airflow_docker_image_pull():
    """
    Pulls the docker images used by our ETLs.

    This DAG runs every 6 hours in production to keep local docker images
    current, so DAG runs don't need to pull on demand.
    """

    @task
    def pull_image(image: str, params):
        """Pull a single docker image, or log what would be pulled in dry-run mode."""
        if bool(params["dry_run"]):
            logger.info("Would pull %s", image)
            return

        logger.info("Pulling %s", image)
        subprocess.run(["docker", "pull", image], check=True)

    pull_tasks = []
    for image in DOCKER_IMAGES:
        # atddocker/atd-airflow:production -> pull_atd-airflow
        image_name = image.split("/")[-1].split(":")[0]
        pull_tasks.append(
            pull_image.override(
                task_id=f"pull_{image_name}",
                # Continue the chain even if an upstream pull failed
                trigger_rule="all_done",
            )(image)
        )

    chain(*pull_tasks)


# Instantiate the DAG
airflow_docker_image_pull()
