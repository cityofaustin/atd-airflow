import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.knack import get_date_filter_arg
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
        "opitem": "Vision Zero graphql-engine Endpoints",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Query Endpoint",
    },
    "HASURA_ADMIN_KEY": {
        "opitem": "Vision Zero graphql-engine Endpoints",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Admin Key",
    },
}

with DAG(
    dag_id="vision_zero_reassociate_missing_locations",
    description="Execute housekeeping routine to associate VZ Polygons and Crashes together",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 3 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=duration(minutes=15),
    tags=["repo:atd_vz_data", "vision-zero", "polygons", "crashes"],
    catchup=False,
) as dag:
    docker_image = "atddocker/vz-location-associations:latest"

    date_filter_arg = get_date_filter_arg(should_replace_monthly=True)
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    # This process will find the locations for CR3 crashes that do not have one but
    # fall into a location and they are not mainlanes.
    update_cr3_locations = DockerOperator(
        task_id="update_cr3_locations",
        image=docker_image,
        auto_remove=True,
        command="python scripts/atd_vzd_update_cr3_locations.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    # This process will find the locations for Non-CR3 crashes that do not have one but
    # fall into a location and they are not mainlanes.
    update_noncr3_locations = DockerOperator(
        task_id="update_noncr3_locations",
        image=docker_image,
        auto_remove=True,
        command="python scripts/atd_vzd_update_noncr3_locations.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    # This process will remove the location for CR3 crashes that are main-lanes.
    dissociate_cr3 = DockerOperator(
        task_id="dissociate_cr3",
        image=docker_image,
        auto_remove=True,
        command="python scripts/atd_vzd_dissociate_cr3_mainlanes.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    # This process will remove the location for Non-CR3 crashes that are main-lanes.
    dissociate_noncr3 = DockerOperator(
        task_id="dissociate_noncr3",
        image=docker_image,
        auto_remove=True,
        command="python scripts/atd_vzd_dissociate_noncr3_mainlanes.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    reassociate_wrong_locations = DockerOperator(
        task_id="reassociate_wrong_locations",
        image=docker_image,
        auto_remove=True,
        command="python scripts/atd_vzd_reassociate_wrong_locations.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

(
    update_cr3_locations
    >> update_noncr3_locations
    >> dissociate_cr3
    >> dissociate_noncr3
    >> reassociate_wrong_locations
)
