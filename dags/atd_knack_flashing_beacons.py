from os import getenv

from utils.docker_operator import DockerOperatorWithFallback
from airflow.sdk import dag
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.knack import get_date_filter_arg
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=30),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "KNACK_APP_ID": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"production.apiKey",
    },
    "SOCRATA_API_KEY_ID": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SOCRATA_API_KEY_SECRET": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SOCRATA_APP_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
    "PGREST_ENDPOINT": {
        "opitem": "atd-knack-services PostgREST",
        "opfield": "production.endpoint",
    },
    "PGREST_JWT": {
        "opitem": "atd-knack-services PostgREST",
        "opfield": "production.jwt",
    },
    "AGOL_USERNAME": {
        "opitem": "ArcGIS Online (AGOL) Scripts Publisher",
        "opfield": "production.username",
    },
    "AGOL_PASSWORD": {
        "opitem": "ArcGIS Online (AGOL) Scripts Publisher",
        "opfield": "production.password",
    },
}


DAG_DOC_MD = """
### atd_knack_flashing_beacons
Loads flashing beacons (view_1563) records from Knack to Postgrest, Socrata, and AGOL.
"""


@dag(
    dag_id="atd_knack_flashing_beacons",
    description="Load flashing beacons (view_1563) records from Knack to Postgrest to AGOL",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2015, 1, 1, tz="America/Chicago"),
    schedule="15 10 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "socrata", "agol", "data-tracker"],
    catchup=False,
    doc_md=DAG_DOC_MD,
)
def atd_knack_flashing_beacons():
    docker_image = "atddocker/atd-knack-services:production"
    app_name = "data-tracker"
    container = "view_1563"

    date_filter_arg = get_date_filter_arg(should_replace_monthly=True)

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    load_postgrest_task = DockerOperatorWithFallback(
        task_id="atd_knack_flashing_beacons_to_postgrest",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_postgrest.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    load_socrata_task = DockerOperatorWithFallback(
        task_id="atd_knack_flashing_beacons_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_socrata.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    load_agol_task = DockerOperatorWithFallback(
        task_id="atd_knack_flashing_beacons_to_agol",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_agol.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    date_filter_arg >> load_postgrest_task >> load_socrata_task >> load_agol_task


dag = atd_knack_flashing_beacons()
