from os import getenv

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag, Param
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.knack import get_date_filter_arg
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=15),
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
}

DAG_DOC_MD = """
## AMD Locations (Knack to PostgREST and Socrata)

Loads AMD locations from Knack Data Tracker (app 'data-tracker', container 'view_5060') then
publishes them to PostgREST and Socrata EDP (resource_id 'df22-6dur')

"""


@dag(
    dag_id="atd_knack_amd_locations",
    description="Load AMD locations from Knack to Postgres to Socrata",
    default_args=DEFAULT_ARGS,
    schedule="37 23 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "socrata", "data-tracker"],
    catchup=False,
    params={
        "docker_image_tag": Param(
            "production",
            type="string",
            title="Docker image tag",
            description="Tag of atddocker/atd-knack-services to run. Use 'local' for a locally built image.",
        ),
    },
    doc_md=DAG_DOC_MD,
)
def atd_knack_amd_locations():
    docker_image = "atddocker/atd-knack-services:{{ params.docker_image_tag }}"
    app_name = "data-tracker"
    container = "view_5060"

    date_filter_arg = get_date_filter_arg(should_replace_monthly=False)

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    to_postgrest = DockerOperator(
        task_id="atd_knack_amd_locations_to_postgrest",
        image=docker_image,
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_postgrest.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    to_socrata = DockerOperator(
        task_id="atd_knack_amd_locations_to_socrata",
        image=docker_image,
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_socrata.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    date_filter_arg >> to_postgrest >> to_socrata


atd_knack_amd_locations()
