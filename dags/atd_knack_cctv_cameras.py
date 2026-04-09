from os import getenv

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.knack import get_date_filter_arg
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=5),
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


@dag(
    dag_id="atd_knack_cctv_cameras",
    description="Publishes CCTV records from Data Tracker to Socrata and AGOL",
    default_args=DEFAULT_ARGS,
    schedule="55 1 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "socrata", "agol", "data-tracker"],
    catchup=False,
    doc_md="""
## CCTV cameras (Knack to PostgREST, Socrata, and AGOL)

Loads CCTV camera records from Knack Data Tracker (app 'data-tracker', container
'view_395') using the 'atddocker/atd-knack-services:production' image, then
publishes them to PostgREST, Socrata, and ArcGIS Online ('AGOL').

### Task flow

1. 'get_date_filter_arg' — supplies an incremental date flag (or full replace on
   the first of the month when configured).
2. 'get_env_vars' — loads API and service credentials from 1Password.
3. 'atd_knack_cctv_cameras_to_postgrest' — runs 'records_to_postgrest.py'.
4. 'atd_knack_cctv_cameras_to_socrata' — runs 'records_to_socrata.py'.
5. 'atd_knack_cctv_cameras_to_agol' — runs 'records_to_agol.py'.

### New Airflow environments

The 'get_date_filter_arg' task uses the previous successful run time
('prev_start_date_success' in task context) to build the incremental date filter.
A brand-new Airflow database has no prior successful runs for this DAG, so that
value may not behave as expected until history exists. When moving this DAG to a
new Airflow environment, add an artificial successful run (or otherwise seed
the behavior you want) so the first real run uses an appropriate baseline date.

### Docker

Tasks use connection 'docker_default' and pull the image on the first task.
""",
)
def atd_knack_cctv_cameras():
    docker_image = "atddocker/atd-knack-services:production"
    app_name = "data-tracker"
    container = "view_395"

    date_filter_arg = get_date_filter_arg(should_replace_monthly=True)

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    to_postgrest = DockerOperator(
        task_id="atd_knack_cctv_cameras_to_postgrest",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_postgrest.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    to_socrata = DockerOperator(
        task_id="atd_knack_cctv_cameras_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_socrata.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    to_agol = DockerOperator(
        task_id="atd_knack_cctv_cameras_to_agol",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_agol.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    date_filter_arg >> to_postgrest >> to_socrata >> to_agol


atd_knack_cctv_cameras()
