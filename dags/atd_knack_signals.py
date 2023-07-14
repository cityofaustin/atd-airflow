import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration, now

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


with DAG(
    dag_id=f"atd_knack_signals",
    description="Load signals (view_197) records from Knack to Postgrest to AGOL and Socata",
    default_args=DEFAULT_ARGS,
    schedule_interval="1 */2 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=duration(minutes=5),
    tags=["repo:atd-knack-services", "knack", "socrata", "agol"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-knack-services:production"
    app_name = "data-tracker"
    container = "view_197"
    prev_start_date = "{{ prev_start_date_success.strftime('%Y-%m-%d') if prev_start_date_success else ''}}"

    # completely replace data on the 1st of the month
    # this is to pick up any edge-case edits that incremental modified date loading may have missed
    if now().day == 1:
        prev_start_date = None

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="atd_knack_signals_to_postgrest",
        image=docker_image,
        auto_remove=True,
        command=f"./atd-knack-services/services/records_to_postgrest.py -a {app_name} -c {container} -d {prev_start_date}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    t2 = DockerOperator(
        task_id="atd_knack_signals_to_socrata",
        image=docker_image,
        auto_remove=True,
        command=f"./atd-knack-services/services/records_to_socrata.py -a {app_name} -c {container} -d {prev_start_date}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t3 = DockerOperator(
        task_id="atd_knack_signals_to_agol",
        image=docker_image,
        auto_remove=True,
        command=f"./atd-knack-services/services/records_to_agol.py -a {app_name} -c {container} -d {prev_start_date}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t1 >> t2 >> t3
