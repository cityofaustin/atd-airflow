import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration, now

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
    "execution_timeout": duration(minutes=15),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS_ARTBOX_POSTGREST = {
    "KNACK_APP_ID": {
        "opitem": "Knack Smark Mobility",
        "opfield": f"production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack Smark Mobility",
        "opfield": f"production.apiKey",
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

REQUIRED_SECRETS_SIGNALS_TO_SMO = {
    "KNACK_APP_ID_DEST": {
        "opitem": "Knack Smark Mobility",
        "opfield": f"production.appId",
    },
    "KNACK_API_KEY_DEST": {
        "opitem": "Knack Smark Mobility",
        "opfield": f"production.apiKey",
    },
    "KNACK_APP_ID_SRC": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"production.appId",
    },
    "KNACK_API_KEY_SRC": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"production.apiKey",
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


"""This DAG keeps signals in sync between Data Tracker and SMO app. It:
- publishes the current signal records in the SMO app to PostgREST (records_to_postgrest.py)
- Assumes the current data tracker signals are in postgrest (that's managed in another DAG)
- compares the current signals in data tracker and SMO, and updates records in SMO (records_to_knack.py)

"""
with DAG(
    dag_id=f"atd_knack_artbox_signals",
    description="Load signals (view_197) records from Knack data tracker to smart mobility",
    default_args=DEFAULT_ARGS,
    schedule_interval="30 0 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "data-tracker", "smart-mobility"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-knack-services:production"
    app_name_src = "data-tracker"
    container_src = "view_197"
    app_name_dest = "smart-mobility"
    container_dest = "view_396"

    date_filter_arg = get_date_filter_arg(should_replace_monthly=True)
    env_vars_t1 = get_env_vars_task(REQUIRED_SECRETS_ARTBOX_POSTGREST)
    env_vars_t2 = get_env_vars_task(REQUIRED_SECRETS_SIGNALS_TO_SMO)

    t1 = DockerOperator(
        task_id="atd_knack_artbox_signals_to_postgrest",
        image=docker_image,
        auto_remove=True,
        command=f"./atd-knack-services/services/records_to_postgrest.py -a {app_name_dest} -c {container_dest} {date_filter_arg}",
        environment=env_vars_t1,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    t2 = DockerOperator(
        task_id="atd_knack_data_tracker_signals_to_smo",
        image=docker_image,
        auto_remove=True,
        command=f"./atd-knack-services/services/records_to_knack.py -a {app_name_src} -c {container_src} {date_filter_arg} -dest {app_name_dest}",
        environment=env_vars_t2,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )
    date_filter_arg >> t1 >> t2
