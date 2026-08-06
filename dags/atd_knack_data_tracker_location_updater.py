from os import getenv

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag
from pendulum import datetime, duration

from utils.knack import get_date_filter_arg
from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=60),
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
    dag_id="atd_knack_data_tracker_location_updater",
    description="With data from AGOL, update signal location information in Knack",
    default_args=DEFAULT_ARGS,
    schedule="19 7 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "data-tracker", "agol"],
    catchup=False,
    doc_md="""
This DAG updates Knack signal location data using AGOL source data.
""",
)
def atd_knack_data_tracker_location_updater():
    app_name = "data-tracker"
    container = "view_1201"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)
    date_filter_arg = get_date_filter_arg()

    update_locations_task = DockerOperator(
        task_id="update_locations",
        image="atddocker/atd-knack-services:production",
        auto_remove="force",
        command=f"./atd-knack-services/services/knack_location_updater.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    date_filter_arg >> update_locations_task


atd_knack_data_tracker_location_updater()
