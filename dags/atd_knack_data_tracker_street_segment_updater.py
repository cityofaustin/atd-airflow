from os import getenv

from utils.docker_operator import DockerOperatorWithFallback
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
### DAG purpose
This DAG updates street segment records in the AMD Data Tracker using ArcGIS Online source data.
"""


@dag(
    dag_id="atd_knack_data_tracker_street_segment_updater",
    description="Update street segment records in Data Tracker with feature data from ArcGIS Online",
    default_args=DEFAULT_ARGS,
    schedule="45 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "data-tracker", "agol"],
    catchup=False,
    doc_md=DAG_DOC_MD,
)
def atd_knack_data_tracker_street_segment_updater():
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    date_filter_arg = get_date_filter_arg()

    update_street_segments_task = DockerOperator(
        task_id="update_street_segments",
        image="atddocker/atd-knack-services:production",
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/knack_street_seg_updater.py -a data-tracker -c view_1198 {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    date_filter_arg >> update_street_segments_task


atd_knack_data_tracker_street_segment_updater()
