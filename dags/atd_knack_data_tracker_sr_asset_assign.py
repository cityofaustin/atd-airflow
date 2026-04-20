from os import getenv

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag
from pendulum import datetime, duration

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
    "KNACK_API_USER_EMAIL": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"production.apiUserEmail",
    },
    "KNACK_API_USER_PW": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"production.apiUserPassword",
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
This DAG assigns signal records to service request issues in the AMD Data Tracker based on service request location.
"""


@dag(
    dag_id="atd_knack_data_tracker_sr_asset_assign",
    description="Assigns signal records to CSR issues in data tracker based on CSR location",
    default_args=DEFAULT_ARGS,
    schedule="* * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "data-tracker"],
    catchup=False,
    doc_md=DAG_DOC_MD,
)
def atd_knack_data_tracker_sr_asset_assign():
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    service_request_asset_assign_task = DockerOperator(
        task_id="service_request_asset_assign",
        image="atddocker/atd-knack-services:production",
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/sr_asset_assign.py -a data-tracker -c view_2362 -s signals",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    service_request_asset_assign_task


atd_knack_data_tracker_sr_asset_assign()
