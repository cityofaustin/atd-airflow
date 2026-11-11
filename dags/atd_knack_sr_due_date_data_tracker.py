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
    "start_date": datetime(2026, 11, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=10),
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
}

DAG_DOC_MD = """

"""


@dag(
    dag_id="atd_knack_data_tracker_sr_due_date",
    description="",
    default_args=DEFAULT_ARGS,
    schedule="* * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "data-tracker"],
    catchup=False,
    doc_md=DAG_DOC_MD,
)
def atd_knack_data_tracker_sr_due_date():
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    service_request_due_date_assign_task = DockerOperator(
        task_id="service_request_due_date_assign",
        image="atddocker/atd-knack-services:production",
        auto_remove="force",
        command=f"./atd-knack-services/services/sr_asset_assign.py -a data-tracker",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    service_request_asset_assign_task


atd_knack_data_tracker_sr_asset_assign()
