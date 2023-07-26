import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration

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

with DAG(
    dag_id=f"atd_knack_data_tracker_sr_asset_assign",
    description="Assigns signal records to CSR issues in data tracker based on CSR location",
    default_args=DEFAULT_ARGS,
    schedule_interval="* * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=duration(minutes=5),
    tags=["repo:atd-knack-services", "knack", "data-tracker"],
    catchup=False,
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="service_request_asset_assign",
        image= "atddocker/atd-knack-services:production",
        auto_remove=True,
        command=f"./atd-knack-services/services/sr_asset_assign.py -a data-tracker -c view_2362 -s signals",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    t1
