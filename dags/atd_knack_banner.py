import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.decorators import task
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
    "BANNER_URL": {
        "opitem": "HRD Banner API",
        "opfield": f"production.endpoint",
    },
    "BANNER_API_KEY": {
        "opitem": "HRD Banner API",
        "opfield": f"production.apiKey",
    },
    "KNACK_APP_ID": {
        "opitem": "Knack Human Resources (HR)",
        "opfield": f"production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack Human Resources (HR)",
        "opfield": f"production.apiKey",
    },
}


with DAG(
    dag_id=f"atd_knack_banner",
    description="Update knack HR app based on records in Banner and CTM",
    default_args=DEFAULT_ARGS,
    schedule_interval="45 7 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=duration(minutes=30),
    tags=["repo:atd-knack-banner", "knack"],
    catchup=False,
) as dag:
    docker_image = f"atddocker/atd-knack-banner:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    update_employees = DockerOperator(
        task_id="update_employees",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"./atd-knack-banner/update_employees.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    update_employees
