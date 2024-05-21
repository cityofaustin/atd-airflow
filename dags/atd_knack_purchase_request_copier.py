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
    "execution_timeout": duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}


REQUIRED_SECRETS = {
    "KNACK_APP_ID": {
        "opitem": "Knack Finance and Purchasing",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack Finance and Purchasing",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.apiKey",
    },
}

with DAG(
    dag_id="atd_knack_purchase_request_copier",
    description="Copy requested records in the finance-purchasing knack app.",
    default_args=DEFAULT_ARGS,
    schedule_interval="* * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "finance-purchasing", "finance"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-knack-services:production"
    app_name = "finance-purchasing"
    container = "view_211"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="purchase_request_copier",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"./atd-knack-services/services/purchase_request_copier.py -a {app_name} -c {container}",
        environment=env_vars,
        tty=True,
        force_pull=False,   # atd_knack_signals pulls this image every 5 minutes
        mount_tmp_dir=False,
    )
    
    t1
