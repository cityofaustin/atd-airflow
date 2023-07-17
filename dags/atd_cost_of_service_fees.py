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
        "opitem": "Knack Right of Way (ROW) Portal",
        "opfield": f"production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack Right of Way (ROW) Portal",
        "opfield": f"production.apiKey",
    },
    "USER": {
        "opitem": "Amanda Read-Only (RO) replica database",
        "opfield": "production.username",
    },
    "PASSWORD": {
        "opitem": "Amanda Read-Only (RO) replica database",
        "opfield": "production.password",
    },
    "HOST": {
        "opitem": "Amanda Read-Only (RO) replica database",
        "opfield": "production.host",
    },
    "PORT": {
        "opitem": "Amanda Read-Only (RO) replica database",
        "opfield": "production.port",
    },
    "SERVICE": {
        "opitem": "Amanda Read-Only (RO) replica database",
        "opfield": "production.service",
    },
}


with DAG(
    dag_id="atd_cost_of_service_fees",
    default_args=DEFAULT_ARGS,
    description="Fetch all cost of service fees fom amanda publish to ROW knack app",
    schedule_interval="7 0 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=duration(minutes=30),
    tags=["repo:atd-cost-of-service-reporting", "knack", "amanda"],
    catchup=False,
) as dag:

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="atd_cost_of_service_fees_to_knack",
        image="atddocker/atd-cost-of-service:production",
        auto_remove=True,
        command="python knack_load_fees.py",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    t1
