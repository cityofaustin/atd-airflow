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
    "execution_timeout": duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
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
    "KITS_SERVER": {
        "opitem": "KITS Traffic Signal Management System",
        "opfield": "production.server",
    },
    "KITS_USER": {
        "opitem": "KITS Traffic Signal Management System",
        "opfield": "production.username",
    },
    "KITS_PASSWORD": {
        "opitem": "KITS Traffic Signal Management System",
        "opfield": "production.password",
    },
    "KITS_DATABSE": {  # SIC
        "opitem": "KITS Traffic Signal Management System",
        "opfield": "production.database",
    },
}


with DAG(
    dag_id=f"atd_kits_sig_stat_pub",
    description="Fetch signal flash statuses KITS and publish to Socata",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-kits", "socrata", "kits"],
    catchup=False,
) as dag:

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="atd_kits_sig_status_to_socrata",
        image="atddocker/atd-kits:production",
        auto_remove=True,
        command="./atd-kits/atd-kits/signal_status_publisher.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    t1
