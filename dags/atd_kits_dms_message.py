from os import getenv

from airflow.sdk import DAG
from utils.docker_operator import DockerOperatorWithFallback
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
    "retry_delay": duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "KNACK_APP_ID": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.apiKey",
    },
    "KITS_SERVER": {
        "opitem": "KITS Traffic Signal Management System",
        "opfield": "production.server",
    },
    "KITS_DATABSE": {
        "opitem": "KITS Traffic Signal Management System",
        "opfield": "production.database",
    },
    "KITS_USER": {
        "opitem": "KITS Traffic Signal Management System",
        "opfield": "production.username",
    },
    "KITS_PASSWORD": {
        "opitem": "KITS Traffic Signal Management System",
        "opfield": "production.password",
    },
}


with DAG(
    dag_id="atd_kits_dms_message_pub",
    description="Extract DMS message from KITS database and upload to Data Tracker (Knack).",
    doc_md="In order to reach kits server, you need special VPN access or run from Cameron Road Office Complex",
    default_args=DEFAULT_ARGS,
    schedule="21 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=duration(minutes=5),
    tags=["repo:atd-kits", "knack", "kits", "dms"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-kits:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="update_knack_dms_message",
        docker_conn_id="docker_default",
        image=docker_image,
        auto_remove="force",
        command="python ./atd-kits/atd-kits/dms_message_pub.py",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t1
