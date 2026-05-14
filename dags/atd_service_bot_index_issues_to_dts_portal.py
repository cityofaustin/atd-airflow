from os import getenv
from pendulum import datetime, duration

from airflow.sdk import DAG
from utils.docker_operator import DockerOperatorWithFallback

from utils.slack_operator import task_fail_slack_alert
from utils.onepassword import get_env_vars_task

doc_md = """
Issues labeled 'Project Index' are updated in the Knack DTS Portal.


These issues' evaluations are then referenced on the DTS website (austinmobility.io)
"""

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "description": "Create/update 'Project Index' issues in the Knack DTS portal from Github.",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=20),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-service-bot:production"

REQUIRED_SECRETS = {
    "KNACK_APP_ID": {
        "opitem": "Knack DTS Portal",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack DTS Portal",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.apiKey",
    },
    "GITHUB_ACCESS_TOKEN": {
        "opitem": "Github Access Token Service Bot",
        "opfield": ".password",
    },
}

with DAG(
    dag_id=f"atd_service_bot_issues_to_dts_portal_{DEPLOYMENT_ENVIRONMENT}",
    default_args=DEFAULT_ARGS,
    doc_md=doc_md,
    schedule="0 5 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-service-bot", "knack", "github"],
    catchup=False,
) as dag:

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    DockerOperatorWithFallback(
        force_pull=True,
        task_id="github_to_dts_portal",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command="./atd-service-bot/gh_index_issues_to_dts_portal.py",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )
