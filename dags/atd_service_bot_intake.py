import os
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from utils.slack_operator import task_fail_slack_alert
from utils.onepassword import load_dict

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "description": "Fetch new DTS service requests and create Github issues",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-service-bot:production"

REQUIRED_SECRETS = {
    "KNACK_APP_ID": {
        "opitem": "Service Bot",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.knackAppId",
    },
    "KNACK_API_KEY": {
        "opitem": "Service Bot",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.knackApiKey",
    },
    "GITHUB_ACCESS_TOKEN": {
        "opitem": "Service Bot",
        "opfield": "shared.githubAccessToken",
    },
    "KNACK_DTS_PORTAL_SERVICE_BOT_USERNAME": {
        "opitem": "Service Bot",
        "opfield": "shared.dtsPortalLoginEmail",
    },
    "KNACK_DTS_PORTAL_SERVICE_BOT_PASSWORD": {
        "opitem": "Service Bot",
        "opfield": "shared.dtsPortalLoginPassword",
    },
}

env_vars = load_dict(REQUIRED_SECRETS)

with DAG(
    dag_id=f"atd_service_bot_issue_intake_{DEPLOYMENT_ENVIRONMENT}",
    default_args=default_args,
    schedule_interval="*/3 * * * *",
    dagrun_timeout=timedelta(minutes=5),
    tags=["repo:atd-service-bot", "knack", "github"],
    catchup=False,
) as dag:
    t1 = DockerOperator(
        task_id="dts_sr_to_github",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="./atd-service-bot/intake.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
    )

    t1
