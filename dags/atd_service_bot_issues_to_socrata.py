import os
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from utils.slack_operator import task_fail_slack_alert
from utils.onepassword import load_dict

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "description": "Publish atd-data-tech Github issues to Socrata",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-service-bot:production"

REQUIRED_SECRETS = {
    "GITHUB_ACCESS_TOKEN": {
        "opitem": "Service Bot",
        "opfield": "shared.githubAccessToken",
    },
    "ZENHUB_ACCESS_TOKEN": {
        "opitem": "Service Bot",
        "opfield": "shared.zenhubAccessToken",
    },
    "SOCRATA_API_KEY_ID": {
        "opitem": "Service Bot",
        "opfield": "shared.socrataApiKeyID",
    },
    "SOCRATA_API_KEY_SECRET": {
        "opitem": "Service Bot",
        "opfield": "shared.socrataApiKeySecret",
    },
    "SOCRATA_APP_TOKEN": {
        "opitem": "Service Bot",
        "opfield": "shared.socrataAppToken",
    },
    "SOCRATA_RESOURCE_ID": {
        "opitem": "Service Bot",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.socrataResourceId",
    },
    "SOCRATA_ENDPOINT": {
        "opitem": "Service Bot",
        "opfield": f"shared.socrataEndpoint",
    },
}

env_vars = load_dict(REQUIRED_SECRETS)

with DAG(
    dag_id=f"atd_service_bot_github_to_socrata_{DEPLOYMENT_ENVIRONMENT}",
    default_args=default_args,
    schedule_interval="21 5 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["repo:atd-service-bot", "socrata", "github"],
    catchup=False,
) as dag:
    t1 = DockerOperator(
        task_id="dts_github_to_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="./atd-service-bot/issues_to_socrata.py",
        environment=env_vars,
        tty=True,
    )

    t1
