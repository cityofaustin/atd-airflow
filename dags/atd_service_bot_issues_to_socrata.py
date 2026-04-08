from os import getenv
from pendulum import datetime, duration

from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from utils.slack_operator import task_fail_slack_alert
from utils.onepassword import get_env_vars_task

doc_md="""
Publishes issues from the atd-data-tech github repository to the open data portal: https://data.austintexas.gov/resource/rzwg-fyv8.json

---
The DTS team site (austinmobility.io) uses the open data portal dataset, if you want to update the issues on the team site with the latest issues from github, trigger this dag.
"""

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "description": "Publish atd-data-tech Github issues to Socrata",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "execution_timeout": duration(minutes=60),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-service-bot:production"

REQUIRED_SECRETS = {
    "GITHUB_ACCESS_TOKEN": {
        "opitem": "Github Access Token Service Bot",
        "opfield": ".password",
    },
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
    "SOCRATA_RESOURCE_ID": {
        "opitem": "Service Bot",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.socrataResourceId",
    },
    "SOCRATA_ENDPOINT": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.endpoint",
    },
}


with DAG(
    dag_id=f"atd_service_bot_github_to_socrata_{DEPLOYMENT_ENVIRONMENT}",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="0 22 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-service-bot", "socrata", "github"],
    catchup=False,
) as dag:

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    DockerOperator(
        task_id="dts_github_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command="./atd-service-bot/issues_to_socrata.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )
