import os
import pendulum

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "description": "Create/update 'Index' issues in the DTS portal from Github.",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2015, 12, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
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
    default_args=default_args,
    schedule_interval="0 5 * * *",
    dagrun_timeout=pendulum.duration(minutes=60),
    tags=["repo:atd-service-bot", "knack", "github"],
    catchup=False,
) as dag:
    @task(
        task_id="get_env_vars",
        execution_timeout=pendulum.duration(seconds=30),
    )
    def get_env_vars():
        from utils.onepassword import load_dict
        env_vars = load_dict(REQUIRED_SECRETS)
        return env_vars
    
    env_vars = get_env_vars()
    
    DockerOperator(
        task_id="github_to_dts_portal",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="./atd-service-bot/gh_index_issues_to_dts_portal.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
    )
