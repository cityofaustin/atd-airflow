import os
import pendulum

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from utils.slack_operator import task_fail_slack_alert


DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "description": "Publish atd-data-tech Github issues to Socrata",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2015, 12, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-service-bot:production"

REQUIRED_SECRETS = {
    "GITHUB_ACCESS_TOKEN": {
        "opitem": "Github Access Token Service Bot",
        "opfield": ".password",
    },
    "ZENHUB_ACCESS_TOKEN": {
        "opitem": "Zenhub Access Token",
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
    default_args=default_args,
    schedule_interval="0 22 * * *",
    dagrun_timeout=pendulum.duration(minutes=60),
    tags=["repo:atd-service-bot", "socrata", "github"],
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
        task_id="dts_github_to_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="./atd-service-bot/issues_to_socrata.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
    )

