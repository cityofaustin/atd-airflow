from os import getenv
from pendulum import datetime, duration

from airflow.sdk import DAG
from utils.docker_operator import DockerOperatorWithFallback

from utils.slack_operator import task_fail_slack_alert
from utils.onepassword import get_env_vars_task

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "description": "Fetch new DTS service requests from Knack DTS Portal and create Github issues",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=5),
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
    "KNACK_DTS_PORTAL_SERVICE_BOT_USERNAME": {
        "opitem": "Knack DTS Portal",
        "opfield": ".username",
    },
    "KNACK_DTS_PORTAL_SERVICE_BOT_PASSWORD": {
        "opitem": "Knack DTS Portal",
        "opfield": ".password",
    },
}

with DAG(
    dag_id=f"atd_service_bot_issue_intake_{DEPLOYMENT_ENVIRONMENT}",
    default_args=DEFAULT_ARGS,
    schedule="*/3 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-service-bot", "knack", "github"],
    catchup=False,
) as dag:

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    DockerOperatorWithFallback(
        task_id="dts_sr_to_github",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command="./atd-service-bot/intake.py",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )
