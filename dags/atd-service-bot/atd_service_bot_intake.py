import os
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from onepasswordconnectsdk.client import Client, new_client
import onepasswordconnectsdk

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
ONEPASSWORD_CONNECT_HOST = os.getenv("OP_CONNECT")
ONEPASSWORD_CONNECT_TOKEN = os.getenv("OP_API_TOKEN")
VAULT_ID = os.getenv("OP_VAULT_ID")

default_args = {
    "owner": "airflow",
    "description": "Fetch new DTS service requests and create Github issues",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

docker_image = "atddocker/atd-service-bot:production"

REQUIRED_SECRETS = {
    "KNACK_APP_ID": {
        "opitem": "Service Bot",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.knackAppId",
        "opvault": VAULT_ID,
    },
    "KNACK_API_KEY": {
        "opitem": "Service Bot",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.knackApiKey",
        "opvault": VAULT_ID,
    },
    "GITHUB_ACCESS_TOKEN": {
        "opitem": "Service Bot",
        "opfield": "shared.githubAccessToken",
        "opvault": VAULT_ID,
    },
    "KNACK_DTS_PORTAL_SERVICE_BOT_USERNAME": {
        "opitem": "Service Bot",
        "opfield": "shared.dtsPortalLoginEmail",
        "opvault": VAULT_ID,
    },
    "KNACK_DTS_PORTAL_SERVICE_BOT_PASSWORD": {
        "opitem": "Service Bot",
        "opfield": "shared.dtsPortalLoginPassword",
        "opvault": VAULT_ID,
    },
}

client: Client = new_client(ONEPASSWORD_CONNECT_HOST, ONEPASSWORD_CONNECT_TOKEN)
env_vars = onepasswordconnectsdk.load_dict(client, REQUIRED_SECRETS)


with DAG(
    dag_id=f"atd_service_bot_issue_intake_{DEPLOYMENT_ENVIRONMENT}",
    default_args=default_args,
    schedule_interval="*/3 * * * *",
    dagrun_timeout=timedelta(minutes=5),
    tags=[DEPLOYMENT_ENVIRONMENT, "knack", "atd-service-bot", "github"],
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
    )

    t1
