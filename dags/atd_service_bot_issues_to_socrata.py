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
    "description": "Publish atd-data-tech Github issues to Socrata",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

docker_image = "atddocker/atd-service-bot:production"

REQUIRED_SECRETS = {
    "GITHUB_ACCESS_TOKEN": {
        "opitem": "Github Access Token Service Bot",
        "opfield": ".password",
        "opvault": VAULT_ID,
    },
    "ZENHUB_ACCESS_TOKEN": {
        "opitem": "Zenhub Access Token",
        "opfield": ".password",
        "opvault": VAULT_ID,
    },
    "SOCRATA_API_KEY_ID": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "add more.apiKeyId",
        "opvault": VAULT_ID,
    },
    "SOCRATA_API_KEY_SECRET": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "add more.apiKeySecret",
        "opvault": VAULT_ID,
    },
    "SOCRATA_APP_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "add more.appToken",
        "opvault": VAULT_ID,
    },
    "SOCRATA_RESOURCE_ID": {
        "opitem": "Service Bot",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.socrataResourceId",
        "opvault": VAULT_ID,
    },
    "SOCRATA_ENDPOINT": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "add more.endpoint",
        "opvault": VAULT_ID,
    },
}

client: Client = new_client(ONEPASSWORD_CONNECT_HOST, ONEPASSWORD_CONNECT_TOKEN)
env_vars = onepasswordconnectsdk.load_dict(client, REQUIRED_SECRETS)

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
        force_pull=True,
    )

    t1
