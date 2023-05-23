import os
from datetime import datetime, timedelta


from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from onepasswordconnectsdk.client import Client, new_client
import onepasswordconnectsdk

# from _slack_operators import task_fail_slack_alert

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
    # "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-service-bot:production"

REQUIRED_SECRETS = {
    "GITHUB_ACCESS_TOKEN": {
        "opitem": "Service Bot",
        "opfield": "shared.githubAccessToken",
        "opvault": VAULT_ID,
    },
    "ZENHUB_ACCESS_TOKEN": {
        "opitem": "Service Bot",
        "opfield": "shared.zenhubAccessToken",
        "opvault": VAULT_ID,
    },
    "SOCRATA_API_KEY_ID": {
        "opitem": "Service Bot",
        "opfield": "shared.socrataApiKeyID",
        "opvault": VAULT_ID,
    },
    "SOCRATA_API_KEY_SECRET": {
        "opitem": "Service Bot",
        "opfield": "shared.socrataApiKeySecret",
        "opvault": VAULT_ID,
    },
    "SOCRATA_APP_TOKEN": {
        "opitem": "Service Bot",
        "opfield": "shared.socrataAppToken",
        "opvault": VAULT_ID,
    },
    "SOCRATA_RESOURCE_ID": {
        "opitem": "Service Bot",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.socrataResourceId",
        "opvault": VAULT_ID,
    },
    "SOCRATA_ENDPOINT": {
        "opitem": "Service Bot",
        "opfield": f"shared.socrataEndpoint",
        "opvault": VAULT_ID,
    },
}

client: Client = new_client(ONEPASSWORD_CONNECT_HOST, ONEPASSWORD_CONNECT_TOKEN)
env_vars = onepasswordconnectsdk.load_dict(client, REQUIRED_SECRETS)

with DAG(
    dag_id="atd_service_bot_github_to_socrata_production",
    default_args=default_args,
    schedule_interval="21 5 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=[DEPLOYMENT_ENVIRONMENT, "socrata", "atd-service-bot", "github"],
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

if __name__ == "__main__":
    dag.cli()
