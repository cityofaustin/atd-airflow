import os
from datetime import datetime, timedelta


from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from onepasswordconnectsdk.client import Client, new_client
import onepasswordconnectsdk

ONEPASSWORD_CONNECT_HOST = os.getenv("OP_CONNECT")
ONEPASSWORD_CONNECT_TOKEN = os.getenv("OP_API_TOKEN")
VAULT_ID = os.getenv("OP_VAULT_ID")

# from _slack_operators import *

default_args = {
    "owner": "airflow",
    "description": "Fetch new DTS service requests and create Github issues",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-service-bot:production"
app_name = "dts-portal"

REQUIRED_SECRETS = {
    "KNACK_APP_ID": {
        "opitem": "Staging Service Bot Knack App ID",
        "opfield": ".password",
        "opvault": VAULT_ID,
    },
    "KNACK_API_KEY": {
        "opitem": "Staging Service Bot Knack API Key",
        "opfield": ".password",
        "opvault": VAULT_ID,
    },
    "GITHUB_ACCESS_TOKEN": {
        "opitem": "Service Bot Github Access Token",
        "opfield": ".password",
        "opvault": VAULT_ID,
    },
    "KNACK_DTS_PORTAL_SERVICE_BOT_USERNAME": {
        "opitem": "Knack DTS Portal Service Bot Login Email",
        "opfield": ".password",
        "opvault": VAULT_ID,
    },
    "KNACK_DTS_PORTAL_SERVICE_BOT_PASSWORD": {
        "opitem": "Knack DTS Portal Service Bot Login Password",
        "opfield": ".password",
        "opvault": VAULT_ID,
    },
}

client: Client = new_client(ONEPASSWORD_CONNECT_HOST, ONEPASSWORD_CONNECT_TOKEN)
env_vars = onepasswordconnectsdk.load_dict(client, REQUIRED_SECRETS)


with DAG(
    dag_id="atd_service_bot_issue_intake_staging",
    default_args=default_args,
    schedule_interval="* * * * *",
    dagrun_timeout=timedelta(minutes=5),
    tags=["staging", "knack", "atd-service-bot", "github"],
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
