import os
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from onepasswordconnectsdk.client import Client, new_client
import onepasswordconnectsdk

ONEPASSWORD_CONNECT_HOST = os.getenv("OP_CONNECT")
ONEPASSWORD_CONNECT_TOKEN = os.getenv("OP_API_TOKEN")
VAULT_ID = os.getenv("OP_VAULT_ID")

default_args = {
    "owner": "airflow",
    "description": "Scrapes trail counter data from the public eco-counter website and publishes it in Socrata",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

one_pass_entry_name = "Ped Data Pub"
docker_image = "atddocker/atd-trail-counters:latest"

client: Client = new_client(ONEPASSWORD_CONNECT_HOST, ONEPASSWORD_CONNECT_TOKEN)

# Gathering one pass entries into a dictionary
vault_data = client.get_item_by_title(one_pass_entry_name, VAULT_ID)

# Parsing object returned from vault into dictionary
env_vars = {}
for entry in vault_data.fields:
    if entry.value:
        env_vars[entry.label] = entry.value

with DAG(
    dag_id="atd_trail_counters",
    default_args=default_args,
    schedule_interval="00 8 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["atd-trail-counter-data", "Socrata"],
    catchup=False,
) as dag:
    t1 = DockerOperator(
        task_id="trail_counter_data_publish",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="python counter_data.py",
        environment=env_vars,
        tty=True,
    )

    t1

if __name__ == "__main__":
    dag.cli()
