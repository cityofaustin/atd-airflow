from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from _slack_operators import *

default_args = {
    "owner": "airflow",
    "description": "Publish sign work order specifications to Postgres, AGOL",
    "depend_on_past": False,
    "start_date": datetime(2021, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-parking-data-meters:production"
env = "prod"

env_vars = Variable.get("atd_parking_data_meters", deserialize_json=True)

with DAG(
    dag_id="atd_parking_data_txn_history",
    default_args=default_args,
    schedule_interval="35 3 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "parking"],
    catchup=False,
) as dag:
    start_date = "{{ prev_execution_date_success }}"
    command = f"python txn_history.py --env prod"
    command = f"{command} --start {start_date}" if start_date != "None" else command

    t1 = DockerOperator(
        task_id="parking_transaction_history_to_s3",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=command,
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1

if __name__ == "__main__":
    dag.cli()
