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
    # query 7 days ago to present. some transactions are not available at endpoint
    # until a few days after they have occurred
    start_date = "{{ (prev_execution_date_success - datetime.timedelta(days=7)).strftime('%Y-%m-%d') if prev_execution_date_success else '2021-12-01'}}"

    t1 = DockerOperator(
        task_id="parking_transaction_history_to_s3",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"python txn_history.py -v --report transactions --env prod --start {start_date}",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t2 = DockerOperator(
        task_id="parking_payment_history_to_s3",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"python txn_history.py -v --report payments --env prod --start {start_date}",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1 >> t2

if __name__ == "__main__":
    dag.cli()
