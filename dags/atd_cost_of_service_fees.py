from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from _slack_operators import *

default_args = {
    "owner": "airflow",
    "description": "Fetch cost of service fees fom amanda publish to ROW knack app",
    "depend_on_past": False,
    "start_date": datetime(2020, 12, 31),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-cost-of-service:production"

# assemble env vars
env_vars = Variable.get("atd_cost_of_service", deserialize_json=True)

with DAG(
    dag_id="atd_cost_of_service_fees_to_knack",
    default_args=default_args,
    schedule_interval="7 4 * * *",  # daily at 4:07 am UTC
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "amanda", "knack"],
    catchup=False,
) as dag:
    t1 = DockerOperator(
        task_id="atd_cost_of_service_fees_to_knack",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="python knack_load_fees.py",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1

if __name__ == "__main__":
    dag.cli()
