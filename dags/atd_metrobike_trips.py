from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from _slack_operators import *

default_args = {
    "owner": "airflow",
    "description": "Fetch metrobike trip data from dropbox and publish to Socrata",
    "depend_on_past": False,
    "start_date": datetime(2020, 12, 31),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-metrobike:production"

# assemble env vars
env_vars = Variable.get("atd_metrobike", deserialize_json=True)

with DAG(
    dag_id="atd_metrobike_trips",
    default_args=default_args,
    schedule_interval="33 1 * * 1",  # runs weekly at 1:33am Monday
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "metrobike"],
    catchup=False,
) as dag:
    t1 = DockerOperator(
        task_id="atd_metrobike_trips_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="python publish_trips.py",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1

if __name__ == "__main__":
    dag.cli()
