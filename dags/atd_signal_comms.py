from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from _slack_operators import *

default_args = {
    "owner": "airflow",
    "description": "Ping network devices and publish to S3, then socrata",
    "depend_on_past": False,
    "start_date": datetime(2021, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-signal-comms:production"
env = "prod"
env_vars = Variable.get("atd_signal_comms", deserialize_json=True)

with DAG(
    dag_id="atd_signal_comms",
    default_args=default_args,
    schedule_interval="35 3 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "parking"],
    catchup=False,
) as dag:
    # start_date = "{{ prev_execution_date_success.strftime('%Y-%m-%d') if prev_execution_date_success else '2021-12-01'}}"
    
    t1 = DockerOperator(
        task_id="run_comm_check_cameras",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"python atd-signal-comms/run_comm_check.py camera --env prod",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1

if __name__ == "__main__":
    dag.cli()
