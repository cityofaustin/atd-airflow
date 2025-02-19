import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration, now

from utils.slack_operator import task_fail_slack_alert, slack_member_ids

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

with DAG(
    dag_id=f"test_docker_failure",
    description="Throws a python exception from within a docker container",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    tags=["repo:atd-airflow", "slack"],
    catchup=False,
) as dag:
    dag.byline = f"{slack_member_ids['Frank']} test failure"
    dag.icon = ":test_tube:"

    t1 = DockerOperator(
        task_id="docker_failure",
        image="frankinaustin/pyfail:latest",
        docker_conn_id="docker_default",
        auto_remove=True,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    t1
