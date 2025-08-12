from os import getenv

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration, now

from utils.slack_operator import task_fail_slack_alert, slack_member_ids

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

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
    description="Throws stacked python exceptions from within a docker container",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    tags=["repo:atd-airflow", "slack"],
    catchup=False,
) as dag:
    dag.byline = f"Test stacked exceptions in a docker container"
    dag.icon = ":test_tube:"

    t1 = DockerOperator(
        task_id="docker_failure",
        image="atddocker/atd-airflow:production",
        command=[
            "python",
            "-c",
            """
import sys
import traceback

def outer_function():
    try:
        middle_function()
    except Exception as e:
        print("Caught exception in outer_function: " + str(e))
        raise RuntimeError("Failed in outer function") from e

def middle_function():
    try:
        inner_function()
    except Exception as e:
        print("Caught exception in middle_function: " + str(e))
        raise ValueError("Failed in middle function") from e

def inner_function():
    print("About to raise ConnectionError")
    raise ConnectionError("Connection failed")

if __name__ == "__main__":
    try:
        outer_function()
    except Exception as e:
        print("Final exception caught at top level:")
        traceback.print_exc()
        sys.exit(1)
""",
        ],
        docker_conn_id="docker_default",
        auto_remove="force",
        tty=True,
        mount_tmp_dir=False,
    )

    t1
