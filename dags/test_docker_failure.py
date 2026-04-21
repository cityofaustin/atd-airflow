from __future__ import annotations

from os import getenv

import pendulum

from airflow.sdk import dag
from utils.docker_operator import DockerOperatorWithFallback

from utils.slack_operator import task_fail_slack_alert, slack_member_ids

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")


@dag(
    dag_id="test_docker_failure",
    schedule=None,
    start_date=pendulum.datetime(2015, 1, 1, tz="America/Chicago"),
    catchup=False,
    tags=["repo:atd-airflow", "slack"],
    default_args={
        "owner": "airflow",
        "retries": 0,
        "execution_timeout": pendulum.duration(minutes=5),
        "on_failure_callback": task_fail_slack_alert,
    },
    doc_md="Throws stacked python exceptions from within a docker container",
)
def test_docker_failure():
    """Test stacked exception handling in Docker container."""

    docker_failure = DockerOperator(
        task_id="docker_failure",
        image="atddocker/atd-airflow:production",
        doc_md="This is an example of task specific documentation",
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

    docker_failure


dag_instance = test_docker_failure()

# Set custom DAG attributes for Slack notifications
if dag_instance:
    dag_instance.byline = "Test stacked exceptions in a docker container"
    dag_instance.icon = ":test_tube:"
