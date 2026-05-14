from __future__ import annotations

import pendulum

from airflow.sdk import dag

from utils.docker_operator import DockerOperatorWithFallback


@dag(
    dag_id="test_docker_operator_wrapper",
    schedule=None,
    start_date=pendulum.datetime(2015, 1, 1, tz="America/Chicago"),
    catchup=False,
    tags=["repo:atd-airflow"],
    default_args={
        "owner": "airflow",
        "retries": 0,
        "execution_timeout": pendulum.duration(minutes=5),
    },
    doc_md="Hello world DAG using DockerOperatorWithFallback with the python3 image",
)
def test_docker_operator_wrapper():
    DockerOperatorWithFallback(
        force_pull=True,
        task_id="hello_world",
        image="python:3",
        command=["python3", "-c", "print('hello world')"],
        docker_conn_id="docker_default",
        auto_remove="force",
        tty=True,
        mount_tmp_dir=False,
    )


test_docker_operator_wrapper()
