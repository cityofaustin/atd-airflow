import os
import pendulum

from airflow.decorators import dag
from airflow.operators.docker_operator import DockerOperator
from utils.slack_operator import task_fail_slack_alert


@dag(
    dag_id="vz-cris-import",
    description="A process which will import the VZ CRIS data into the database on a daily basis using data on the SFTP endpoint",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="America/Chicago"),
    catchup=False,
    tags=["vision-zero", "cris", "repo:atd-vz-data"],
    on_failure_callback=task_fail_slack_alert,
)
def cris_import():

    DockerOperator(
        task_id="run_cris_import",
        environment=dict(os.environ),
        image="atddocker/vz-cris-import:production",
        auto_remove=True,
        tty=True,
        force_pull=True,
    )


cris_import()
