from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

from _slack_operators import *

default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Uploads CR3 files to records that dont have it. (staging)',
        'depend_on_past'        : False,
        'start_date'            : datetime(2019, 1, 1),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(minutes=5),
        'on_failure_callback'   : task_fail_slack_alert,
}

with DAG(
        'atd_visionzero_cr3_staging',
        default_args=default_args,
        schedule_interval="*/30 * * * *",
        catchup=False,
        tags=["staging", "visionzero"],
) as dag:
        #
        # Task: docker_command
        # Description: Runs a docker container with CentOS, and waits 30 seconds before being terminated.
        #
        t1 = DockerOperator(
                task_id='docker_command',
                image='atddocker/atd-vz-etl:master',
                api_version='auto',
                auto_remove=True,
                command="/app/process_cris_cr3.py",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                environment=Variable.get("atd_visionzero_cris_staging", deserialize_json=True),
                execution_timeout=timedelta(minutes=60),
        )

        t1
