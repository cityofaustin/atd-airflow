from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

from _slack_operators import *

default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Uploads CR3 files to records that dont have it. (production)',
        'depend_on_past'        : False,
        'start_date'            : datetime(2019, 1, 1),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(minutes=5),
        'on_failure_callback'   : task_fail_slack_alert,
        'on_success_callback'   : task_success_slack_alert,
}

with DAG(
        'atd_visionzero_cr3_production',
        default_args=default_args,
        schedule_interval="*/30 * * * *",
        catchup=False,
        tags=["production", "visionzero"],
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
                environment=Variable.get("atd_visionzero_cris_production", deserialize_json=True)
        )

        #
        # Task: print_hello
        # Description: Prints hello world in the console
        #
        t2 = BashOperator(
                task_id='print_hello',
                bash_command='echo "hello world"'
        )

        t1 >> t2
