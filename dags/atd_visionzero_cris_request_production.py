from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

from _slack_operators import *

default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Creates a new CRIS data extract (production).',
        'depend_on_past'        : False,
        'start_date'            : datetime(2019, 1, 1),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(minutes=5),
        'on_failure_callback'   : task_fail_slack_alert,
}

with DAG(
        'atd_visionzero_cris_request_production',
        default_args=default_args,
        schedule_interval="0 6 * * *",
        catchup=False,
        tags=["production", "visionzero"],
) as dag:

        #
        # Task: docker_command
        # Description: Runs a docker container with CentOS, and waits 30 seconds before being terminated.
        #
        t1 = DockerOperator(
                task_id='docker_command',
                image='atddocker/atd-vz-etl:production',
                api_version='auto',
                auto_remove=True,
                command="/app/process_cris_request.py",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                environment=Variable.get("atd_visionzero_cris_production", deserialize_json=True)
        )

        #
        # Task: report_errors
        # Description: Sends a message to slack to report any errors.
        #
        t2 = BashOperator(
                task_id='report_errors',
                bash_command='echo "Not Yet Implemented"'
        )

        t1 >> t2
