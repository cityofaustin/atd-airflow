from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

from _slack_operators import *

default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Imports raw CSV extracts into VZD via Hasura (production).',
        'depend_on_past'        : False,
        'start_date'            : datetime(2019, 1, 1),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(minutes=5),
        'on_failure_callback'   : task_fail_slack_alert,
        'on_success_callback'   : task_success_slack_alert,
}

# We first need to gather the environment variables for this execution
atd_visionzero_cris_envvars=Variable.get("atd_visionzero_cris_production", deserialize_json=True)

with DAG(
        'atd_visionzero_hasura_geocode_production',
        default_args=default_args,
        schedule_interval="0 8 * * *",
        catchup=False,
        tags=["production", "visionzero"],
) as dag:
        #
        # Task: docker_command_geocode
        # Description: Imports a raw CSV file with crash records into our database via GraphSQL/Hasura.
        #
        t1 = DockerOperator(
                task_id='docker_command_geocode',
                image='atddocker/atd-vz-etl:production',
                api_version='auto',
                auto_remove=True,
                command="/app/process_hasura_geocode.py",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                environment=atd_visionzero_cris_envvars
        )

        #
        # Task: report_errors
        # Description: Reports an error
        #
        t2 = BashOperator(
                task_id='report_errors',
                bash_command='echo "Not Yet Implemented"'
        )

        t1 >> t2
