from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

from _slack_operators import *

environment_vars_production = Variable.get("atd_visionzero_cris_production", deserialize_json=True)
environment_vars_staging = Variable.get("atd_visionzero_cris_staging", deserialize_json=True)
vzv_data_query_vars = Variable.get("atd_visionzero_vzv_query_production", deserialize_json=True)

default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Exports data from VZD into Socrata (production).',
        'depend_on_past'        : False,
        'start_date'            : datetime(2019, 1, 1),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(minutes=5),
        'on_failure_callback'   : task_fail_slack_alert,
}

with DAG(
        'atd_visionzero_socrata_export_production',
        default_args=default_args,
        schedule_interval="0 9 * * *",
        catchup=False,
        tags=["production", "visionzero"],
) as dag:

        socrata_backup_crashes = BashOperator(
                task_id="socrata_backup_crashes",
                bash_command="sh ~/dags/bash_scripts/vzv_backup_socrata_production.sh",
                env=vzv_data_query_vars
        )

        #
        # Task: docker_command
        # Description: Runs a docker container with CentOS, and waits 30 seconds before being terminated.
        #
        upsert_to_staging = DockerOperator(
                task_id='upsert_to_staging',
                image='atddocker/atd-vz-etl:production',
                api_version='auto',
                auto_remove=True,
                command="/app/process_socrata_export.py",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                environment=environment_vars_staging
        )

        #
        # Task: docker_command
        # Description: Runs a docker container with CentOS, and waits 30 seconds before being terminated.
        #
        # upsert_to_production = DockerOperator(
        #         task_id='upsert_to_production',
        #         image='atddocker/atd-vz-etl:production',
        #         api_version='auto',
        #         auto_remove=True,
        #         command="/app/process_socrata_export.py",
        #         docker_url="tcp://localhost:2376",
        #         network_mode="bridge",
        #         environment=environment_vars_production
        # )

        #
        # Task: recover_on_error
        # Description: We need to recover if the last task failed
        #
        recover_on_error = BashOperator(
                task_id='report_errors',
                trigger_rule='one_failed',
                bash_command='echo "Not Yet Implemented"',
        )

        socrata_backup_crashes >> upsert_to_staging >> recover_on_error
