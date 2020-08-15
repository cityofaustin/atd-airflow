from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

from _slack_operators import *

environment_vars_production = Variable.get("atd_visionzero_cris_production", deserialize_json=True)
environment_vars_staging = Variable.get("atd_visionzero_cris_staging", deserialize_json=True)
vzv_data_query_vars = Variable.get("atd_visionzero_vzv_query_staging", deserialize_json=True)

default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Exports data from VZD into Socrata (staging).',
        'depend_on_past'        : False,
        'start_date'            : datetime(2019, 1, 1),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 0,
        'retry_delay'           : timedelta(minutes=5),
        # 'on_failure_callback'   : task_fail_slack_alert,
}

with DAG(
        'atd_visionzero_socrata_export_staging',
        default_args=default_args,
        schedule_interval="0 9 * * *",
        catchup=False,
        tags=["staging", "visionzero"],
) as dag:

        #
        # Downloads the entire datasets from Socrata and uploads to S3
        #
        socrata_backup_crashes = BashOperator(
                task_id="socrata_backup_crashes",
                # Notice this line has a space ('vzv_backup_socrata.sh ') as the last character
                # that is intended since somehow not keeping the space is breaking the template library.
                bash_command="~/dags/bash_scripts/vzv_backup_socrata.sh ",
                env={**vzv_data_query_vars, **environment_vars_staging}
        )

        #
        # Task: upsert_to_socrata
        # Description: Downloads data from VZD and attempts insertion to Socrata
        #
        upsert_to_socrata = DockerOperator(
                task_id='upsert_to_socrata',
                image='atddocker/atd-vz-etl:staging',
                api_version='auto',
                auto_remove=True,
                command="/app/process_socrata_export.py",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                trigger_rule='none_failed',
                environment=environment_vars_staging,
        )

        # Executes if the last task fails
        recover_on_error = BashOperator(
                task_id='recover_on_error',
                bash_command="~/dags/bash_scripts/vzv_restore_socrata.sh ",
                trigger_rule='one_failed',
                env={**vzv_data_query_vars, **environment_vars_staging},
                # on_success_callback=task_success_slack_alert
        )

        socrata_backup_crashes >> upsert_to_socrata >> recover_on_error
