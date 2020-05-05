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
        'retry_delay'           : timedelta(minutes=5)
        'on_failure_callback'   : task_fail_slack_alert,
        'on_success_callback'   : task_success_slack_alert,
}

# We first need to gather the environment variables for this execution
atd_visionzero_cris_envvars=Variable.get("atd_visionzero_cris_production", deserialize_json=True)
atd_visionzero_cris_volumes=Variable.get("atd_visionzero_cris_volumes", deserialize_json=True)

with DAG(
        'atd_visionzero_download_production',
        default_args=default_args,
        schedule_interval="0 7 * * *",
        catchup=False,
        tags=["production", "visionzero"],
) as dag:
        #
        # Task: docker_command_aws_download
        # Description: Copies raw csv files from S3
        #
        aws_copy = DockerOperator(
                task_id='docker_command_aws_download',
                image='atddocker/atd-vz-etl:production',
                api_version='auto',
                auto_remove=True,
                command="sh -c \"aws s3 cp s3://$ATD_CRIS_DOWNLOAD_CSV_BUCKET /data --recursive\"",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                environment=atd_visionzero_cris_envvars,
                volumes=[
                        atd_visionzero_cris_volumes["ATD_VOLUME_DATA"],
                        atd_visionzero_cris_volumes["ATD_VOLUME_TEMP"],
                ],
        )

        #
        # Task: docker_command_clean_up_s3
        # Description: Moves CSV files to another location for backup.
        #
        clean_up = DockerOperator(
                task_id='docker_command_clean_up_s3',
                image='atddocker/atd-vz-etl:production',
                api_version='auto',
                auto_remove=True,
                command="sh -c \"aws s3 mv s3://$ATD_CRIS_DOWNLOAD_CSV_BUCKET/ s3://$ATD_CRIS_IMPORT_CSV_BUCKET/ --recursive\"",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                environment=atd_visionzero_cris_envvars,
                volumes=[
                        atd_visionzero_cris_volumes["ATD_VOLUME_DATA"],
                        atd_visionzero_cris_volumes["ATD_VOLUME_TEMP"],
                ],
        )

        #
        # Schedule the tasks in order
        #
        aws_copy >> clean_up
