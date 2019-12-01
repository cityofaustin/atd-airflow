from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Imports raw CSV extracts into VZD via Hasura (staging).',
        'depend_on_past'        : False,
        'start_date'            : datetime(2019, 1, 1),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(minutes=5)
}

# We first need to gather the environment variables for this execution
atd_visionzero_cris_envvars=Variable.get("atd_visionzero_cris_staging", deserialize_json=True)
atd_visionzero_cris_volumes=Variable.get("atd_visionzero_cris_volumes", deserialize_json=True)

with DAG('atd_visionzero_hasura_import_staging', default_args=default_args, schedule_interval="0 3 * * *", catchup=False) as dag:

        #
        # Task: clean_up
        # Description: Removes any zip, csv, xml or email files from the tmp directory.
        #
        clean_up = DockerOperator(
                task_id='docker_command_crashes',
                image='atddocker/atd-vz-etl:master',
                api_version='auto',
                auto_remove=True,
                command='sh -c "ls -lha /data && rm -rf /data/* && ls -lha /data && ls -lha /app/tmp && rm -rf /app/tmp/* && ls -lha /app/tmp"',
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                environment=atd_visionzero_cris_volumes,
                volumes=[
                        atd_visionzero_cris_volumes["ATD_VOLUME_DATA"],
                        atd_visionzero_cris_volumes["ATD_VOLUME_TEMP"],
                ],
        )

