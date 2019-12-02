from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Imports raw CSV extracts into VZD via Hasura (production).',
        'depend_on_past'        : False,
        'start_date'            : datetime(2019, 1, 1),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(minutes=5)
}

# We first need to gather the environment variables for this execution
atd_visionzero_cris_envvars=Variable.get("atd_visionzero_cris_production", deserialize_json=True)
atd_visionzero_cris_volumes=Variable.get("atd_visionzero_cris_volumes", deserialize_json=True)

with DAG('atd_visionzero_hasura_import_production', default_args=default_args, schedule_interval="0 3 * * *", catchup=False) as dag:
        #
        # Task: docker_command_crashes
        # Description: Imports a raw CSV file with crash records into our database via GraphSQL/Hasura.
        #
        crash = DockerOperator(
                task_id='docker_command_crashes',
                image='atddocker/atd-vz-etl:production',
                api_version='auto',
                auto_remove=True,
                command="/app/process_hasura_import.py crash",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                environment=atd_visionzero_cris_envvars,
                volumes=[
                        atd_visionzero_cris_volumes["ATD_VOLUME_DATA"],
                        atd_visionzero_cris_volumes["ATD_VOLUME_TEMP"],
                ],
        )

        #
        # Task: docker_command_unit
        # Description: Imports a raw CSV file with unit records into our database via GraphSQL/Hasura.
        #
        unit = DockerOperator(
                task_id='docker_command_unit',
                image='atddocker/atd-vz-etl:production',
                api_version='auto',
                auto_remove=True,
                command="/app/process_hasura_import.py unit",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                environment=atd_visionzero_cris_envvars,
                volumes=[
                        atd_visionzero_cris_volumes["ATD_VOLUME_DATA"],
                        atd_visionzero_cris_volumes["ATD_VOLUME_TEMP"],
                ],
        )

        #
        # Task: docker_command_person
        # Description: Imports a raw CSV file with person records into our database via GraphSQL/Hasura.
        #
        person = DockerOperator(
                task_id='docker_command_person',
                image='atddocker/atd-vz-etl:production',
                api_version='auto',
                auto_remove=True,
                command="/app/process_hasura_import.py person",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                environment=atd_visionzero_cris_envvars,
                volumes=[
                        atd_visionzero_cris_volumes["ATD_VOLUME_DATA"],
                        atd_visionzero_cris_volumes["ATD_VOLUME_TEMP"],
                ],
        )

        #
        # Task: docker_command_primaryperson
        # Description: Imports a raw CSV file with primary person records into our database via GraphSQL/Hasura.
        #
        primaryperson = DockerOperator(
                task_id='docker_command_primaryperson',
                image='atddocker/atd-vz-etl:production',
                api_version='auto',
                auto_remove=True,
                command="/app/process_hasura_import.py primaryperson",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                environment=atd_visionzero_cris_envvars,
                volumes=[
                        atd_visionzero_cris_volumes["ATD_VOLUME_DATA"],
                        atd_visionzero_cris_volumes["ATD_VOLUME_TEMP"],
                ],
        )

        #
        # Task: docker_command_charges
        # Description: Imports a raw CSV file with charges records into our database via GraphSQL/Hasura.
        #
        charges = DockerOperator(
                task_id='docker_command_charges',
                image='atddocker/atd-vz-etl:production',
                api_version='auto',
                auto_remove=True,
                command="/app/process_hasura_import.py charges",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                environment=atd_visionzero_cris_envvars,
                volumes=[
                        atd_visionzero_cris_volumes["ATD_VOLUME_DATA"],
                        atd_visionzero_cris_volumes["ATD_VOLUME_TEMP"],
                ],
        )

        #
        # Task: docker_command_aws_copy
        # Description: Copies raw csv files to S3 for backup, history and analysis.
        #
        aws_copy = DockerOperator(
                task_id='docker_command_aws_copy',
                image='atddocker/atd-vz-etl:production',
                api_version='auto',
                auto_remove=True,
                command="sh -c \"aws s3 cp /data s3://$ATD_CRIS_IMPORT_CSV_BUCKET --recursive --exclude '*.xml'\"",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                environment=atd_visionzero_cris_envvars,
                volumes=[
                        atd_visionzero_cris_volumes["ATD_VOLUME_DATA"],
                        atd_visionzero_cris_volumes["ATD_VOLUME_TEMP"],
                ],
        )

        #
        # Task: clean_up
        # Description: Removes any zip, csv, xml or email files from the tmp directory.
        #
        clean_up = DockerOperator(
                task_id='docker_command_clean_up',
                image='atddocker/atd-vz-etl:production',
                api_version='auto',
                auto_remove=True,
                command='sh -c "ls -lha /data && rm -rf /data/* && ls -lha /data && ls -lha /app/tmp && rm -rf /app/tmp/* && ls -lha /app/tmp"',
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
        crash >> unit >> person >> primaryperson >> charges>> aws_copy >> clean_up
