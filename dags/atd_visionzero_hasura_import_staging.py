from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Creates a new CRIS data extract (staging).',
        'depend_on_past'        : False,
        'start_date'            : datetime(2019, 1, 1),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 0,
        'retry_delay'           : timedelta(minutes=5)
}

# We first need to gather the environment variables for this execution
atd_visionzero_cris_envvars=Variable.get("atd_visionzero_cris_staging", deserialize_json=True)
atd_visionzero_cris_volumes=Variable.get("atd_visionzero_cris_volumes", deserialize_json=True)

with DAG('atd_visionzero_cris_request_staging', default_args=default_args, schedule_interval="0 3 * * *", catchup=False) as dag:

        #
        # Task: docker_command_crashes
        # Description: Reads crash records CSV files and inserts to database via GraphQL/Hasura.
        #
        t1 = DockerOperator(
                task_id='docker_command_crashes',
                image='atddocker/atd-vz-etl:master',
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

        # #
        # # Task: docker_command_units
        # # Description: Reads unit records CSV files and inserts to database via GraphQL/Hasura.
        # #
        # t2 = DockerOperator(
        #         task_id='docker_command_units',
        #         image='atddocker/atd-vz-etl:master',
        #         api_version='auto',
        #         auto_remove=True,
        #         command="/app/process_hasura_import.py unit",
        #         docker_url="tcp://localhost:2376",
        #         network_mode="bridge",
        #         environment=atd_visionzero_cris_envvars
        # )
        #
        # #
        # # Task: docker_command_units
        # # Description: Reads person records CSV files and inserts to database via GraphQL/Hasura.
        # #
        # t3 = DockerOperator(
        #         task_id='docker_command_person',
        #         image='atddocker/atd-vz-etl:master',
        #         api_version='auto',
        #         auto_remove=True,
        #         command="/app/process_hasura_import.py person",
        #         docker_url="tcp://localhost:2376",
        #         network_mode="bridge",
        #         environment=atd_visionzero_cris_envvars
        # )
        #
        # #
        # # Task: docker_command_units
        # # Description: Reads primary person records CSV files and inserts to database via GraphQL/Hasura.
        # #
        # t4 = DockerOperator(
        #         task_id='docker_command_primary_person',
        #         image='atddocker/atd-vz-etl:master',
        #         api_version='auto',
        #         auto_remove=True,
        #         command="/app/process_hasura_import.py primaryperson",
        #         docker_url="tcp://localhost:2376",
        #         network_mode="bridge",
        #         environment=atd_visionzero_cris_envvars
        # )
        #
        # #
        # # Task: docker_command_units
        # # Description: Reads charges records CSV files and inserts to database via GraphQL/Hasura.
        # #
        # t5 = DockerOperator(
        #         task_id='docker_command_charges',
        #         image='atddocker/atd-vz-etl:master',
        #         api_version='auto',
        #         auto_remove=True,
        #         command="/app/process_hasura_import.py charges",
        #         docker_url="tcp://localhost:2376",
        #         network_mode="bridge",
        #         environment=atd_visionzero_cris_envvars
        # )

        #
        # Task: upload_aws
        # Description: Uploads CSV files to an S3 Bucket for backup
        #
        t6 = BashOperator(
                task_id='upload_aws',
                bash_command='echo "Not Yet Implemented"'
        )

        #
        # Task: clean_up
        # Description: Removes any zip, xml, csv files from the local server.
        #
        t7 = BashOperator(
                task_id='bash_clean_up',
                bash_command='echo "Not Yet Implemented"'
        )

        #
        # We now set up the tasks in the right order
        #
        #t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
        t1 >> t6 >> t7
