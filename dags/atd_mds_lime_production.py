from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Gathers MDS data from Lime',
        'depend_on_past'        : False,
        'start_date'            : datetime(2018, 1, 1),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(minutes=5)
}

mds_provider = "lime"
current_time = datetime.now() + timedelta(days=-1, hours=-6)
time_max = f"{current_time.year}-{current_time.month}-{current_time.day}-{(current_time.hour)}"
environment_vars = Variable.get("atd_mds_config_production", deserialize_json=True)
docker_image = 'atddocker/atd-mds-etl:production'

with DAG(f"atd_mds_{mds_provider}_production", default_args=default_args, schedule_interval="15 * * * *", catchup=False) as dag:
        #
        # Task: provider_extract
        # Description: Given a schedule block, the script extracts data from the MDS provider within the schedule's time window
        # then it uploads the data into S3 for further processing.
        #
        t1 = DockerOperator(
                task_id='provider_extract',
                image=docker_image,
                api_version='auto',
                auto_remove=True,
                command=f"./provider_extract.py --provider '{mds_provider}' --time-max '{time_max}' --interval 1",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                environment=environment_vars
        )

        #
        # Task: provider_sync_db
        # Description: Downloads the extracted MDS data from S3, and inserts each trip into a postgres database.
        #
        t2 = DockerOperator(
                task_id='provider_sync_db',
                image=docker_image,
                api_version='auto',
                auto_remove=True,
                command=f"./provider_sync_db.py --provider '{mds_provider}' --time-max '{time_max}' --interval 1",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                environment=environment_vars
        )

        #
        # Task: provider_sync_socrata
        # Description: Downloads the extracted MDS data from S3, and inserts each trip into a postgres database.
        #
        t3 = DockerOperator(
                task_id='provider_sync_socrata',
                image=docker_image,
                api_version='auto',
                auto_remove=True,
                command=f"./provider_sync_socrata.py",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                environment=environment_vars
        )

        t1 >> t2 >> t3
