from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Use of the DockerOperator',
        'depend_on_past'        : False,
        'start_date'            : datetime(2018, 1, 3),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(minutes=5)
}

with DAG('atd_etl_template', default_args=default_args, schedule_interval="5 * * * *", catchup=False) as dag:
        #
        # Task: print_current_date
        # Description: It prints the current date in the command line
        #
        t1 = BashOperator(
                task_id='print_current_date',
                bash_command='date'
        )

        #
        # Task: docker_command
        # Description: Runs a docker container with CentOS, and waits 30 seconds before being terminated.
        #
        t2 = DockerOperator(
                task_id='docker_command',
                image='alpine:latest',
                api_version='auto',
                auto_remove=True,
                command="/bin/sleep 30",
                docker_url="tcp://localhost:2376",
                network_mode="bridge",
                # Change these in Airflow UI -> Admin -> Variables
                environment = Variable.get("atd_etl_template", deserialize_json=True)
        )

        #
        # Task: print_hello
        # Description: Prints hello world in the console
        #
        t3 = BashOperator(
                task_id='print_hello',
                bash_command='echo "hello world"'
        )

        t1 >> t3 >> t2