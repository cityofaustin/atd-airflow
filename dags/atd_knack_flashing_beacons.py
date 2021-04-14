from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from _slack_operators import task_fail_slack_alert

default_args = {
    "owner": "airflow",
    "description": "Load flashing beacons (view_1563) records from Knack to Postgrest to AGOL and Socrata",  # noqa:E501
    "depend_on_past": False,
    "start_date": datetime(2021, 4, 14),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-knack-services:production"

# command args
script_task_1 = "records_to_postgrest"
script_task_2 = "records_to_socrata"
script_task_3 = "records_to_agol"
app_name = "data-tracker"
container = "view_1563"
env = "prod"

# assemble env vars
env_vars = Variable.get("atd_knack_services_postgrest", deserialize_json=True)
atd_knack_auth = Variable.get("atd_knack_auth", deserialize_json=True)
env_vars["KNACK_APP_ID"] = atd_knack_auth[app_name][env]["app_id"]
env_vars["KNACK_API_KEY"] = atd_knack_auth[app_name][env]["api_key"]
env_vars["SOCRATA_API_KEY_ID"] = Variable.get("atd_service_bot_socrata_api_key_id")
env_vars["SOCRATA_API_KEY_SECRET"] = Variable.get(
    "atd_service_bot_socrata_api_key_secret"
)
env_vars["SOCRATA_APP_TOKEN"] = Variable.get("atd_service_bot_socrata_app_token")
env_vars["AGOL_USERNAME"] = Variable.get("agol_username")
env_vars["AGOL_PASSWORD"] = Variable.get("agol_password")

with DAG(
    dag_id="atd_knack_flashing_beacons",
    default_args=default_args,
    schedule_interval="15 3 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "knack"],
    catchup=False,
) as dag:
    # completely replace data on 15th day of every month
    # this is a failsafe catch records that may have been missed via incremental loading
    date_filter = "{{ '1970-01-01' if ds.endswith('15') else prev_execution_date_success or '1970-01-01' }}"  # noqa:E501
    t1 = DockerOperator(
        task_id="atd_knack_flashing_beacons_to_postgrest",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_1}.py -a {app_name} -c {container} -d "{date_filter}"',  # noqa:E501
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t2 = DockerOperator(
        task_id="atd_knack_flashing_beacons_to_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_2}.py -a {app_name} -c {container} -d "{date_filter}"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t3 = DockerOperator(
        task_id="atd_knack_flashing_beacons_to_agol",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"./atd-knack-services/services/{script_task_3}.py -a {app_name} -c {container} -d '{date_filter}'",  # noqa:E501
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1 >> [t2, t3]

if __name__ == "__main__":
    dag.cli()
