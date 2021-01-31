from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from _slack_operators import task_fail_slack_alert

default_args = {
    "owner": "airflow",
    "description": "Update signals in the VZA app from Data Tracker",
    "depend_on_past": False,
    "start_date": datetime(2020, 9, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-knack-services:production"
app_name_src = "data-tracker"
app_name_dest = "vza"
container_src = "view_197"
container_dest = "view_567"
env = "prod"

# assemble env vars
env_vars = Variable.get("atd_knack_services_postgrest", deserialize_json=True)
atd_knack_auth = Variable.get("atd_knack_auth", deserialize_json=True)

with DAG(
    dag_id="atd_knack_data_tracker_signals_to_vza",
    default_args=default_args,
    schedule_interval="* 6 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "knack"],
    catchup=False,
) as dag:
    """
    Although this DAG relies on Signals from the data tracker (view_197), the Knack >
    Postgres ETL is managed in a separate, pre-existing dag.
    """
    date = "{{ prev_execution_date_success or '1970-01-01' }}"
    env_vars["KNACK_APP_ID"] = atd_knack_auth[app_name_dest][env]["app_id"]
    env_vars["KNACK_API_KEY"] = atd_knack_auth[app_name_dest][env]["api_key"]

    t1 = DockerOperator(
        task_id="atd_knack_vza_signals_to_postgrest",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/records_to_postgrest.py -a {app_name_dest} -c {container_dest} -d "{date}"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    env_vars["KNACK_APP_ID_SRC"] = atd_knack_auth[app_name_src][env]["app_id"]
    env_vars["KNACK_APP_ID_DEST"] = atd_knack_auth[app_name_dest][env]["app_id"]
    env_vars["KNACK_API_KEY_DEST"] = atd_knack_auth[app_name_dest][env]["api_key"]

    t2 = DockerOperator(
        task_id="atd_knack_update_vza_signals_from_vza",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/records_to_knack.py -a {app_name_src} -c {container_src} -d "{date}" -dest {app_name_dest}',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1 >> t2

if __name__ == "__main__":
    dag.cli()
