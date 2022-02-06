from copy import deepcopy
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from _slack_operators import task_fail_slack_alert

default_args = {
    "owner": "airflow",
    "description": "Append inventory item counts in the Data Tracker to Socrata open dataset",
    "depend_on_past": False,
    "start_date": datetime(2020, 9, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-knack-services:production"
app_name = "data-tracker"
container = "view_2863"
env = "prod"

# assemble env vars
env_vars = Variable.get("atd_knack_services_postgrest", deserialize_json=True)
atd_knack_auth = Variable.get("atd_knack_auth", deserialize_json=True)
env_vars["SOCRATA_API_KEY_ID"] = Variable.get("atd_service_bot_socrata_api_key_id")
env_vars["SOCRATA_API_KEY_SECRET"] = Variable.get(
    "atd_service_bot_socrata_api_key_secret"
)
env_vars["KNACK_APP_ID"] = atd_knack_auth[app_name][env]["app_id"]
env_vars["KNACK_API_KEY"] = atd_knack_auth[app_name][env]["api_key"]

with DAG(
    dag_id="atd_knack_inventory_items_nightly_snapshot",
    default_args=default_args,
    schedule_interval="13 4 * * *",  # at 4:13am UTC
    dagrun_timeout=timedelta(minutes=300),
    tags=["production", "knack"],
    catchup=False,
) as dag:
    """
    Push Data Tracker inventory items to PostgREST. We always push all records (by setting
    data to 1970-01-01) because it is required to create a snapshot of the
    inventory quantities on-hand.
    """
    t1 = DockerOperator(
        task_id="atd_knack_data_tracker_inventory_items_to_postgrest",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/records_to_postgrest.py -a {app_name} -c {container} -d "1970-01-01"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )
    """
    Append Data Tracker inventory items snapshot to Socrata. This creates a daily running
    on-hand quantity report.
    See: https://github.com/cityofaustin/atd-data-tech/issues/7581
    """
    t2 = DockerOperator(
        task_id="atd_knack_data_tracker_inventory_items_daily_snapshot_to_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/records_to_socrata.py -a {app_name} -c {container} -d "1970-01-01"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1 >> t2

if __name__ == "__main__":
    dag.cli()
