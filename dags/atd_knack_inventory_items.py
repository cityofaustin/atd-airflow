from copy import deepcopy
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from _slack_operators import task_fail_slack_alert

default_args = {
    "owner": "airflow",
    "description": "Update inventory items in the Data Tracker from the Finance & Purchassing system",
    "depend_on_past": False,
    "start_date": datetime(2020, 9, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-knack-services:production"
app_name_src = "finance-purchasing"
app_name_dest = "data-tracker"
container_dest = "view_2863"
container_src = "view_788"
env = "prod"

# assemble env vars
env_vars = Variable.get("atd_knack_services_postgrest", deserialize_json=True)
atd_knack_auth = Variable.get("atd_knack_auth", deserialize_json=True)

with DAG(
    dag_id="atd_knack_inventory_items_finance_to_data_tracker",
    default_args=default_args,
    schedule_interval="3 */6 * * *",  # At minute 3 past every 6th hour
    dagrun_timeout=timedelta(minutes=300),
    tags=["production", "knack"],
    catchup=False,
) as dag:

    date = "{{ prev_execution_date_success or '1970-01-01' }}"
    """
    Push Finance & Purchasing inventory items to PostgREST
    """
    env_vars_t1 = deepcopy(env_vars)
    env_vars_t1["KNACK_APP_ID"] = atd_knack_auth[app_name_src][env]["app_id"]
    env_vars_t1["KNACK_API_KEY"] = atd_knack_auth[app_name_src][env]["api_key"]

    t1 = DockerOperator(
        task_id="atd_knack_finance_inventory_items_to_postgrest",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/records_to_postgrest.py -a {app_name_src} -c {container_src} -d "{date}"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars_t1,
        tty=True,
    )
    """
    Push Data Tracker inventory items to PostgREST. We always push all records (by setting
    data to 1970-01-01) because it is required for task #4—to create a snapshot of the
    inventory quantities on-hand.
    """
    env_vars_t2 = deepcopy(env_vars)
    env_vars_t2["KNACK_APP_ID"] = atd_knack_auth[app_name_dest][env]["app_id"]
    env_vars_t2["KNACK_API_KEY"] = atd_knack_auth[app_name_dest][env]["api_key"]

    t2 = DockerOperator(
        task_id="atd_knack_data_tracker_inventory_items_to_postgrest",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/records_to_postgrest.py -a {app_name_dest} -c {container_dest} -d "1970-01-01"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars_t2,
        tty=True,
    )

    """
    Update Data Tracker inventory items based from Finance & Purchasing system inventory items
    """
    env_vars["KNACK_APP_ID_SRC"] = atd_knack_auth[app_name_src][env]["app_id"]
    env_vars["KNACK_APP_ID_DEST"] = atd_knack_auth[app_name_dest][env]["app_id"]
    env_vars["KNACK_API_KEY_DEST"] = atd_knack_auth[app_name_dest][env]["api_key"]

    t3 = DockerOperator(
        task_id="atd_knack_update_data_tracker_inventory_items_from_finance_inventory",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/records_to_knack.py -a {app_name_src} -c {container_src} -d "{date}" -dest {app_name_dest}',  # noqa
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
    env_vars_t4 = deepcopy(env_vars)
    env_vars_t4["KNACK_APP_ID"] = atd_knack_auth[app_name_dest][env]["app_id"]
    env_vars_t4["KNACK_API_KEY"] = atd_knack_auth[app_name_dest][env]["api_key"]

    t4 = DockerOperator(
        task_id="atd_knack_data_tracker_inventory_items_daily_snapshot_to_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/records_to_socrata.py -a {app_name_dest} -c {container_dest} -d "1970-01-01"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars_t4,
        tty=True,
    )

    t1 >> t2 >> t3 >> t4

if __name__ == "__main__":
    dag.cli()