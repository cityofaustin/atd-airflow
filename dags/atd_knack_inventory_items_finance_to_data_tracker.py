from os import getenv

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.knack import get_date_filter_arg
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "KNACK_APP_ID_DATA_TRACKER": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"production.appId",
    },
    "KNACK_API_KEY_DATA_TRACKER": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"production.apiKey",
    },
    "KNACK_APP_ID_FINANCE": {
        "opitem": "Knack Finance and Purchasing",
        "opfield": f"production.appId",
    },
    "KNACK_API_KEY_FINANCE": {
        "opitem": "Knack Finance and Purchasing",
        "opfield": f"production.apiKey",
    },
    "SOCRATA_API_KEY_ID": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SOCRATA_API_KEY_SECRET": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SOCRATA_APP_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
    "PGREST_ENDPOINT": {
        "opitem": "atd-knack-services PostgREST",
        "opfield": "production.endpoint",
    },
    "PGREST_JWT": {
        "opitem": "atd-knack-services PostgREST",
        "opfield": "production.jwt",
    },
    "AGOL_USERNAME": {
        "opitem": "ArcGIS Online (AGOL) Scripts Publisher",
        "opfield": "production.username",
    },
    "AGOL_PASSWORD": {
        "opitem": "ArcGIS Online (AGOL) Scripts Publisher",
        "opfield": "production.password",
    },
}

# finance > postgrest
finance_inventory_postgrest_required_secrets = dict(REQUIRED_SECRETS)
finance_inventory_postgrest_required_secrets["KNACK_APP_ID"] = (
    finance_inventory_postgrest_required_secrets["KNACK_APP_ID_FINANCE"]
)
finance_inventory_postgrest_required_secrets["KNACK_API_KEY"] = (
    finance_inventory_postgrest_required_secrets["KNACK_API_KEY_FINANCE"]
)
# data tracker > postgrest
data_tracker_inventory_postgrest_required_secrets = dict(REQUIRED_SECRETS)
data_tracker_inventory_postgrest_required_secrets["KNACK_APP_ID"] = (
    data_tracker_inventory_postgrest_required_secrets["KNACK_APP_ID_DATA_TRACKER"]
)
data_tracker_inventory_postgrest_required_secrets["KNACK_API_KEY"] = (
    data_tracker_inventory_postgrest_required_secrets["KNACK_API_KEY_DATA_TRACKER"]
)
# postgres (finance) > data tracker
finance_inventory_data_tracker_sync_required_secrets = dict(REQUIRED_SECRETS)
finance_inventory_data_tracker_sync_required_secrets["KNACK_APP_ID_SRC"] = (
    finance_inventory_data_tracker_sync_required_secrets["KNACK_APP_ID_FINANCE"]
)
finance_inventory_data_tracker_sync_required_secrets["KNACK_APP_ID_DEST"] = (
    finance_inventory_data_tracker_sync_required_secrets["KNACK_APP_ID_DATA_TRACKER"]
)
finance_inventory_data_tracker_sync_required_secrets["KNACK_API_KEY_DEST"] = (
    finance_inventory_data_tracker_sync_required_secrets["KNACK_API_KEY_DATA_TRACKER"]
)


DAG_DOC_MD = """
### atd_knack_inventory_items_finance_to_data_tracker
Updates Data Tracker inventory items using records from the Finance and Purchasing system.
"""


@dag(
    dag_id="atd_knack_inventory_items_finance_to_data_tracker",
    description="Update inventory items in the Data Tracker from the Finance and Purchasing system",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2015, 1, 1, tz="America/Chicago"),
    schedule="55 1 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "data-tracker", "finance"],
    catchup=False,
    doc_md=DAG_DOC_MD,
)
def atd_knack_inventory_items_finance_to_data_tracker():
    docker_image = "atddocker/atd-knack-services:production"
    app_name_src = "finance-purchasing"
    app_name_dest = "data-tracker"
    container_dest = "view_2863"
    container_src = "view_788"

    finance_inventory_postgrest_env_vars = get_env_vars_task(
        finance_inventory_postgrest_required_secrets
    )
    data_tracker_inventory_postgrest_env_vars = get_env_vars_task(
        data_tracker_inventory_postgrest_required_secrets
    )
    finance_inventory_data_tracker_sync_env_vars = get_env_vars_task(
        finance_inventory_data_tracker_sync_required_secrets
    )
    date_filter_arg = get_date_filter_arg(should_replace_monthly=False)

    load_finance_inventory_to_postgrest_task = DockerOperator(
        task_id="atd_knack_finance_inventory_items_to_postgrest",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_postgrest.py -a {app_name_src} -c {container_src} {date_filter_arg}",
        environment=finance_inventory_postgrest_env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    load_data_tracker_inventory_to_postgrest_task = DockerOperator(
        task_id="atd_knack_data_tracker_inventory_items_to_postgrest",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_postgrest.py -a {app_name_dest} -c {container_dest} {date_filter_arg}",
        environment=data_tracker_inventory_postgrest_env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    sync_finance_inventory_to_data_tracker_task = DockerOperator(
        task_id="atd_knack_update_data_tracker_inventory_items_from_finance_inventory",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_knack.py -a {app_name_src} -c {container_src} {date_filter_arg} -dest {app_name_dest}",
        environment=finance_inventory_data_tracker_sync_env_vars,
        tty=True,
        mount_tmp_dir=False,
    )
    (
        date_filter_arg
        >> load_finance_inventory_to_postgrest_task
        >> load_data_tracker_inventory_to_postgrest_task
        >> sync_finance_inventory_to_data_tracker_task
    )


dag = atd_knack_inventory_items_finance_to_data_tracker()
