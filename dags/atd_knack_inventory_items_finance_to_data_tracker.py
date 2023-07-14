import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.knack import get_date_filter_arg
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": duration(minutes=5),
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
t1_required_secrets = dict(REQUIRED_SECRETS)
t1_required_secrets["KNACK_APP_ID"] = t1_required_secrets["KNACK_APP_ID_FINANCE"]
t1_required_secrets["KNACK_API_KEY"] = t1_required_secrets["KNACK_API_KEY_FINANCE"]
# data tracker > postgrest
t2_required_secrets = dict(REQUIRED_SECRETS)
t2_required_secrets["KNACK_APP_ID"] = t2_required_secrets["KNACK_APP_ID_DATA_TRACKER"]
t2_required_secrets["KNACK_API_KEY"] = t2_required_secrets["KNACK_API_KEY_DATA_TRACKER"]
# postgres (finance) > data tracker
t3_required_secrets = dict(REQUIRED_SECRETS)
t3_required_secrets["KNACK_APP_ID_SRC"] = t3_required_secrets["KNACK_APP_ID_FINANCE"]
t3_required_secrets["KNACK_APP_ID_DEST"] = t3_required_secrets["KNACK_APP_ID_DATA_TRACKER"]
t3_required_secrets["KNACK_API_KEY_DEST"] =t3_required_secrets["KNACK_API_KEY_DATA_TRACKER"]

with DAG(
    dag_id="atd_knack_inventory_items_finance_to_data_tracker",
    default_args=DEFAULT_ARGS,
    schedule_interval="55 1 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=duration(minutes=5),
    tags=["repo:atd-knack-services", "knack", "socrata", "agol", "data-tracker"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-knack-services:production"
    app_name_src = "finance-purchasing"
    app_name_dest = "data-tracker"
    container_dest = "view_2863"
    container_src = "view_788"

    env_vars_t1 = get_env_vars_task(t1_required_secrets)
    env_vars_t2 = get_env_vars_task(t2_required_secrets)
    env_vars_t3 = get_env_vars_task(t3_required_secrets)
    date_filter_arg = get_date_filter_arg(should_replace_monthly=False)

    t1 = DockerOperator(
        task_id="atd_knack_finance_inventory_items_to_postgrest",
        image=docker_image,
        auto_remove=True,
        command=f'./atd-knack-services/services/records_to_postgrest.py -a {app_name_src} -c {container_src} {date_filter_arg}', 
        environment=env_vars_t1,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    t2 = DockerOperator(
        task_id="atd_knack_data_tracker_inventory_items_to_postgrest",
        image=docker_image,
        auto_remove=True,
        command=f'./atd-knack-services/services/records_to_postgrest.py -a {app_name_dest} -c {container_dest} {date_filter_arg}',
        environment=env_vars_t2,
        tty=True,
        mount_tmp_dir=False,
    )


    t3 = DockerOperator(
        task_id="atd_knack_update_data_tracker_inventory_items_from_finance_inventory",
        image=docker_image,
        auto_remove=True,
        command=f'./atd-knack-services/services/records_to_knack.py -a {app_name_src} -c {container_src} {date_filter_arg} -dest {app_name_dest}', 
        environment=env_vars_t3,
        tty=True,
        mount_tmp_dir=False,
    )
    date_filter_arg >> t1 >> t2 >> t3
