from os import getenv

from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert
from utils.knack import get_date_filter_arg

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=60),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "KNACK_APP_ID": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": "production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": "production.apiKey",
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
}


with DAG(
    dag_id=f"atd_knack_amd_preventative_maintenance",
    description="Copies primary signal preventive maintenance records to secondary signals. Then, loads preventative maintenance work order (view_3887) records from Knack to Postgrest and Socrata.",
    doc_md="**Need VPN access or addition to security group allow list to reach Postgrest**",
    default_args=DEFAULT_ARGS,
    schedule="15 4 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "socrata", "data-tracker"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-knack-services:production"
    app_name = "data-tracker"
    container = "view_3887"
    copy_to_secondary_view = "view_4284"

    date_filter_arg = get_date_filter_arg(should_replace_monthly=True)

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="atd_knack_preventative_maintenance_copy",
        image=docker_image,
        auto_remove="force",
        command=f"./atd-knack-services/services/signal_pm_copier.py -a {app_name} -c {copy_to_secondary_view}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t2 = DockerOperator(
        task_id="atd_knack_amd_pm_to_postgrest",
        image=docker_image,
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_postgrest.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t3 = DockerOperator(
        task_id="atd_knack_amd_pm_to_socrata",
        image=docker_image,
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_socrata.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    date_filter_arg >> t1 >> t2 >> t3
