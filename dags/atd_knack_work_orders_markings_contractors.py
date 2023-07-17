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
    "KNACK_APP_ID": {
        "opitem": "Knack Signs and Markings",
        "opfield": f"production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack Signs and Markings",
        "opfield": f"production.apiKey",
    },
    "PGREST_ENDPOINT": {
        "opitem": "atd-knack-services PostgREST",
        "opfield": "production.endpoint",
    },
    "PGREST_JWT": {
        "opitem": "atd-knack-services PostgREST",
        "opfield": "production.jwt",
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
    "AGOL_USERNAME": {
        "opitem": "ArcGIS Online (AGOL) Scripts Publisher",
        "opfield": "production.username",
    },
    "AGOL_PASSWORD": {
        "opitem": "ArcGIS Online (AGOL) Scripts Publisher",
        "opfield": "production.password",
    },
}


with DAG(
    dag_id="atd_knack_work_orders_markings_contractors",
    description="Load markings contractor work orders records from Knack to Postgrest to AGOL, Socrata",
    default_args=DEFAULT_ARGS,
    # runs once at 1130a ct and again at 140pm ct
    schedule_interval="5 2 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=duration(minutes=5),
    tags=["repo:atd-knack-services", "knack", "socrata", "agol", "signs-markings"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-knack-services:production"
    app_name = "signs-markings"
    container = "view_3628"

    date_filter_arg = get_date_filter_arg(should_replace_monthly=True)

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="atd_knack_markings_contractor_work_orders_to_postgrest",
        image=docker_image,
        auto_remove=True,
        command=f"./atd-knack-services/services/records_to_postgrest.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )


    t2 = DockerOperator(
        task_id="atd_knack_markings_work_orders_to_agol",
        image=docker_image,
        auto_remove=True,
        command=f"./atd-knack-services/services/records_to_agol.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    t3 = DockerOperator(
        task_id="atd_knack_markings_contractor_work_orders_agol_build_markings_segment_geometries",
        image=docker_image,
        auto_remove=True,
        command=f'./atd-knack-services/services/agol_build_markings_segment_geometries.py -l markings_jobs  {date_filter_arg}',
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t4 = DockerOperator(
        task_id="atd_knack_markings_contractor_work_orders_to_socrata",
        image=docker_image,
        auto_remove=True,
        command=f'./atd-knack-services/services/records_to_socrata.py -a {app_name} -c {container} {date_filter_arg}',
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    date_filter_arg >> t1 >> t2 >> t3 >> t4
