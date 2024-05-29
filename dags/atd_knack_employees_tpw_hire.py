# test locally with: docker compose run --rm airflow-cli dags test atd_knack_employees_tpw_hire

import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration, now

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert
from utils.knack import get_date_filter_arg

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=30),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS_KNACK_SERVICES = {
    "PGREST_ENDPOINT": {
        "opitem": "atd-knack-services PostgREST",
        "opfield": "production.endpoint",
    },
    "PGREST_JWT": {
        "opitem": "atd-knack-services PostgREST",
        "opfield": "production.jwt",
    },
}

REQUIRED_SECRETS_HR_MANAGER = {
    "KNACK_APP_ID": {
        "opitem": "Knack Human Resources (HR)",
        "opfield": f"production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack Human Resources (HR)",
        "opfield": f"production.apiKey",
    },
}

REQUIRED_SECRETS_TPW_HIRE = {
    "KNACK_APP_ID": {
        "opitem": "TPW Hire Knack App",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "TPW Hire Knack App",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.apiKey",
    },
}

REQUIRED_SECRETS_KNACK_TO_KNACK = {
    "KNACK_APP_ID_DEST": REQUIRED_SECRETS_TPW_HIRE["KNACK_APP_ID"],
    "KNACK_API_KEY_DEST": REQUIRED_SECRETS_TPW_HIRE["KNACK_API_KEY"],
    "KNACK_APP_ID_SRC": REQUIRED_SECRETS_HR_MANAGER["KNACK_APP_ID"],
    "KNACK_API_KEY_SRC": REQUIRED_SECRETS_HR_MANAGER["KNACK_API_KEY"],
}

REQUIRED_SECRETS_HR_MANAGER.update(REQUIRED_SECRETS_KNACK_SERVICES)
REQUIRED_SECRETS_TPW_HIRE.update(REQUIRED_SECRETS_KNACK_SERVICES)
REQUIRED_SECRETS_KNACK_TO_KNACK.update(REQUIRED_SECRETS_KNACK_SERVICES)

with DAG(
    dag_id=f"atd_knack_employees_tpw_hire",
    description="Copies Banner TPW employee data from the HR knack app to TPW hire knack app.",
    default_args=DEFAULT_ARGS,
    schedule_interval="30 1 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "hr-manager", "tpw-hire", "banner"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-knack-services:production"
    app_name_src =  "hr-manager" 
    container_src = "view_684"
    app_name_dest = "tpw-hire"
    container_dest = "view_148"

    date_filter_arg = get_date_filter_arg(should_replace_monthly=True)
    env_vars_t1 = get_env_vars_task(REQUIRED_SECRETS_HR_MANAGER)
    env_vars_t2 = get_env_vars_task(REQUIRED_SECRETS_TPW_HIRE)
    env_vars_t3 = get_env_vars_task(REQUIRED_SECRETS_KNACK_TO_KNACK)

    t1 = DockerOperator(
        task_id="hr_accounts_to_postgrest",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"python ./atd-knack-services/services/records_to_postgrest.py -a {app_name_src} -c {container_src} {date_filter_arg}",
        environment=env_vars_t1,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    t2 = DockerOperator(
        task_id="tpw_hire_employees_to_postgrest",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"python ./atd-knack-services/services/records_to_postgrest.py -a {app_name_dest} -c {container_dest} {date_filter_arg}",
        environment=env_vars_t2,
        tty=True,
        mount_tmp_dir=False,
    )

    t3 = DockerOperator(
        task_id="hr_accounts_to_tpw_hire",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"python ./atd-knack-services/services/records_to_knack.py -a {app_name_src} -c {container_src} --app-name-dest {app_name_dest} {date_filter_arg}",
        environment=env_vars_t3,
        tty=True,
        mount_tmp_dir=False,
    )
    date_filter_arg >> t1 >> t2 >> t3
