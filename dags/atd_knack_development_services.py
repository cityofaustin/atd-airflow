# test locally with: docker compose run --rm airflow-cli dags test atd_knack_development_services

import os

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.helpers import chain
from pendulum import datetime, duration, now

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
    "execution_timeout": duration(minutes=30),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "KNACK_APP_ID": {
        "opitem": "Knack Development Services (TDS)",
        "opfield": f"production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack Development Services (TDS)",
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
}


def knack_services_task_template(task_id, image, command, env_vars, pull=False):
    return DockerOperator(
        task_id=task_id,
        image=image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=command,
        environment=env_vars,
        tty=True,
        force_pull=pull,
        mount_tmp_dir=False,
        trigger_rule="all_done",
    )


with DAG(
    dag_id="atd_knack_development_services",
    description="Downloads Knack data for several objects then publishes it to socrata datasets.",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "socrata", "tds", "development-services"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-knack-services:production"
    env_vars = get_env_vars_task(REQUIRED_SECRETS)
    date_filter_arg = get_date_filter_arg(should_replace_monthly=True)

    app_name = "development-services"

    commands = [
        {
            "task_id": "tia_mitigations_to_postgrest",
            "command": f"./atd-knack-services/services/records_to_postgrest.py -a development-services -c view_2814 {date_filter_arg}",
        },
        {
            "task_id": "tia_mitigations_to_socrata",
            "command": f"./atd-knack-services/services/records_to_socrata.py -a development-services -c view_2814 {date_filter_arg}",
        },
        {
            "task_id": "ts_cases_to_postgrest",
            "command": f"./atd-knack-services/services/records_to_postgrest.py -a development-services -c view_2926 {date_filter_arg}",
        },
        {
            "task_id": "ts_cases_to_socrata",
            "command": f"./atd-knack-services/services/records_to_socrata.py -a development-services -c view_2926 {date_filter_arg}",
        },
        {
            "task_id": "ts_scope_cases_to_postgrest",
            "command": f"./atd-knack-services/services/records_to_postgrest.py -a development-services -c view_2919 {date_filter_arg}",
        },
        {
            "task_id": "ts_scope_cases_to_socrata",
            "command": f"./atd-knack-services/services/records_to_socrata.py -a development-services -c view_2919 {date_filter_arg}",
        },
        {
            "task_id": "ts_submission_cycles_to_postgrest",
            "command": f"./atd-knack-services/services/records_to_postgrest.py -a development-services -c view_2920 {date_filter_arg}",
        },
        {
            "task_id": "ts_submission_cycles_to_socrata",
            "command": f"./atd-knack-services/services/records_to_socrata.py -a development-services -c view_2920 {date_filter_arg}",
        },
        {
            "task_id": "sif_formal_assessments_to_postgrest",
            "command": f"./atd-knack-services/services/records_to_postgrest.py -a development-services -c view_2921 {date_filter_arg}",
        },
        {
            "task_id": "sif_formal_assessments_to_socrata",
            "command": f"./atd-knack-services/services/records_to_socrata.py -a development-services -c view_2921 {date_filter_arg}",
        },
        {
            "task_id": "sif_formal_reviews_to_postgrest",
            "command": f"./atd-knack-services/services/records_to_postgrest.py -a development-services -c view_2924 {date_filter_arg}",
        },
        {
            "task_id": "sif_formal_reviews_to_socrata",
            "command": f"./atd-knack-services/services/records_to_socrata.py -a development-services -c view_2924 {date_filter_arg}",
        },
        {
            "task_id": "swf_final_assessments_to_postgrest",
            "command": f"./atd-knack-services/services/records_to_postgrest.py -a development-services -c view_2922 {date_filter_arg}",
        },
        {
            "task_id": "swf_final_assessments_to_socrata",
            "command": f"./atd-knack-services/services/records_to_socrata.py -a development-services -c view_2922 {date_filter_arg}",
        },
        {
            "task_id": "swf_final_reviews_to_postgrest",
            "command": f"./atd-knack-services/services/records_to_postgrest.py -a development-services -c view_2925 {date_filter_arg}",
        },
        {
            "task_id": "swf_final_reviews_to_socrata",
            "command": f"./atd-knack-services/services/records_to_socrata.py -a development-services -c view_2925 {date_filter_arg}",
        },
        {
            "task_id": "tia_determinations_to_postgrest",
            "command": f"./atd-knack-services/services/records_to_postgrest.py -a development-services -c view_2923 {date_filter_arg}",
        },
        {
            "task_id": "tia_determinations_to_socrata",
            "command": f"./atd-knack-services/services/records_to_socrata.py -a development-services -c view_2923 {date_filter_arg}",
        },
    ]

    tasks = []

    for cmd in commands:
        # We want the first task to pull the latest docker image
        if len(tasks) == 0:
            pull = True
        else:
            pull = False
        tasks.append(
            knack_services_task_template(
                task_id=cmd["task_id"],
                image=docker_image,
                command=cmd["command"],
                env_vars=env_vars,
                pull=pull,
            )
        )

    chain(*tasks)
