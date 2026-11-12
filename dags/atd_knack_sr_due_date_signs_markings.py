from os import getenv

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag, Param
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 11, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "execution_timeout": duration(minutes=10),
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
}

DAG_DOC_MD = """
Reads sr records from Signs and Markings view 3059 and looks for a corresponding due date from flex notes in view 3060.
Then updates the record from view 3059 with the due date.

https://atd.knack.com/signs-markings#atd-data-tracker-api-public/csr-issues-due-date/
"""


@dag(
    dag_id="atd_knack_signs_markings_sr_due_date",
    description="",
    default_args=DEFAULT_ARGS,
    schedule="11 4 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "signs-markings"],
    catchup=False,
    doc_md=DAG_DOC_MD,
    params={
        "docker_image_tag": Param(
            "production",
            type="string",
            title="Docker image tag",
            description="Tag of atddocker/atd-knack-services to run. Use 'local' for a locally built image.",
        ),
    },
)
def atd_knack_signs_markings_sr_due_date():
    env_vars = get_env_vars_task(REQUIRED_SECRETS)
    docker_image = "atddocker/atd-knack-services:{{ params.docker_image_tag }}"

    service_request_due_date_assign_task = DockerOperator(
        task_id="service_request_due_date_assign",
        image=docker_image,
        auto_remove="force",
        command=f"./atd-knack-services/services/sr_due_date_assign.py -a signs-markings",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    service_request_due_date_assign_task


atd_knack_signs_markings_sr_due_date()
