# test locally with: docker compose run --rm airflow-cli dags test atd_knack_signal_requests

from os import getenv

from airflow.sdk import DAG
from utils.docker_operator import DockerOperatorWithFallback
from pendulum import datetime, duration

from utils.knack import get_date_filter_arg
from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Knack Services DAG: Traffic Signal Requests

Load signal requests (view_200) records from Knack to AGOL

## Troubleshooting

You should not need to be on VPN to reach Knack or AGOL.

Most of the time just re-triggering this DAG will likely resolve any issues automatically.

The most common bug is when the underlying Knack view is changed and the corresponding AGOL feature service
was not updated to match.

"""

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=15),
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

with DAG(
    dag_id=f"atd_knack_signal_requests",
    description="Load signal requests (view_200) records from Knack to AGOL",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="30 0 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "data-tracker"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-knack-services:production"
    app_name = "data-tracker"
    container = "view_200"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    date_filter_arg = get_date_filter_arg(should_replace_monthly=True)

    t1 = DockerOperatorWithFallback(
        task_id="signal_requests_to_postgrest",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_postgrest.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t2 = DockerOperatorWithFallback(
        task_id="signal_requests_to_agol",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_agol.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    date_filter_arg >> t1 >> t2
