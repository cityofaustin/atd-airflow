from os import getenv

from utils.docker_operator import DockerOperatorWithFallback
from airflow.sdk import dag
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.knack import get_date_filter_arg
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
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
        "opitem": "atd-road-conditions PostgREST",
        "opfield": "production.endpoint",
    },
    "PGREST_JWT": {
        "opitem": "atd-road-conditions PostgREST",
        "opfield": "production.jwt",
    },
}


@dag(
    dag_id="road_conditions_socrata",
    default_args=DEFAULT_ARGS,
    description="Fetch road condition sensor data from postgrest and publish to socrata",
    schedule="*/5 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-road-conditions", "socrata"],
    catchup=False,
    doc_md="""
## Road conditions to Socrata

Fetches road condition sensor data from PostgREST and publishes it to Socrata
using the 'atddocker/atd-road-conditions:production' image. Requires VPN access 
to reach PostgREST.
""",
)
def road_conditions_socrata():

    date_filter_arg = get_date_filter_arg(should_replace_monthly=False)

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    publish_road_conditions_to_socrata = DockerOperatorWithFallback(
        task_id="road_conditions_socrata",
        image="atddocker/atd-road-conditions:production",
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-road-conditions/socrata.py {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    date_filter_arg >> publish_road_conditions_to_socrata


road_conditions_socrata()
