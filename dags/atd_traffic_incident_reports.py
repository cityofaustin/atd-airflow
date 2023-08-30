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
    "execution_timeout": duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    # Database
    "HOST": {
        "opitem": "atd-traffic-incident-reports",
        "opfield": "production.Host",
    },
    "PORT": {
        "opitem": "atd-traffic-incident-reports",
        "opfield": "production.Port",
    },
    "USER": {
        "opitem": "atd-traffic-incident-reports",
        "opfield": "production.Username",
    },
    "SERVICE": {
        "opitem": "atd-traffic-incident-reports",
        "opfield": "production.Service",
    },
    "PASSWORD": {
        "opitem": "atd-traffic-incident-reports",
        "opfield": "production.Password",
    },
    # PostgREST
    "PGREST_TOKEN": {
        "opitem": "atd-traffic-incident-reports",
        "opfield": "production.Postgrest JWT",
    },
    "PGREST_ENDPOINT": {
        "opitem": "atd-traffic-incident-reports",
        "opfield": "production.Postgrest Endpoint",
    },
}


with DAG(
    dag_id="atd_traffic_incident_reports",
    description="wrapper etl for atd-traffic-incident-reports docker image connects to oracle db and updates postrgrest with incidents",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-traffic-incident-reports", "postgrest"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-traffic-incident-reports:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="traffic_incident_reports_to_postgres",
        image=docker_image,
        auto_remove=True,
        command=f"python main.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    t1
