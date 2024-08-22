# Test locally with: docker compose run --rm airflow-cli dags test atd_moped_components_to_agol

import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator
from airflow.decorators import task
from airflow.models import Param
from pendulum import datetime, duration, now, parse

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
    "retry_delay": duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "HASURA_ENDPOINT": {
        "opitem": "Moped Hasura Admin",
        "opfield": "production.Endpoint",
    },
    "HASURA_ADMIN_SECRET": {
        "opitem": "Moped Hasura Admin",
        "opfield": "production.Admin Secret",
    },
    "AGOL_USERNAME": {
        "opitem": "AGOL Scripts Publisher",
        "opfield": "production.Username",
    },
    "AGOL_PASSWORD": {
        "opitem": "AGOL Scripts Publisher",
        "opfield": "production.Password",
    },
}


@task(
    task_id="get_args",
)
def get_args(full_replace_param, **context):
    full_replace = full_replace_param == "True"

    if full_replace:
        return "-f"
    else:
        prev_start_date = context.get("prev_start_date_success") or parse("1970-01-01")
        return f"-d {prev_start_date.isoformat()}"


with DAG(
    dag_id="atd_moped_components_to_agol",
    description="publish component record data to ArcGIS Online (AGOL)",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=duration(minutes=5),
    tags=["repo:atd-moped", "moped", "agol"],
    catchup=False,
    params={"full_replace": Param(False, type="boolean")},
) as dag:
    # docker_image = "atddocker/atd-moped-etl-arcgis:production"
    docker_image = "ubuntu:latest"

    # date_filter_arg = get_date_filter_arg()
    args = get_args("{{ params.full_replace }}")
    # args = get_args(params=dag.params)

    # env_vars = get_env_vars_task(REQUIRED_SECRETS)

    # t1 = DockerOperator(
    #     task_id="moped_components_to_agol",
    #     image=docker_image,
    #     auto_remove=True,
    #     command=f"python components_to_agol.py {date_filter_arg}",
    #     environment=env_vars,
    #     tty=True,
    #     force_pull=True,
    #     mount_tmp_dir=False,
    # )

    t1 = DockerOperator(
        task_id="moped_components_to_agol",
        image=docker_image,
        auto_remove=True,
        command=f"echo {args}",
        # environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    # t1 = BashOperator(
    #     task_id="test_param",
    #     bash_command='echo "{{params.full_replace}}"',
    # )

    args >> t1
