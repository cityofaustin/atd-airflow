# Test locally with: docker compose run --rm airflow-cli dags test atd_moped_components_to_agol

import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
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
def get_args(params, **context):
    """Task to get a date filter based on previous success date. If there
    is no prev success date, 1970-01-01 is returned. If arg_override is provided,
    it will be returned instead of the date filter arg.

    Args:
        arg_override (string): if provided, the arg string will be used instead of the date filter
        in the Python script invocation.
        context (dict): Airflow task context, which contains the prev_start_date_success
            variable.

    Returns:
        Str: the -d flag and ISO date string or script argument override string.
    """
    full_replace = bool(params["full_replace"])
    if full_replace == False:
        prev_start_date = context.get("prev_start_date_success") or parse("1970-01-01")
        return f"-d {prev_start_date.isoformat()}"
    else:
        return "-f"


@task.branch(task_id="branch")
def branch(params):
    full_replace = bool(params["full_replace"])

    if full_replace:
        return "moped_components_to_agol_full"
    else:
        return "moped_components_to_agol_incremental"


with DAG(
    dag_id="atd_moped_components_to_agol",
    description="publish component record data to ArcGIS Online (AGOL)",
    default_args=DEFAULT_ARGS,
    # schedule_interval="*/5 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    schedule_interval="*/1 * * * *",
    tags=["repo:atd-moped", "moped", "agol"],
    catchup=False,
    params={"full_replace": Param(default=False, type="boolean")},
    max_active_runs=1,  # Block schedule while DAG with params is triggered
) as dag:
    # docker_image = "atddocker/atd-moped-etl-arcgis:production"
    docker_image = "ubuntu:latest"

    args = get_args()

    # env_vars = get_env_vars_task(REQUIRED_SECRETS)

    # t1 = DockerOperator(
    #     task_id="moped_components_to_agol",
    #     image=docker_image,
    #     auto_remove=True,
    #     command=f"python components_to_agol.py {args}",
    #     environment=env_vars,
    #     tty=True,
    #     force_pull=True,
    #     mount_tmp_dir=False,
    # execution_timeout=(
    #     duration(minutes=3)
    #     if '{{ params.arg_override == "" }}'
    #     else duration(seconds=1)
    # ),
    # )
    branch = branch()

    full = DockerOperator(
        task_id="moped_components_to_agol_full",
        image=docker_image,
        auto_remove=True,
        command=f"sleep 2m",
        # environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        execution_timeout=duration(seconds=1),
    )

    incremental = DockerOperator(
        task_id="moped_components_to_agol_incremental",
        image=docker_image,
        auto_remove=True,
        command=f"sleep 2m",
        # environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        execution_timeout=duration(minutes=2),
    )

    args >> branch >> [full, incremental]
