# Test locally with: docker compose run --rm airflow-cli dags test atd_moped_ecapris_funding_sync

from os import getenv

from airflow.decorators import dag, task
from airflow.operators.docker_operator import DockerOperator
from airflow.models import Param
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

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


def get_required_secrets(environment):
    return {
        "HASURA_ENDPOINT": {
            "opitem": "Moped Hasura Admin",
            "opfield": f"{environment}.Endpoint",
        },
        "HASURA_ADMIN_SECRET": {
            "opitem": "Moped Hasura Admin",
            "opfield": f"{environment}.Admin Secret",
        },
        "ORACLE_USER": {
            "opitem": "Finance Data Warehouse Oracle DB",
            "opfield": "production.Username",
        },
        "ORACLE_PASSWORD": {
            "opitem": "Finance Data Warehouse Oracle DB",
            "opfield": "production.Password",
        },
        "ORACLE_HOST": {
            "opitem": "Finance Data Warehouse Oracle DB",
            "opfield": "production.Host",
        },
        "ORACLE_PORT": {
            "opitem": "Finance Data Warehouse Oracle DB",
            "opfield": "production.Port",
        },
        "ORACLE_SERVICE": {
            "opitem": "Finance Data Warehouse Oracle DB",
            "opfield": "production.Service",
        },
    }


@task.branch(task_id="branch")
def branch(params):
    """Task to determine whether to dry run or not based on web server input.
    See https://airflow.apache.org/docs/apache-airflow/2.10.5/core-concepts/dags.html#branching
    See https://airflow.apache.org/docs/apache-airflow/2.10.5/core-concepts/params.html.

    Args:
        params (dict): Airflow params dictionary that contains user input value or default.
        context (dict): Airflow task context, which contains the prev_start_date_success
            variable.

    Returns:
        Str: the task id of the task branch to follow.
    """
    dry_run = bool(params["dry_run"])

    if dry_run:
        return "ecapris_funding_sync_dry_run"
    else:
        return "ecapris_funding_sync"


@dag(
    dag_id="atd_moped_ecapris_funding_sync",
    description="sync eCapris funding to Moped database",
    default_args=DEFAULT_ARGS,
    # Scheduled to run after atd_finance_data_fdus DAG
    schedule=("33 8 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None),
    dagrun_timeout=duration(minutes=30),
    tags=["repo:atd-moped", "moped", "ecapris"],
    catchup=False,
    params={
        "dry_run": Param(default=False, type="boolean"),
        "target_database": Param(
            default=DEPLOYMENT_ENVIRONMENT,
            enum=["production", "staging", "development"],
            description="Target Moped environment. Defaults to the current deployment environment. Override to target staging manually.",
        ),
    },
    max_active_runs=1,  # Block schedule while DAG with params is triggered
)
def sync_ecapris_funding():
    # There is no staging tag for this image. Test locally with development or run production code against staging or production environments.
    docker_image = f"atddocker/atd-moped-etl-ecapris-funding:{DEPLOYMENT_ENVIRONMENT}"

    target_database = "{{ params.target_database }}"
    REQUIRED_SECRETS = get_required_secrets(target_database)
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    branch_task = branch()

    common_docker_config = {
        "image": docker_image,
        "docker_conn_id": "docker_default",
        "auto_remove": "force",
        "environment": env_vars,
        "tty": True,
        "force_pull": True,
        "mount_tmp_dir": False,
    }

    ecapris_funding_sync_dry_run = DockerOperator(
        task_id="ecapris_funding_sync_dry_run",
        command="python3.14 ecapris_funding_sync.py -n",
        **common_docker_config,
    )

    ecapris_funding_sync = DockerOperator(
        task_id="ecapris_funding_sync",
        command=f"python3.14 ecapris_funding_sync.py",
        **common_docker_config,
    )

    env_vars >> branch_task >> [ecapris_funding_sync_dry_run, ecapris_funding_sync]


sync_ecapris_funding()
