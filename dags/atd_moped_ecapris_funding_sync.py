# Test locally with: docker compose run --rm airflow-cli dags test atd_moped_ecapris_funding_sync

from os import getenv

from utils.docker_operator import DockerOperatorWithFallback
from airflow.sdk import dag, Param, task
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Troubleshooting
Trigger the DAG again as needed since this one upserts records

## Testing
Trigger the DAG with the Moped local stack running to move records from ODP to local or staging Moped database
"""

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


@task
def get_required_secrets(params):
    target_database = params["target_database"]
    return {
        "SOCRATA_API_KEY_ID": {
            "opitem": "Socrata Key ID, Secret, and Token",
            "opfield": "socrata.apiKeyId",
        },
        "SOCRATA_API_KEY_SECRET": {
            "opitem": "Socrata Key ID, Secret, and Token",
            "opfield": "socrata.apiKeySecret",
        },
        "SOCRATA_TOKEN": {
            "opitem": "Socrata Key ID, Secret, and Token",
            "opfield": "socrata.appToken",
        },
        "SOCRATA_ENDPOINT": {
            "opitem": "Socrata Key ID, Secret, and Token",
            "opfield": "socrata.endpoint",
        },
        "HASURA_ENDPOINT": {
            "opitem": "Moped Hasura Admin",
            "opfield": f"{target_database}.Endpoint",
        },
        "HASURA_ADMIN_SECRET": {
            "opitem": "Moped Hasura Admin",
            "opfield": f"{target_database}.Admin Secret",
        },
        "FUNDING_DATASET_IDENTIFIER": {
            "opitem": "Moped ETLs",
            "opfield": f"{target_database}.FUNDING_DATASET_IDENTIFIER",
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
    doc_md=doc_md,
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
            enum=(
                ["production", "staging"]
                if DEPLOYMENT_ENVIRONMENT == "production"
                else ["staging", "development"]
            ),
            description="Target Moped database. Defaults to the current deployment environment.",
        ),
    },
    max_active_runs=1,  # Block schedule while DAG with params is triggered
)
def sync_ecapris_funding():
    # No staging tag for this image. Push test code to development image or run production image against staging or production environments.
    docker_image = f"atddocker/atd-moped-etl-ecapris-funding:{DEPLOYMENT_ENVIRONMENT}"

    REQUIRED_SECRETS = get_required_secrets()
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    branch_task = branch()

    common_docker_config = {
        "image": docker_image,
        "docker_conn_id": "docker_default",
        "auto_remove": "force",
        "environment": env_vars,
        "tty": True,
        "mount_tmp_dir": False,
    }

    ecapris_funding_sync_dry_run = DockerOperatorWithFallback(
        force_pull=True,
        task_id="ecapris_funding_sync_dry_run",
        command="python3.14 ecapris_funding_sync.py -n",
        **common_docker_config,
    )

    ecapris_funding_sync = DockerOperatorWithFallback(
        task_id="ecapris_funding_sync",
        command=f"python3.14 ecapris_funding_sync.py",
        **common_docker_config,
    )

    env_vars >> branch_task >> [ecapris_funding_sync_dry_run, ecapris_funding_sync]


sync_ecapris_funding()
