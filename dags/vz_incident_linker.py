from os import getenv

from airflow.sdk import dag, task, Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert, slack_member_ids

doc_md = """
Process CAD files in two steps. First, transfer files from COACD network drive to S3. Then, transform and load files in to the VZ database via graphql API.

Files are delivered to the shared network drive daily at 5am. If no files are found in the network drive or in S3 bucket, tasks will throw an error.

If files are not delivered, reach out to Oleg or Gita or Donghong on the public safety enterprise data team.
"""

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT")

secrets_env_prefix = None

if DEPLOYMENT_ENVIRONMENT == "production":
    secrets_env_prefix = "prod"
elif DEPLOYMENT_ENVIRONMENT == "staging":
    secrets_env_prefix = "staging"
else:
    secrets_env_prefix = "dev"

docker_image = f"atddocker/vz-incidents:{'production' if DEPLOYMENT_ENVIRONMENT == 'production' else 'latest'}"


REQUIRED_SECRETS = {
    "HASURA_GRAPHQL_ENDPOINT": {
        "opitem": "Vision Zero ETLs",
        "opfield": f"{secrets_env_prefix}.HASURA_GRAPHQL_ENDPOINT",
    },
    "HASURA_GRAPHQL_ADMIN_SECRET": {
        "opitem": "Vision Zero ETLs",
        "opfield": f"{secrets_env_prefix}.HASURA_GRAPHQL_ADMIN_SECRET",
    },
}

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
    "execution_timeout": duration(minutes=45),
}


@task(
    task_id="get_is_dry_run_arg",
)
def get_is_dry_run_arg(params):
    """Return ` --dry-run` if the dry_run param has been set"""
    if bool(params["dry_run"]):
        return " --dry-run"
    else:
        return ""


@task(
    task_id="get_incident_link_limit",
)
def get_incident_link_limit(params):
    """Return ` --limit {number}` if the incident_link_limit param has been set"""
    if bool(params["incident_link_limit"]):
        return f" --limit {params["incident_link_limit"]}"
    else:
        return ""


@dag(
    dag_id="vz-cad-incidents-import",
    description="A DAG which creates VZ incident records in the Vision Zero database.",
    doc_md=doc_md,
    # the CAD file export happens daily at 5a CT
    schedule="* 7 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    start_date=datetime(2023, 1, 1, tz="America/Chicago"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["repo:atd-vz-data", "vision-zero", "vz-incidents", "linker"],
    params={
        "dry_run": Param(
            title="Dry run",
            default=False,
            type="boolean",
            description_md="Applies the dry-run flag to all tasks. No records will be processed.",
        ),
        "incident_link_limit": Param(
            title="Incident link limit",
            default=None,
            type=["integer", "null"],
            description_md="The maximum number of records to link via incident_linker.py. Otherwise the script's default limit will be applied.",
        ),
    },
)
def etl_data_import():
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    dry_run_arg = get_is_dry_run_arg()

    incident_link_limit = get_incident_link_limit()

    incidents_linker = DockerOperator(
        task_id="cad_incidents_links",
        environment=env_vars,
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"incident_linker.py{dry_run_arg}{incident_link_limit}",
        tty=True,
        mount_tmp_dir=False,
    )

    ([env_vars, dry_run_arg, incident_link_limit] >> incidents_linker)


etl_data_import()
