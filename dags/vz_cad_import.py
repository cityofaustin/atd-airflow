from os import getenv

from airflow.sdk import dag, task, Param
from docker.types import Mount

from pendulum import datetime, duration

from utils.docker_operator import DockerOperatorWithFallback
from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert, slack_member_ids

doc_md = """
Process CAD files in two steps. First, transfer files from COACD netork drive to S3. Then, transform and load files in to the VZ database via graphql API.

If no files are found in the network drive or in S3 bucket, tasks will throw an error.
"""

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT")

secrets_env_prefix = None

if DEPLOYMENT_ENVIRONMENT == "production":
    secrets_env_prefix = "prod"
elif DEPLOYMENT_ENVIRONMENT == "staging":
    secrets_env_prefix = "staging"
else:
    secrets_env_prefix = "dev"

docker_image = f"atddocker/vz-cad-incidents-import:{'production' if DEPLOYMENT_ENVIRONMENT == 'production' else 'latest'}"


# for local dev, replace `"/your/path/here` with the abs path to your testing files, e.g.,
# /Users/john/atd/vision-zero/etl/cad_incidents_import/test_data
mount_source = (
    "/mnt/vision_zero_cad"
    if DEPLOYMENT_ENVIRONMENT == "production"
    else "/your/path/here"
)

REQUIRED_SECRETS = {
    "BUCKET_ENV": {
        "opitem": "Vision Zero ETLs",
        "opfield": f"{secrets_env_prefix}.BUCKET_ENV",
    },
    "AWS_ACCESS_KEY_ID": {
        "opitem": "Vision Zero ETLs",
        "opfield": f"common.AWS_ACCESS_KEY_ID",
    },
    "AWS_SECRET_ACCESS_KEY": {
        "opitem": "Vision Zero ETLs",
        "opfield": f"common.AWS_SECRET_ACCESS_KEY",
    },
    "BUCKET_NAME": {
        "opitem": "Vision Zero ETLs",
        "opfield": f"common.BUCKET_NAME",
    },
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
    "retry_delay": duration(minutes=1),
    "on_failure_callback": task_fail_slack_alert,
}


@task(
    task_id="get_args",
)
def get_is_dry_run_arg(params):
    """Return ` --dry-run` if the dry_run param has been set"""
    if bool(params["dry_run"]):
        return " --dry-run"
    else:
        return ""


@dag(
    dag_id="vz-cad-incidents-import",
    description="A DAG which imports CAD records into the Vision Zero database.",
    doc_md=doc_md,
    schedule="15 6 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    start_date=datetime(2023, 1, 1, tz="America/Chicago"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["repo:atd-vz-data", "vision-zero", "cad", "import"],
    params={
        "dry_run": Param(default=False, type="boolean"),
    },
)
def etl_data_import():
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    dry_run_arg = get_is_dry_run_arg()

    files_volume_mount = Mount(
        source=mount_source,
        target="/mnt/vision_zero_cad",
        type="bind",
    )

    incidents_to_s3 = DockerOperatorWithFallback(
        task_id="cad_incidents_to_s3",
        environment=env_vars,
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"incidents_to_s3.py{dry_run_arg}",
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        mounts=[files_volume_mount],
    )

    incidents_import = DockerOperatorWithFallback(
        task_id="cad_incidents_import",
        environment=env_vars,
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"incidents_import.py --archive{dry_run_arg}",
        tty=True,
        mount_tmp_dir=False,
        mounts=[files_volume_mount],
    )

    env_vars >> incidents_to_s3 >> incidents_import


etl_data_import()
