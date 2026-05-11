from os import getenv

from airflow.sdk import dag
from utils.docker_operator import DockerOperatorWithFallback
from pendulum import datetime

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert, slack_member_ids

doc_md = """
Extracts EMS and AFD data from files in an S3 bucket and imports to a VZ Database.

If no email is found in the S3 bucket, the task will throw an error.

Until the automatic email forwarding is fixed, Xavier manually forwards the email. As such, this may fail if Xavier does not forward the email.


Issue tracking email forwarding: https://github.com/cityofaustin/atd-data-tech/issues/21712

"""


DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT")

secrets_env_prefix = None

if DEPLOYMENT_ENVIRONMENT == "production":
    secrets_env_prefix = "prod"
elif DEPLOYMENT_ENVIRONMENT == "staging":
    secrets_env_prefix = "staging"
else:
    secrets_env_prefix = "dev"

docker_image = f"atddocker/vz-afd-ems-import:{'production' if DEPLOYMENT_ENVIRONMENT == 'production' else 'latest'}"


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


@dag(
    dag_id="vz-afd-ems-incident-import",
    description="A DAG which imports EMS and AFD data into the Vision Zero database.",
    doc_md=doc_md,
    # todo: we are currently skipping weekends
    # https://github.com/cityofaustin/atd-data-tech/issues/25781
    schedule="45 7 * * 1-5" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    start_date=datetime(2023, 1, 1, tz="America/Chicago"),
    catchup=False,
    tags=["repo:atd-vz-data", "vision-zero", "ems", "afd", "import"],
    on_failure_callback=(
        task_fail_slack_alert if DEPLOYMENT_ENVIRONMENT == "production" else None
    ),
)
def etl_data_import():
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    # EMS
    ems_import = DockerOperatorWithFallback(
        task_id="run_ems_import",
        environment=env_vars,
        image=docker_image,
        auto_remove="force",
        command="ems",
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    # AFD
    afd_import = DockerOperatorWithFallback(
        task_id="run_afd_import",
        environment=env_vars,
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command="afd",
        tty=True,
        mount_tmp_dir=False,
    )

    env_vars >> [ems_import, afd_import]

dag_instance = etl_data_import()

if dag_instance:
    dag_instance.byline = (
        f"Failure impacts Vision Zero team, {slack_member_ids['John']} & {slack_member_ids['Frank']}"
    )
