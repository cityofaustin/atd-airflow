"""Process CRIS extract zips—including CSVs and PDFs.

The target DB and S3 bucket subdirectory are controlled by the Airflow `ENVIRONMENT`
env var, which determines which 1pass secrets to apply to the docker runtime env.

Check the 1Pass entry to understand exactly what will happen when you trigger
this tag in a given context, but the expected behavior is that you may set the 
Airflow `ENVIRONMENT` to `production`, `staging`, or `dev`, with the following
results:
- production: use <bucket-name>/prod/inbox and production hasura cluster
- staging: use <bucket-name>/staging/inbox and staging hasura cluster
- dev: use <bucket-name>/dev/inbox and localhost hasura cluster
"""
import os
from pendulum import datetime, duration

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert


DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT")
secrets_env_prefix = None

if DEPLOYMENT_ENVIRONMENT == "production":
    secrets_env_prefix = "prod"
elif DEPLOYMENT_ENVIRONMENT == "staging":
    secrets_env_prefix = "staging"
else:
    secrets_env_prefix = "dev"


REQUIRED_SECRETS = {
    "ENV": {
        "opitem": "Vision Zero CRIS Import - v2",
        "opfield": f"{secrets_env_prefix}.env",
    },
    "AWS_ACCESS_KEY_ID": {
        "opitem": "Vision Zero CRIS Import - v2",
        "opfield": f"common.AWS_ACCESS_KEY_ID",
    },
    "AWS_SECRET_ACCESS_KEY": {
        "opitem": "Vision Zero CRIS Import - v2",
        "opfield": f"common.AWS_SECRET_ACCESS_KEY",
    },
    "BUCKET_NAME": {
        "opitem": "Vision Zero CRIS Import - v2",
        "opfield": f"common.BUCKET_NAME",
    },
    "EXTRACT_PASSWORD": {
        "opitem": "Vision Zero CRIS Import - v2",
        "opfield": f"common.EXTRACT_PASSWORD",
    },
    "HASURA_GRAPHQL_ENDPOINT": {
        "opitem": "Vision Zero CRIS Import - v2",
        "opfield": f"{secrets_env_prefix}.HASURA_GRAPHQL_ENDPOINT",
    },
    "HASURA_GRAPHQL_ADMIN_SECRET": {
        "opitem": "Vision Zero CRIS Import - v2",
        "opfield": f"{secrets_env_prefix}.HASURA_GRAPHQL_ADMIN_SECRET",
    },
}

docker_image = f"atddocker/vz-cris-import:{'production' if DEPLOYMENT_ENVIRONMENT == 'production' else 'latest'}"


DEFAULT_ARGS = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "execution_timeout": duration(minutes=60),
    "on_failure_callback": task_fail_slack_alert,
}


with DAG(
    catchup=False,
    dag_id="vz-cris-import",
    description="Import TxDOT CRIS CSVs and PDFs into the Vision Zero database",
    default_args=DEFAULT_ARGS,
    schedule="0 5 * * *" if DEPLOYMENT_ENVIRONMENT == "prod" else None,
    start_date=datetime(2024, 8, 1, tz="America/Chicago"),
    tags=["vision-zero", "cris", "repo:atd-vz-data"],
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    cris_import = DockerOperator(
        task_id="run_cris_import",
        docker_conn_id="docker_default",
        image=docker_image,
        command=f"./cris_import.py --csv --pdf --s3-download --s3-upload --s3-archive --workers 7",
        environment=env_vars,
        auto_remove=True,
        tty=True,
        force_pull=True,
    )

    cris_import
