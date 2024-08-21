import os
from pendulum import datetime, duration

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert


DEPLOYMENT_ENVIRONMENT = "prod" if os.getenv("ENVIRONMENT") == "production" else "dev"

REQUIRED_SECRETS = {
    "ENV": {
        "opitem": "Vision Zero CRIS Import - v2",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.env",
    },
    "AWS_ACCESS_KEY_ID": {
        "opitem": "Vision Zero CRIS Import - v2",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.AWS_ACCESS_KEY_ID",
    },
    "AWS_SECRET_ACCESS_KEY": {
        "opitem": "Vision Zero CRIS Import - v2",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.AWS_SECRET_ACCESS_KEY",
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
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.HASURA_GRAPHQL_ENDPOINT",
    },
    "HASURA_GRAPHQL_ADMIN_SECRET": {
        "opitem": "Vision Zero CRIS Import - v2",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.HASURA_GRAPHQL_ADMIN_SECRET",
    },
}

docker_image = f"atddocker/vz-cris-import:{'production' if DEPLOYMENT_ENVIRONMENT == 'prod' else 'latest'}"


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
