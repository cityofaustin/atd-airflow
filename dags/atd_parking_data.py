# test locally with: docker compose run --rm airflow-cli dags test atd_parking_data
from os import getenv
from datetime import timedelta

from airflow.sdk import task, DAG, Param
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime, duration, parse, now

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert
from utils.time import get_previous_success_start_time

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=60 * 5),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-parking-data-meters:production"

REQUIRED_SECRETS = {
    # Socrata
    "SO_WEB": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.endpoint",
    },
    "SO_PASS": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SO_USER": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SO_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
    "TXNS_DATASET": {
        "opitem": "Parking Data ETL",
        "opfield": "socrata.Transactions Dataset",
    },
    # PostgREST
    "POSTGREST_TOKEN": {
        "opitem": "Parking Data ETL",
        "opfield": "postgrest.Token",
    },
    "POSTGREST_ENDPOINT": {
        "opitem": "Parking Data ETL",
        "opfield": "postgrest.Endpoint",
    },
    # AWS S3
    "AWS_ACCESS_ID": {
        "opitem": "Parking Data ETL",
        "opfield": "aws.Access ID",
    },
    "AWS_ACCESS_ID": {
        "opitem": "Parking Data ETL",
        "opfield": "aws.Access ID",
    },
    "AWS_ACCESS_KEY_ID": {
        "opitem": "Parking Data ETL",
        "opfield": "aws.Access ID",
    },
    "AWS_SECRET_ACCESS_KEY": {
        "opitem": "Parking Data ETL",
        "opfield": "aws.Secret Access Key",
    },
    "AWS_PASS": {
        "opitem": "Parking Data ETL",
        "opfield": "aws.Secret Access Key",
    },
    "BUCKET": {
        "opitem": "Parking Data ETL",
        "opfield": "aws.Bucket Name",
    },
    "BUCKET_NAME": {
        "opitem": "Parking Data ETL",
        "opfield": "aws.Bucket Name",
    },
    # Flowbird
    "ENDPOINT": {
        "opitem": "Parking Data ETL",
        "opfield": "flowbird.Endpoint",
    },
    "USER": {
        "opitem": "Parking Data ETL",
        "opfield": "flowbird.ATD Username",
    },
    "PASSWORD": {
        "opitem": "Parking Data ETL",
        "opfield": "flowbird.ATD Password",
    },
}

@task
def format_start_date(prev) -> str:
    return parse(prev).format("YYYY-MM-DD")

with DAG(
    dag_id="atd_parking_data",
    description="Scripts that download and process parking data.",
    default_args=default_args,
    schedule="35 8 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-parking-data", "parking", "socrata", "postgrest"],
    catchup=False,
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)
    three_days_ago = now("America/Chicago").subtract(days=3)
    prev = get_previous_success_start_time(fallback_date=three_days_ago.to_iso8601_string())
    prev_exec = format_start_date(prev)

    docker_tasks = []
    docker_tasks.append(
        DockerOperator(
            task_id="smartfolio_transactions",
            image=docker_image,
            docker_conn_id="docker_default",
            command=f"python txn_history.py -v --report transactions --env prod --start {prev_exec}",
            api_version="auto",
            auto_remove="force",
            environment=env_vars,
            tty=True,
            force_pull=True,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    docker_tasks.append(
        DockerOperator(
            task_id="process_smartfolio_transactions",
            image=docker_image,
            docker_conn_id="docker_default",
            command=f"python smartfolio_s3.py --lastmonth True",
            api_version="auto",
            auto_remove="force",
            environment=env_vars,
            tty=True,
            force_pull=False,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    docker_tasks.append(
        DockerOperator(
            task_id="transactions_to_socrata",
            image=docker_image,
            docker_conn_id="docker_default",
            command=f"python parking_socrata.py --dataset transactions",
            api_version="auto",
            auto_remove="force",
            environment=env_vars,
            tty=True,
            force_pull=False,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    # All tasks will run sequentially
    for i in range(1, len(docker_tasks)):
        docker_tasks[i - 1] >> docker_tasks[i]
