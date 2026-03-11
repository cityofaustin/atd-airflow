# test locally with: docker compose run --rm airflow-cli dags test atd_finance_data_fdu_program_tagging

from os import getenv

from airflow.sdk import task, DAG
from airflow.providers.docker.operators.docker import DockerOperator
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
    "execution_timeout": duration(minutes=60),  # some queries are very slow
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "USER": {
        "opitem": "Finance Data Warehouse Oracle DB",
        "opfield": "production.Username",
    },
    "PASSWORD": {
        "opitem": "Finance Data Warehouse Oracle DB",
        "opfield": "production.Password",
    },
    "HOST": {
        "opitem": "Finance Data Warehouse Oracle DB",
        "opfield": "production.Host",
    },
    "PORT": {
        "opitem": "Finance Data Warehouse Oracle DB",
        "opfield": "production.Port",
    },
    "SERVICE": {
        "opitem": "Finance Data Warehouse Oracle DB",
        "opfield": "production.Service",
    },
    "BUCKET": {
        "opitem": "atd-finance-data",
        "opfield": "production.Bucket",
    },
    "AWS_ACCESS_KEY_ID": {
        "opitem": "atd-finance-data",
        "opfield": "production.Access ID",
    },
    "AWS_SECRET_ACCESS_KEY": {
        "opitem": "atd-finance-data",
        "opfield": "production.Secret Access Key",
    },
    "PROGRAM_DATASET": {
        "opitem": "atd-finance-data",
        "opfield": "production.Programs Dataset ID",
    },
    "SO_USER": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SO_PASS": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SO_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
    "SO_WEB": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.endpoint",
    },
}


with DAG(
    dag_id="atd_finance_data_fdu_program_tagging",
    description="Gets Finance data from a database, places it in an S3 bucket, then classifies the FDU by programs/subprograms then sends the data to socrata.",
    default_args=DEFAULT_ARGS,
    schedule="33 4 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-finance-data", "knack", "socrata", "fdu"],
    catchup=False,
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="fdus_to_s3",
        image="atddocker/atd-finance-data:production",
        docker_conn_id="docker_default",
        auto_remove="force",
        command="python3 upload_to_s3.py fdu_expenses_obligated",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    t2 = DockerOperator(
        task_id="tagging_fdus",
        image="atddocker/atd-finance-data:production",
        docker_conn_id="docker_default",
        auto_remove="force",
        command="python3 fdu_program_tagging.py",
        environment=env_vars,
        tty=True,
        force_pull=False,
        mount_tmp_dir=False,
    )

    t1 >> t2
