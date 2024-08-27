# test locally with: docker compose run --rm airflow-cli dags test atd_finance_data_objects

import os

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

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

# Data tracker Knack app secrets
DATA_TRACKER_SECRETS = {
    "KNACK_APP_ID": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": "production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": "production.apiKey",
    },
}

# Finance purchasing Knack app secrets
FINANCE_PURCHASING_SECRETS = {
    "KNACK_APP_ID": {
        "opitem": "Knack Finance and Purchasing",
        "opfield": "production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack Finance and Purchasing",
        "opfield": "production.apiKey",
    },
}

OTHER_SECRETS = {
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
    "TASK_DATASET": {
        "opitem": "atd-finance-data",
        "opfield": "production.Task Dataset ID",
    },
    "FDU_DATASET": {
        "opitem": "atd-finance-data",
        "opfield": "production.FDU Dataset ID",
    },
    "DEPT_UNITS_DATASET": {
        "opitem": "atd-finance-data",
        "opfield": "production.Department Units Dataset ID",
    },
    "SUBPROJECTS_DATASET": {
        "opitem": "atd-finance-data",
        "opfield": "production.Subprojects Dataset ID",
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

# Combine env vars to create one for each Knack app
DATA_TRACKER_SECRETS.update(OTHER_SECRETS)
FINANCE_PURCHASING_SECRETS.update(OTHER_SECRETS)

with DAG(
    dag_id="atd_finance_data_objects",
    description="Gets Finance data from a database, places it in an S3 bucket, then moves it along to Knack and socrata.",
    default_args=DEFAULT_ARGS,
    schedule_interval="23 7 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-finance-data", "knack", "data-tracker", "socrata"],
    catchup=False,
) as dag:
    data_tracker_env = get_env_vars_task(DATA_TRACKER_SECRETS)
    finance_purchasing_env = get_env_vars_task(FINANCE_PURCHASING_SECRETS)

    t1 = DockerOperator(
        task_id="objects_to_s3",
        image="atddocker/atd-finance-data:production",
        docker_conn_id="docker_default",
        auto_remove=True,
        command="python3 upload_to_s3.py objects",
        environment=data_tracker_env,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    t2 = DockerOperator(
        task_id="objects_to_finance_purchasing",
        image="atddocker/atd-finance-data:production",
        docker_conn_id="docker_default",
        auto_remove=True,
        command="python3 s3_to_knack.py objects finance-purchasing",
        environment=finance_purchasing_env,
        tty=True,
        force_pull=False,
        mount_tmp_dir=False,
    )

    t1 >> t2
