# test locally with: docker compose run --rm airflow-cli dags test atd_parking_data
import os

from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models import DAG
from airflow.models.dagrun import DagRun
from airflow.models.param import Param
from airflow.models.taskinstance import TaskInstance
from airflow.operators.docker_operator import DockerOperator

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
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
    "FISERV_DATASET": {
        "opitem": "Parking Data ETL",
        "opfield": "socrata.Fiserv Payments Dataset",
    },
    "METERS_DATASET": {
        "opitem": "Parking Data ETL",
        "opfield": "socrata.Parking Meters Dataset",
    },
    "PAYMENTS_DATASET": {
        "opitem": "Parking Data ETL",
        "opfield": "socrata.Parking Meter Credit Card Payments Dataset",
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
    # Passport
    "OPS_MAN_USER": {
        "opitem": "Parking Data ETL",
        "opfield": "passport.Username",
    },
    "OPS_MAN_PASS": {
        "opitem": "Parking Data ETL",
        "opfield": "passport.Password",
    },
    # Fiserv
    "FSRV_EMAIL": {
        "opitem": "Parking Data ETL",
        "opfield": "fiserv.Expected Email Address",
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
    "USER_PARD": {
        "opitem": "Parking Data ETL",
        "opfield": "flowbird.PARD Username",
    },
    "PASSWORD_PARD": {
        "opitem": "Parking Data ETL",
        "opfield": "flowbird.PARD Password",
    },
}

docker_commands = [
    "python txn_history.py -v --report transactions",
    "python txn_history.py -v --report payments",
    "python txn_history.py -v --report payments --user pard",
    "python passport_txns.py -v",
    "python fiserv_email_pub.py",
    "python fiserv_DB.py",
    "python payments_s3.py",
    "python payments_s3.py --user pard",
    "python passport_DB.py",
    "python smartfolio_s3.py",
    "python match_field_processing.py",
    "python parking_socrata.py --dataset payments",
    "python parking_socrata.py --dataset fiserv",
    "python parking_socrata.py --dataset transactions",
]

# Task names for the above docker commands
task_ids = [
    "txn_history_report_transactions",
    "txn_history_report_payments",
    "txn_history_report_payments_user_pard",
    "passport_txns",
    "fiserv_email_pub",
    "fiserv_DB",
    "payments_s3",
    "payments_s3_user_pard",
    "passport_DB",
    "smartfolio_s3",
    "match_field_processing",
    "parking_socrata_dataset_payments",
    "parking_socrata_dataset_fiserv",
    "parking_socrata_dataset_transactions",
]


@task
def decide_prev_month(prev_exec):
    """
    Determines if the current month or the current plus previous month S3
        folders are needed. If it is within a week of the previous month,
        also upsert that months data.
    Parameters
    ----------
    prev_execution_date_success : String
        Last date the flow was successful.

    Returns
    -------
    Prev_month : Bool
        Argument if the previous month should be run.

    """
    last_date = datetime.strptime(prev_exec, "%Y-%m-%d")
    # If in the first 8 days of the month of last few days of the month re-run
    # the previous month's data to make sure it is complete.
    if last_date.day < 8 or last_date.day > 26:
        prev_month = True
    else:
        prev_month = False


@task
def add_command_arguments(commands, prev_exec, prev_month):
    # Adding parameters to to our docker commands to include the previous exec date
    # and wether or not to run the previous month

    output_tasks = []
    for c in commands:
        if "txn_history.py" in c:
            c = f"{c} --start {prev_exec}"

        if "passport_txns.py" in c:
            c = f"{c} --start {prev_exec}"

        if "fiserv_DB.py" in c:
            c = f"{c} --lastmonth {prev_month}"

        if "payments_s3.py" in c:
            c = f"{c} --lastmonth {prev_month}"

        if "passport_DB.py" in c:
            c = f"{c} --lastmonth {prev_month}"

        if "smartfolio_s3.py" in c:
            c = f"{c} --lastmonth {prev_month}"

        output_tasks.append(c)
    return output_tasks


with DAG(
    dag_id="atd_parking_data",
    description="Scripts that download and process parking data for finance reporting.",
    default_args=default_args,
    schedule_interval="35 8 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=timedelta(minutes=60 * 4),
    tags=["repo:atd-parking-data", "parking", "socrata", "postgrest"],
    catchup=False,
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)
    prev_exec = "{{ (prev_start_date_success - macros.timedelta(days=7)).strftime('%Y-%m-%d') if prev_start_date_success else '2023-08-01'}}"

    prev_month = decide_prev_month(prev_exec)

    tasks = add_command_arguments(docker_commands, prev_exec, prev_month)

    docker_tasks = []
    for i, docker_command in enumerate(docker_commands):
        docker_task = DockerOperator(
            task_id=task_ids[i],
            image=docker_image,
            command=docker_command,
            api_version="auto",
            auto_remove=True,
            environment=env_vars,
            dag=dag,
            tty=True,
            force_pull=False,
        )
        docker_tasks.append(docker_task)

    # Set up the dependencies between tasks
    for i in range(1, len(docker_tasks)):
        docker_tasks[i - 1] >> docker_tasks[i]
