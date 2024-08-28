import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration, now

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
    "execution_timeout": duration(minutes=50),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    # Socrata
    "SO_KEY": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SO_SECRET": {
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
    # Microstrategy
    "MSTRO_USERNAME": {
        "opitem": "Microstrategy API",
        "opfield": "shared.Username",
    },
    "MSTRO_PASSWORD": {
        "opitem": "Microstrategy API",
        "opfield": "shared.Password",
    },
    "BASE_URL": {
        "opitem": "Microstrategy API",
        "opfield": "shared.Base URL",
    },
    "PROJECT_ID": {
        "opitem": "Microstrategy API",
        "opfield": "shared.Project ID",
    },
    "EXPENSES_DATASET": {
        "opitem": "atd-bond-reporting",
        "opfield": "production.Expenses Dataset",
    },
    "SUBPROJECT_DATASET": {
        "opitem": "atd-bond-reporting",
        "opfield": "production.Subproject Dataset",
    },
    "FDU_DATASET": {
        "opitem": "atd-bond-reporting",
        "opfield": "production.FDU Dataset",
    },
    # S3
    "BUCKET": {
        "opitem": "atd-bond-reporting",
        "opfield": "production.S3 Bucket",
    },
    "AWS_PASS": {
        "opitem": "atd-bond-reporting",
        "opfield": "production.AWS Secret Access Key",
    },
    "AWS_ACCESS_ID": {
        "opitem": "atd-bond-reporting",
        "opfield": "production.AWS Access ID",
    },
    # PostgREST
    "POSTGREST_TOKEN": {
        "opitem": "atd-bond-reporting",
        "opfield": "production.Postgrest JWT",
    },
    "POSTGREST_ENDPOINT": {
        "opitem": "atd-bond-reporting",
        "opfield": "production.Postgrest Endpoint",
    },
}


with DAG(
    dag_id="atd_bond_reporting",
    description="Wrapper ETL for the atd-bond-reporting docker image with commands for moving the data from S3 to Socrata.",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 9 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-bond-reporting", "socrata", "microstrategy"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-bond-reporting:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="microstrategy_report_2020_bond_expenses",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command='python atd-bond-reporting/microstrategy_to_s3.py -r "2020 Bond Expenses Obligated"',
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
    )

    t2 = DockerOperator(
        task_id="microstrategy_report_all_bonds_expenses",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command='python atd-bond-reporting/microstrategy_to_s3.py -r "All bonds Expenses Obligated"',
        environment=env_vars,
        tty=True,
        force_pull=False,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
    )

    t3 = DockerOperator(
        task_id="microstrategy_report_fdu_expenses_quarterly",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command='python atd-bond-reporting/microstrategy_to_s3.py -r "FDU Expenses by Quarter"',
        environment=env_vars,
        tty=True,
        force_pull=False,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
    )

    t4 = DockerOperator(
        task_id="microstrategy_report_2020_bond_metadata",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command='python atd-bond-reporting/microstrategy_to_s3.py -r "2020 Division Group and Unit"',
        environment=env_vars,
        tty=True,
        force_pull=False,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
    )

    t5 = DockerOperator(
        task_id="bond_data_to_postgres",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command="python atd-bond-reporting/bond_data.py",
        environment=env_vars,
        tty=True,
        force_pull=False,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
    )

    t6 = DockerOperator(
        task_id="bond_data_processing",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command="python atd-bond-reporting/bond_calculations.py",
        environment=env_vars,
        tty=True,
        force_pull=False,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
    )

    t7 = DockerOperator(
        task_id="bond_quarterly_data_processing",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove=True,
        command="python atd-bond-reporting/quarterly_reporting.py",
        environment=env_vars,
        tty=True,
        force_pull=False,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
