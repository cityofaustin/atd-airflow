from os import getenv
from datetime import datetime as vanilla_datetime

from pendulum import datetime, duration

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "description": "Backup, compress and store airflow pg_dump in S3",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=60),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "AWS_ACCESS_KEY_ID": {
        "opitem": "AWS atd-airflow IAM user API credentials",
        "opfield": "production.access key",
    },
    "AWS_SECRET_ACCESS_KEY": {
        "opitem": "AWS atd-airflow IAM user API credentials",
        "opfield": "production.secret key",
    },
}

with DAG(
    dag_id=f"airflow_db_backup_{DEPLOYMENT_ENVIRONMENT}",
    default_args=default_args,
    # 3:00 AM central
    schedule="0 3 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-airflow", "backup"],
    catchup=False,
    doc_md="""
### Airflow metadata database backup

This DAG dumps the Airflow Postgres database, compresses the output with bzip2,
and uploads the archive to S3 under a date-based key.

#### Credentials and runtime environment

AWS credentials for the upload are loaded at runtime from 1Password Connect
via 'utils.onepassword', using the mapping in 'REQUIRED_SECRETS'.

Task 'backup_airflow_db' runs 'pg_dump' inside the Docker container
'atd-airflow-postgres-1' (user and database 'airflow'), pipes through
'bzip2 -9', and streams to 's3://atd-airflow/$current_date/airflow-db-backup.bz2'
using the AWS CLI. The 'current_date' value is set in an upstream task so the
object lands in a per-day prefix.
""",
) as dag:

    @task()
    def get_env_vars():
        """
        Load secrets required for the backup upload from 1Password Connect.

        Returns:
            dict: Environment-style key/value pairs (e.g. AWS access key and
                secret) as resolved from 'REQUIRED_SECRETS'.
        """
        from utils.onepassword import load_dict

        return load_dict(REQUIRED_SECRETS)

    @task()
    def add_todays_date_to_dict(secrets):
        """
        Add today's calendar date to the secrets dict for S3 path layout.

        Args:
            secrets (dict): Secret key/value pairs from 'get_env_vars'.

        Returns:
            dict: The same dict with 'current_date' set to 'YYYY-MM-DD' (local
                date from the task runtime).
        """
        secrets["current_date"] = vanilla_datetime.today().strftime("%Y-%m-%d")
        return secrets

    env_vars = get_env_vars()
    env_vars = add_todays_date_to_dict(env_vars)

    BashOperator(
        task_id="backup_airflow_db",
        env=env_vars,
        bash_command=f"docker exec -i atd-airflow-postgres-1 pg_dump -U airflow airflow | bzip2 -9 | AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY aws s3 cp - s3://atd-airflow/$current_date/airflow-db-backup.bz2",
        doc_md="""
Dump the Airflow Postgres database from the 'atd-airflow-postgres-1' container,
compress with bzip2, and upload to the dated key under 's3://atd-airflow/'.
AWS credentials come from the task 'env' (1Password-backed upstream tasks).
""",
    )
