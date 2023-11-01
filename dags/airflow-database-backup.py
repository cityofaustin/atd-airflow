import os
from pendulum import datetime, duration

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import datetime as vanilla_datetime

from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "description": "Backup, compress and store airflow pg_dump in S3",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=30),
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
    schedule_interval="0 3 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-airflow", "backup"],
    catchup=False,
) as dag:

    @task()
    def get_env_vars():
        from utils.onepassword import load_dict
        return load_dict(REQUIRED_SECRETS)

    @task()
    def add_todays_date_to_dict(secrets):
        secrets["current_date"] = vanilla_datetime.today().strftime('%Y-%m-%d')
        return secrets

    env_vars = get_env_vars()
    env_vars = add_todays_date_to_dict(env_vars)

    BashOperator(
      task_id="backup_airflow_db",
      env=env_vars,
      bash_command=f"docker exec -i atd-airflow-postgres-1 pg_dump -U airflow airflow | bzip2 -9 | AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY aws s3 cp - s3://atd-airflow/$current_date_airflow-db-backup.bz2"
    )
