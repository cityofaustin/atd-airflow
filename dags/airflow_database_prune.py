import os
import pendulum

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Param

from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT")

# Configuration: number of days to keep in the Airflow DB
AIRFLOW_DB_PRUNE_DAYS = int(os.getenv("AIRFLOW_DB_PRUNE_DAYS", "30"))

# Calculate the timestamp N days ago in ISO format
CLEAN_BEFORE_TIMESTAMP = (
    pendulum.now("America/Chicago") - pendulum.duration(days=AIRFLOW_DB_PRUNE_DAYS)
).to_iso8601_string()

default_args = {
    "owner": "airflow",
    "description": "Clean up old Airflow metadata database records",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2015, 12, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
    "execution_timeout": pendulum.duration(minutes=10),
}

with DAG(
    dag_id="airflow_database_prune",
    default_args=default_args,
    schedule_interval="10 4 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-airflow", "airflow", "maintenance"],
    catchup=False,
    params={"days_back_to_prune": Param(default=30, type="integer", minimum=15)},
) as dag:
    t1 = BashOperator(
        task_id="airflow_db_clean",
        bash_command=f"airflow db clean --yes --clean-before-timestamp '{CLEAN_BEFORE_TIMESTAMP}'",
    )

    t1
