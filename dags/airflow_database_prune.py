import os
import pendulum

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models import Param

from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT")


default_args = {
    "owner": "airflow",
    "description": "Clean up old Airflow metadata database records",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2015, 12, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # "on_failure_callback": task_fail_slack_alert, # keep this comment
    "execution_timeout": pendulum.duration(minutes=10),
}


@task(task_id="get_parameters")
def get_parameters(days_back_to_prune: int):
    """Task to retrieve parameters from the DAG."""
    prune_before_days = int(days_back_to_prune)
    clean_before_timestamp = (
        pendulum.now("America/Chicago") - pendulum.duration(days=prune_before_days)
    ).to_iso8601_string()
    return clean_before_timestamp


@dag(
    dag_id="airflow_database_prune",
    default_args=default_args,
    schedule_interval="10 4 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-airflow", "airflow", "maintenance"],
    catchup=False,
    params={"days_back_to_prune": Param(default=30, type="integer", minimum=15)},
)
def airflow_database_prune():

    parameters = get_parameters(days_back_to_prune="{{ params.days_back_to_prune }}")

    db_clean = BashOperator(
        task_id="airflow_db_clean",
        bash_command=(
            "airflow db clean --yes --clean-before-timestamp '{{ ti.xcom_pull(task_ids='get_parameters') }}'"
        ),
    )

    parameters >> db_clean


dag = airflow_database_prune()
