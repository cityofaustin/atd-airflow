import os
import pendulum

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.utils.log.logging_mixin import LoggingMixin

from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT")


default_task_args = {
    "owner": "airflow",
    "description": "Clean up old Airflow metadata database records",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2015, 12, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
}


@task(task_id="get_days_back_to_prune")
def get_days_back_to_prune(days_back_to_prune: int):
    """Task to retrieve the number of days to prune back."""
    prune_before_days = int(days_back_to_prune)
    logger = LoggingMixin().log
    logger.info(f"Pruning Airflow DB records older than {prune_before_days} days")
    return prune_before_days


@task(task_id="get_clean_before_timestamp")
def get_clean_before_timestamp(prune_before_days: int):
    """Task to calculate the timestamp to prune before."""
    clean_before_timestamp = (
        pendulum.now("America/Chicago") - pendulum.duration(days=prune_before_days)
    ).to_iso8601_string()
    logger = LoggingMixin().log
    logger.info(
        f"Pruning records before {pendulum.parse(clean_before_timestamp).to_datetime_string()} America/Chicago, ISO8601: {clean_before_timestamp}"
    )
    return clean_before_timestamp


@task.bash(task_id="airflow_db_clean")
def db_clean_bash(timestamp: str) -> str:
    cmd = f'airflow db clean --yes --clean-before-timestamp "{timestamp}"'
    logger = LoggingMixin().log
    logger.info(f"Running command: {cmd}")
    return cmd


@task.bash(task_id="airflow_log_file_cleanup")
def log_file_cleanup_bash(day_interval: int) -> str:
    cmd = (
        f'find /opt/airflow/logs/ -type f -name "*.log" -mtime +{day_interval} -delete'
    )
    logger = LoggingMixin().log
    logger.info(f"Running command: {cmd}")
    return cmd


@task.bash(task_id="airflow_log_dir_cleanup")
def log_dir_cleanup_bash() -> str:
    cmd = "find /opt/airflow/logs/ -type d -empty -delete"
    logger = LoggingMixin().log
    logger.info(f"Running command: {cmd}")
    return cmd


@dag(
    dag_id="airflow_purge_logs_prune_database",
    default_args=default_task_args,
    schedule_interval="0 0 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-airflow", "airflow", "maintenance"],
    catchup=False,
    params={"days_back_to_prune": Param(default=30, type="integer", minimum=15)},
)
def airflow_purge_logs_prune_database():

    prune_before_days = get_days_back_to_prune(
        days_back_to_prune="{{ params.days_back_to_prune }}"
    )

    clean_before_timestamp = get_clean_before_timestamp(prune_before_days)

    db_clean = db_clean_bash(clean_before_timestamp)

    log_file_cleanup = log_file_cleanup_bash(prune_before_days)

    log_dir_cleanup = log_dir_cleanup_bash()

    (
        prune_before_days
        >> clean_before_timestamp
        >> db_clean
        >> log_file_cleanup
        >> log_dir_cleanup
    )


dag = airflow_purge_logs_prune_database()
