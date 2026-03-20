import logging
from os import getenv
from pendulum import datetime
from datetime import timedelta

from airflow.sdk import Param, dag, task
from airflow.exceptions import AirflowException

from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT")
logger = logging.getLogger(__name__)


default_task_args = {
    "owner": "airflow",
    "description": "Clean up old Airflow metadata database records",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
}


@task(task_id="get_days_back_to_prune")
def get_days_back_to_prune(days_back_to_prune: str):
    """
    Retrieve the number of days to prune back from the Airflow database.

    Args:
        days_back_to_prune (str): Number of days to look back for pruning.

    Returns:
        int: The number of days to prune back.
    """
    try:
        prune_before_days = int(days_back_to_prune)
    except ValueError:
        raise AirflowException(
            f"Invalid 'days_back_to_prune' parameter: '{days_back_to_prune}'. Must be an integer."
        )
    logger.info(f"Pruning Airflow DB records older than {prune_before_days} days")
    return prune_before_days


@task(task_id="get_clean_before_timestamp")
def get_clean_before_timestamp(prune_before_days: int):
    """
    Calculate the ISO8601 timestamp before which records should be pruned.

    Args:
        prune_before_days (int): Number of days to look back for pruning.

    Returns:
        str: ISO8601 formatted timestamp.
    """
    from pendulum import duration, now, parse

    clean_before_timestamp = (
        now("America/Chicago") - duration(days=prune_before_days)
    ).to_iso8601_string()
    logger.info(
        f"Pruning records before {parse(clean_before_timestamp).to_datetime_string()} America/Chicago, ISO8601: {clean_before_timestamp}"
    )
    return clean_before_timestamp


@task.bash(task_id="airflow_db_clean")
def db_clean_bash(timestamp: str) -> str:
    """
    Generate the bash command to clean the Airflow database before a given timestamp.

    Args:
        timestamp (str): ISO8601 formatted timestamp.

    Returns:
        str: Bash command string.
    """
    cmd = (
        "python3 /opt/airflow/toolbox/airflow_metadata_db/run_db_clean.py "
        f'--clean-before-timestamp "{timestamp}"'
    )
    logger.info(f"Running command: {cmd}")
    return cmd


@task.bash(task_id="airflow_log_file_cleanup")
def log_file_cleanup_bash(day_interval: int) -> str:
    """
    Generate the bash command to delete Airflow log files older than a given number of days.

    Args:
        day_interval (int): Number of days; logs older than this will be deleted.

    Returns:
        str: Bash command string.
    """
    cmd = (
        f'find /opt/airflow/logs/ -type f -name "*.log" -mtime +{day_interval} -delete'
    )
    logger.info(f"Running command: {cmd}")
    return cmd


@task.bash(task_id="airflow_log_dir_cleanup")
def log_dir_cleanup_bash() -> str:
    """
    Generate the bash command to delete empty directories in the Airflow logs directory.

    Returns:
        str: Bash command string.
    """
    cmd = "find /opt/airflow/logs/ -type d -empty -delete"
    logger.info(f"Running command: {cmd}")
    return cmd


@dag(
    dag_id="airflow_purge_logs_prune_database",
    default_args=default_task_args,
    schedule="0 0 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-airflow", "airflow", "maintenance"],
    catchup=False,
    doc_md="""
### Airflow metadata and log cleanup

This DAG prunes old Airflow metadata and rotates filesystem logs.

#### Why a wrapper utility is used for DB cleanup

In Airflow 3, task runtime is isolated from direct metadata DB access, and tasks
may see a blocked URL such as 'airflow-db-not-allowed:///'. Running
'airflow db clean' directly in a normal task can therefore fail.

To avoid that, task 'airflow_db_clean' runs:

'python3 /opt/airflow/toolbox/airflow_metadata_db/run_db_clean.py'

That utility invokes 'airflow db clean' with an explicit metadata DB URL via:

'AIRFLOW_DB_CLEAN_SQL_ALCHEMY_CONN'

In compose, both DB env vars map to one source value
('AIRFLOW_METADATA_DB_SQL_ALCHEMY_CONN') to keep configuration DRY.
""",
    params={
        "days_back_to_prune": Param(
            default=30,
            type="integer",
            minimum=15,
            title="Days Back To Prune",
            description="Delete metadata/log records older than this many days.",
        )
    },
    dagrun_timeout=timedelta(minutes=10),
)
def airflow_purge_logs_prune_database():

    prune_before_days = get_days_back_to_prune(
        days_back_to_prune="{{ params.days_back_to_prune }}"
    )
    clean_before_timestamp = get_clean_before_timestamp(prune_before_days)
    db_clean = db_clean_bash(clean_before_timestamp)
    log_file_cleanup = log_file_cleanup_bash(prune_before_days)
    log_dir_cleanup = log_dir_cleanup_bash()

    # expressly defining the order of execution to be serial beyond what can be
    # inferred from the task dependencies. the intent is to spread out io load
    (
        prune_before_days
        >> clean_before_timestamp
        >> db_clean
        >> log_file_cleanup
        >> log_dir_cleanup
    )


airflow_purge_logs_prune_database()
