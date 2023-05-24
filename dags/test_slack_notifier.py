import os
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "description": "Test if the Slack notifier is working",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
}


def task_fail():
    raise Exception("Task failed")


with DAG(
    dag_id=f"test_slack_notifier_{DEPLOYMENT_ENVIRONMENT}",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=[DEPLOYMENT_ENVIRONMENT, "slack"],
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="task_fail",
        python_callable=task_fail,
    )

    t1
