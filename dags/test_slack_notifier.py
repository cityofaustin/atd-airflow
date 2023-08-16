import os
from pendulum import datetime, duration

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "description": "Test if the Slack notifier is working",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}


def task_fail():
    raise Exception("Task failure test successfully triggered")


with DAG(
    dag_id=f"test_slack_notifier_{DEPLOYMENT_ENVIRONMENT}",
    default_args=default_args,
    schedule_interval=None,
    tags=["slack"],
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="task_fail",
        python_callable=task_fail,
    )

    t1
