"""
    Description: A script to test Slack Integration
"""
from datetime import timedelta

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago

from _slack_operators import *

# First, load our environment variables as a dictionary
environment_vars = Variable.get("atd_mds_monthly_report_production", deserialize_json=True)

args = {
    "owner": "airflow",
    "start_date": days_ago(2),
    "email": [environment_vars.get("email_recipients", "")],
    "email_on_failure": True,
    "on_failure_callback": task_fail_slack_alert,
    "on_success_callback": task_success_slack_alert,
}

#
# Set up our dag
#
dag = DAG(
    dag_id="atd_mds_monthly_report",
    description="Gathers the data from MDS and submits to Knack",
    default_args=args,
    schedule_interval="0 18 2 * *",  # (3rd day of the month, at midnight UTC)
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "mds"],
)

#
# A quick successful test
#
run_python = BashOperator(
    task_id="run_census_script",
    bash_command="python3 ~/dags/python_scripts/atd_mds_monthly_report.py",
    env=environment_vars,
    dag=dag,
)

#
# Send an email when done
#
email_task = EmailOperator(
    to=environment_vars.get("email_recipients", ""),
    task_id="email_task",
    subject="MDS Data Inserted in Knack: start_date {{ ds }}",
    mime_charset="utf-8",
    params={"content1": "random"},
    html_content="The MDS Data has been inserted into Knack without errors. Task Random ID: {{ params.content1 }}  task_key - {{ task_instance_key_str }} test_mode - {{ test_mode }} task_owner - {{ task.owner}} hostname - {{ ti.hostname }}",
    dag=dag,
)

run_python >> email_task

if __name__ == "__main__":
    dag.cli()
