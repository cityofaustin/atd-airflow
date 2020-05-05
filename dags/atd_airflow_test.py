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
environment_vars = Variable.get("atd_airflow_test", deserialize_json=True)

args = {
    "owner": "airflow",
    "start_date": days_ago(2),
    "email": [environment_vars.get("EMAIL_RECIPIENT", "")],
    "email_on_failure": True,
    "on_failure_callback": task_fail_slack_alert,
    "on_success_callback": task_success_slack_alert
}

#
# Set up our dag
#
dag = DAG(
    dag_id="atd_airflow_test",
    description="Script to test airflow functionality",
    default_args=args,
    schedule_interval="05 23 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "census"],
)

#
# A quick successful test
#
run_success = BashOperator(
    task_id="trigger_success",
    bash_command="echo $(date)",
    env=environment_vars,
    dag=dag,
)


#
# A bad command we need failing
#
run_fail = BashOperator(
    task_id="trigger_failure",
    bash_command="invalidcommand",
    env=environment_vars,
    dag=dag,
)

#
# Send an email when done
#
email_task = EmailOperator(
    to=environment_vars.get("EMAIL_RECIPIENT", ""),
    task_id="email_admin",
    subject="Templated Subject: start_date {{ ds }}",
    mime_charset="utf-8",
    params={"content1": "random"},
    html_content="Templated Content: content1 - {{ params.content1 }}  task_key - {{ task_instance_key_str }} test_mode - {{ test_mode }} task_owner - {{ task.owner}} hostname - {{ ti.hostname }}",
    dag=dag,
)

run_success >> run_fail >> email_task

if __name__ == "__main__":
    dag.cli()
