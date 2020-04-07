"""
    Description: script to get daily Census 2020 response rates for Austin MSA census Tracts
"""
from datetime import timedelta

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago


# First, load our environment variables as a dictionary
environment_vars = Variable.get("atd_census_2020", deserialize_json=True)

args = {
    "owner": "airflow",
    "start_date": days_ago(2),
    'email': [environment_vars.get("EMAIL_RECIPIENT", "")],
    'email_on_failure': True,
}

#
# Set up our dag
#
dag = DAG(
    dag_id="atd_census_download",
    description="Script to get daily Census 2020 response rates for Austin MSA census Tracts and publish to Socrata",
    default_args=args,
    schedule_interval="0 17 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "census"],
)

#
# Our python code execution
#
run_python = BashOperator(
    task_id="run_census_script",
    bash_command="python3 ~/dags/python_scripts/census2020.py",
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

run_python >> email_task

if __name__ == "__main__":
    dag.cli()
