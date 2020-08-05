"""
    Description: script to get daily Census 2020 response rates for Austin MSA census Tracts
"""
from datetime import timedelta

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago

from _slack_operators import *

# First, load our environment variables as a dictionary
environment_vars = Variable.get("atd_census_2020", deserialize_json=True)

args = {
    "owner": "airflow",
    "start_date": days_ago(2),
    "email": [environment_vars.get("EMAIL_RECIPIENT", "")],
    "email_on_failure": True,
    "on_failure_callback": task_fail_slack_alert,
}

#
# Set up our dag
#
dag = DAG(
    dag_id="atd_census_download",
    description="Script to get daily Census 2020 response rates for Austin MSA census Tracts and publish to Socrata",
    default_args=args,
    schedule_interval="05 23 * * *",
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

run_python

if __name__ == "__main__":
    dag.cli()
