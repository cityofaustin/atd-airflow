"""
   Gathers data for MDS report and uploads to knack
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator

from _slack_operators import *

default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Gathers MDS data and uploads to Knack',
        'depend_on_past'        : False,
        'start_date'            : datetime(2018, 1, 1),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(minutes=5),
        'on_failure_callback'   : task_fail_slack_alert,
}

environment_vars = Variable.get("atd_mds_monthly_report_production", deserialize_json=True)

with DAG(
        "atd_mds",
        default_args=default_args,
        schedule_interval="40 7 3 * *",
        catchup=False,
        tags=["production", "mds"],
) as dag:
        #
        # Task: run_python
        # Description: Gathers data from Hasura to generate report data and uploads to Knack.
        #
	run_python = BashOperator(
	    task_id="run_python_script",
	    bash_command="python3 ~/dags/python_scripts/atd_mds_monthly_report.py",
	    env=environment_vars,
	    dag=dag,
	)

        #
        # Task: provider_sync_db
        # Description: Emails users whenever the process is complete.
        #
	email_task = EmailOperator(
	    to=environment_vars.get("email_recipients", ""),
	    task_id="email_admin",
	    subject="MDS Data Inserted in Knack: {{ ds }}",
	    mime_charset="utf-8",
	    html_content="The MDS Data has been inserted into Knack without errors",
	    dag=dag,
	)

	run_python >> email_task
