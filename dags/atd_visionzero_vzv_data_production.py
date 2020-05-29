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

# We need both, one contains the Hasura endpoint
# the other contains the query we need to download the data
hasura_env_vars = Variable.get("atd_visionzero_cris_production", deserialize_json=True)
vzv_data_query = Variable.get("atd_visionzero_vzv_query", deserialize_json=True)
environment_vars = {**hasura_env_vars, **vzv_data_query}

args = {
    "owner": "airflow",
    "start_date": days_ago(2),
    "email": [environment_vars.get("EMAIL_RECIPIENT", "")],
    "email_on_failure": True,
    "on_failure_callback": task_fail_slack_alert
}

#
# Set up our dag
#
dag = DAG(
    dag_id="atd_visionzero_vzv_data_production",
    description="Uploads and Updates Vision Zero Viewer Data",
    default_args=args,
    schedule_interval="0 9 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "visionzero"],
)

#
# A quick successful test
#
download_data = BashOperator(
    task_id="download_data",
    bash_command='curl $HASURA_ENDPOINT --data "$VZV_DATA_QUERY" > $VZV_DATA_FILENAME',
    env=environment_vars,
    dag=dag,
)


#
# Extract view_vzv_header_totals
#
view_vzv_header_totals = BashOperator(
    task_id="view_vzv_header_totals",
    bash_command='cat $VZV_DATA_FILENAME | jq -r ".data.view_vzv_header_totals" | in2csv -f json -v --no-inference > view_vzv_header_totals.csv',
    env=environment_vars,
    dag=dag,
)

#
# Send an email when done
#
email_task = EmailOperator(
    to=environment_vars.get("EMAIL_RECIPIENT", ""),
    task_id="email_task",
    subject="Templated Subject: start_date {{ ds }}",
    mime_charset="utf-8",
    params={"content1": "random"},
    html_content="Templated Content: content1 - {{ params.content1 }}  task_key - {{ task_instance_key_str }} test_mode - {{ test_mode }} task_owner - {{ task.owner}} hostname - {{ ti.hostname }}",
    dag=dag,
)

download_data >> \
view_vzv_header_totals >> \
email_task

if __name__ == "__main__":
    dag.cli()