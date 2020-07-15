"""
    Description: A script to test Slack Integration
"""
import uuid
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
vzv_data_query = Variable.get("atd_visionzero_vzv_query_production", deserialize_json=True)
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
    bash_command='curl --header "Content-Type: application/json" ' +
        '--header "x-hasura-admin-secret: $HASURA_ADMIN_KEY" ' +
        '--data "{ \\"query\\": \\"$VZV_DATA_QUERY\\"}" ' +
        '"$HASURA_ENDPOINT" > $VZV_DATA_FILENAME && ' +
        'aws s3 cp $VZV_DATA_FILENAME s3://$VZV_DATA_BUCKET/'
    ,
    env=environment_vars,
    dag=dag,
)

#
# Transforms JSON to multiple CSV files
#
transform_to_csv = BashOperator(
    task_id="transform_to_csv",
    bash_command='aws s3 cp s3://$VZV_DATA_BUCKET/$VZV_DATA_FILENAME . && ' +
                 'cat $VZV_DATA_FILENAME | jq -r ".data.view_vzv_header_totals" | in2csv -f json -v --no-inference > view_vzv_header_totals.csv && ' +
                 'cat $VZV_DATA_FILENAME | jq -r ".data.view_vzv_by_month_year" | in2csv -f json -v --no-inference > view_vzv_by_month_year.csv && ' +
                 'cat $VZV_DATA_FILENAME | jq -r ".data.view_vzv_by_time_of_day" | in2csv -f json -v --no-inference > view_vzv_by_time_of_day.csv && ' +
                 'cat $VZV_DATA_FILENAME | jq -r ".data.view_vzv_demographics_age_sex_eth" | in2csv -f json -v --no-inference > view_vzv_demographics_age_sex_eth.csv && ' +
                 'cat $VZV_DATA_FILENAME | jq -r ".data.view_vzv_by_mode" | in2csv -f json -v --no-inference > view_vzv_by_mode.csv && ' +
                 'aws s3 cp . s3://$VZV_DATA_BUCKET/ --recursive --exclude "*" --include "*.csv"'
    ,
    env=environment_vars,
    dag=dag,
)

#
# Send an email when done
#
email_task = EmailOperator(
    to=environment_vars.get("EMAIL_RECIPIENT", ""),
    task_id="email_task",
    subject="VZV Data Processed Successfully: {{ ds }}",
    mime_charset="utf-8",
    params={"unique_id": str(uuid.uuid4())},
    html_content="Message ID: {{ params.unique_id }}  task_key - {{ task_instance_key_str }} test_mode - {{ test_mode }} task_owner - {{ task.owner}} hostname - {{ ti.hostname }}",
    dag=dag,
)

download_data >> transform_to_csv >> email_task

if __name__ == "__main__":
    dag.cli()