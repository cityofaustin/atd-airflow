"""
    Description: This scripts will re-associate crashes to locations if:
    1. The crash does not have already a location_id
    2. A location id can be found (i.e., it will not overwrite a null location id for another null)
"""
from datetime import timedelta

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from _slack_operators import task_fail_slack_alert

# First, load our environment variables as a dictionary
environment_vars = Variable.get("atd_visionzero_hasura_sql_staging", deserialize_json=True)

args = {
    "owner": "airflow",
    "start_date": days_ago(2),
    "on_failure_callback": task_fail_slack_alert
}

#
# Set up our dag
#
dag = DAG(
    dag_id="atd_visionzero_reassociate_missing_locations_staging",
    description="This script re-processes location associations in VZD",
    default_args=args,
    schedule_interval="0 3 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["staging", "visionzero"],
)

#
# Our python code execution
#
process_noncr3 = BashOperator(
    task_id="process_noncr3",
    bash_command="python3 ~/dags/python_scripts/atd_vzd_update_noncr3_locations.py",
    env=environment_vars,
    dag=dag,
)


process_cr3 = BashOperator(
    task_id="process_cr3",
    bash_command="python3 ~/dags/python_scripts/atd_vzd_update_cr3_locations.py",
    env=environment_vars,
    dag=dag,
)


process_noncr3 >> process_cr3


if __name__ == "__main__":
    dag.cli()
