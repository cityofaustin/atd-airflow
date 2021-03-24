from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

from _slack_operators import *

default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Extracts the diagram and narrative out of CR3 pdfs. (production)',
        'depend_on_past'        : False,
        'start_date'            : datetime(2019, 1, 1),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(minutes=5),
        'on_failure_callback'   : task_fail_slack_alert,
}

with DAG(
        'atd_visionzero_cr3_diagram_narrative_production',
        default_args=default_args,
        schedule_interval="*/10 18-18,20-23,0-5 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["production", "visionzero"],
) as dag:
        #
        # Task: docker_command
        # Description: Runs a docker container with CentOS, and waits 30 seconds before being terminated.
        #
        t1 = BashOperator(
            task_id="run_python_script",
            bash_command="python3 ~/dags/python_scripts/cr3_extract_diagram_ocr_narrative.py -v -d --update-narrative --update-timestamp --batch 100 --cr3-source atd-vision-zero-editor production/cris-cr3-files --save-diagram-s3 atd-vision-zero-website cr3_crash_diagrams/production",
            env=Variable.get("atd_visionzero_cris_production", deserialize_json=True),
            dag=dag,
        )

        t1
