"""
    Description: This script will attempt to clean up invalid CR3s in the database.
    1. First, it removes the CR3 mark from temporary records.
    2. Secondly, it scans for any CR3 records that have not yet been scanned, then indexes the contents of the files.
        The indexing is basically done as follows:
        1. First the application runs file -b --mime <crash_id>.pdf file which provides the
        mime type (application/json, application/html, application/pdf, etc. And it also
        provides the encoding type: binary, us-ascii, etc. We are looking for "application/pdf"
        files with "binary" encoding.
        2. We also scan for the size of the file in local disk, smaller files (usually less
        than 5 kilobytes) are failed CR3 attempts.

        A combination of these two factors are provided to the database, and to VZE that allows
        the UI to show or hide the download button based on trivial rules:
        - If it is not binary and not an "application/json" and the size is not greater than 5kb,
        then it's not a valid PDF.
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
    dag_id="atd_visionzero_process_missing_cr3_pdf_production",
    description="This script processes invalid CR3 PDFs",
    default_args=args,
    schedule_interval="0 3 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "visionzero"],
)

#
# This process will remove the CR3 PDF flag for any temporary records.
#
process_cr3_temp = BashOperator(
    task_id="process_cr3_temp",
    bash_command="python3 ~/dags/python_scripts/atd_vzd_temp_record_remove_cr3_pdf.py",
    env=environment_vars,
    dag=dag,
)

#
# This second process will scan for any records .
#
process_cr3_scan = BashOperator(
    task_id="process_cr3_scan",
    bash_command="python3 ~/dags/python_scripts/atd_vzd_cr3_scan_pdf_records.py",
    env=environment_vars,
    dag=dag,
)

process_cr3_temp >> process_cr3_scan

if __name__ == "__main__":
    dag.cli()
