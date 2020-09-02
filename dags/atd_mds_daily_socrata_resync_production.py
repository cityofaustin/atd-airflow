from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

from _slack_operators import *

default_args = {
    "owner": "airflow",
    "description": "Upserts the past 30 days of data into socrata",
    "depend_on_past": False,
    "start_date": datetime(2018, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

current_time = datetime.now()
current_time_min = current_time + timedelta(days=-30)
current_time_max = current_time + timedelta(days=-1, hours=-8)
time_min = f"{current_time_min.year}-{current_time_min.month}-{current_time_min.day}-01"
time_max = f"{current_time_max.year}-{current_time_max.month}-{current_time_max.day}-{current_time_max.hour}"

# Calculate the dates we need to gather data for
socrata_sync_date_start = current_time + timedelta(days=-30)
socrata_sync_date_end = current_time + timedelta(days=1)
socrata_sync_date_format = "%Y-%m-%d"

environment_vars = Variable.get("atd_mds_config_production", deserialize_json=True)
docker_image = "atddocker/atd-mds-etl:production"

with DAG(
    f"atd_mds_daily_socrata_resync_production",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    catchup=False,
    tags=["production", "mds"],
) as dag:
    #
    # Task: socrata_sync
    # Description: Upserts the mast month of data into socrata
    socrata_time_min = socrata_sync_date_start.strftime(socrata_sync_date_format)
    socrata_time_max = socrata_sync_date_end.strftime(socrata_sync_date_format)
    socrata_sync = DockerOperator(
        task_id="socrata_sync",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"./provider_full_db_sync_socrata.py  --time-min '{socrata_time_min}' --time-max '{socrata_time_max}'",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=environment_vars,
        dag=dag
    )
