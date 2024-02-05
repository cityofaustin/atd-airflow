import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration

from utils.knack import get_date_filter_arg
from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "KNACK_APP_ID": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"production.apiKey",
    },
    "AGOL_USERNAME": {
        "opitem": "ArcGIS Online (AGOL) Scripts Publisher",
        "opfield": "production.username",
    },
    "AGOL_PASSWORD": {
        "opitem": "ArcGIS Online (AGOL) Scripts Publisher",
        "opfield": "production.password",
    },
}

with DAG(
    dag_id=f"atd_knack_data_tracker_location_updater",
    description="With data from AGOL, update the Signal Engineer Area field in Knack ",
    default_args=DEFAULT_ARGS,
    schedule_interval="19 7 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "data-tracker", "agol"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-knack-services:production"
    app_name = "data-tracker"
    container = "view_1201" 

    env_vars = get_env_vars_task(REQUIRED_SECRETS)
    
    date_filter_arg = get_date_filter_arg()

    t1 = DockerOperator(
        task_id="update_locations",
        image= "atddocker/atd-knack-services:production",
        docker_conn_id="docker_default",
        auto_remove=True,
        command=f"./atd-knack-services/services/knack_location_updater.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    date_filter_arg >> t1
