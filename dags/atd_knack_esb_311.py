import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from docker.types import Mount 
from pendulum import datetime, duration, now

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

DOCKER_IMAGE ="atddocker/atd-knack-311:production"

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

REQUIRED_SECRETS_DATA_TRACKER = {
    "KNACK_APP_ID": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"production.apiKey",
    },
    "ESB_ENDPOINT": {
        "opitem": "CTM Enterprise Service Bus - ESB - 311 Interface",
        "opfield": f"production.endpoint",
    }
}

REQUIRED_SECRETS_SIGNS_MARKIGNS = {
    "KNACK_APP_ID": {
        "opitem": "Knack Signs and Markings",
        "opfield": f"production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack Signs and Markings",
        "opfield": f"production.apiKey",
    },
    "ESB_ENDPOINT": {
        "opitem": "CTM Enterprise Service Bus - ESB - 311 Interface",
        "opfield": f"production.endpoint",
    }
}


with DAG(
    dag_id=f"atd_knack_esb_311",
    description="Publishes 311 SR activities from Knack to 311 CSR via the CTM ESB",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/7 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-311", "311", "knack", "esb"],
    catchup=False,
) as dag:
    env_vars_data_tracker = get_env_vars_task(REQUIRED_SECRETS_DATA_TRACKER)
    env_vars_signs_markings = get_env_vars_task(REQUIRED_SECRETS_SIGNS_MARKIGNS)

    # the self-signed certificate and key must be stored within the airflow project directory
    # accoriding to the path specified in the docker compose volumne definition
    cert_mount =  Mount(
        source="atd-airflow_knack-certs", 
        target="/app/atd-knack-311/certs",
        type="volume"
    )

    t1 = DockerOperator(
        task_id="knack_amd_data_tracker_activities_to_311",
        image=DOCKER_IMAGE,
        auto_remove=True,
        command="./atd-knack-311/send_knack_messages_to_esb.py data-tracker",
        environment=env_vars_data_tracker,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        network_mode="bridge",
        mounts=[cert_mount]
    )

    t2 = DockerOperator(
        task_id="knack_amd_signs_markings_activities_to_311",
        image=DOCKER_IMAGE,
        auto_remove=True,
        command="./atd-knack-311/send_knack_messages_to_esb.py signs-markings",
        environment=env_vars_signs_markings,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        network_mode="bridge",
        mounts=[cert_mount]
    )

    t1 >> t2
