import os
from pendulum import datetime, duration
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "description": "Extracts the diagram and narrative out of CR3 pdfs",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-vz-cr3-extract:production"

REQUIRED_SECRETS = {
    "HASURA_ENDPOINT": {
        "opitem": "Vision Zero CRIS Import",
        "opfield": "production.GraphQL Endpoint",
    },
    "HASURA_ADMIN_KEY": {
        "opitem": "Vision Zero CRIS Import",
        "opfield": "production.GraphQL Endpoint key",
    }, 
    "AWS_ACCESS_KEY_ID": {
        "opitem": "Vision Zero CRIS Import",
        "opfield": "production.AWS Access key",
    },
    "AWS_SECRET_ACCESS_KEY": {
        "opitem": "Vision Zero CRIS Import",
        "opfield": "production.AWS Secret key",
    }
}

with DAG(
    dag_id=f"vz_cr3_ocr_narrative_extract_{DEPLOYMENT_ENVIRONMENT}",
    default_args=default_args,
    schedule_interval="*/5 8-10 * * *", # Every 5 minutes, at 8A, 9A, and 10A 
    dagrun_timeout=timedelta(minutes=5),
    tags=["repo:atd-vz-data", "vision-zero"],
    catchup=False,
) as dag:
    @task(
        task_id="get_env_vars",
        execution_timeout=timedelta(seconds=30),
    )
    def get_env_vars():
        from utils.onepassword import load_dict
        return load_dict(REQUIRED_SECRETS)

    env_vars = get_env_vars()
    
    DockerOperator(
        task_id="ocr_narrative_extract",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="./cr3_extract_diagram/cr3_extract_diagram_ocr_narrative.py -v -d --update-narrative --update-timestamp --batch 100 --cr3-source atd-vision-zero-editor production/cris-cr3-files --save-diagram-s3 atd-vision-zero-website cr3_crash_diagrams/production",
        environment=env_vars,
        tty=True,
        force_pull=True,
    )
