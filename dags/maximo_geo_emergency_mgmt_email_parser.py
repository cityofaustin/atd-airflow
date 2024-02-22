import os
from pendulum import datetime, duration

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "description": "Parse the most recent email received containing Maximo Emergency Management data",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=20),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "AWS_DEFAULT_REGION": {
        "opitem": "Maximo Geo Integrations",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.AWS Default Region",
    },
    "AWS_ACCESS_KEY_ID": {
        "opitem": "Maximo Geo Integrations",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.AWS Access Key",
    },
    "AWS_SECRET_ACCESS_KEY": {
        "opitem": "Maximo Geo Integrations",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.AWS Secret Access Key",
    },
}

# fmt: off
with DAG(
    dag_id=f"maximo_geo_emergency_mgmt_email_parser_{DEPLOYMENT_ENVIRONMENT}",
    default_args=default_args,
    schedule_interval="*/30 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:dts-maximo-geo-integration", "maximo", "geo", "emergency-management", "email-parser"],
    catchup=False,
) as dag:
# fmt: on

    @task(
        task_id="get_env_vars",
        execution_timeout=duration(seconds=30),
    )
    def get_env_vars():
        from utils.onepassword import load_dict

        return load_dict(REQUIRED_SECRETS)

    env_vars = get_env_vars()

    DockerOperator(
        task_id="parse_email",
        image="atddocker/maximo-geo-emergency-mgmt:production",
        api_version="auto",
        auto_remove=True,
        # command="./cr3_extract_diagram/cr3_extract_diagram_ocr_narrative.py -v -d --update-narrative --update-timestamp --batch 100 --cr3-source atd-vision-zero-editor production/cris-cr3-files --save-diagram-s3 atd-vision-zero-website cr3_crash_diagrams/production",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )
