import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.knack import get_date_filter_arg
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

# secrets atd_visionzero_hasura_sql_production

REQUIRED_SECRETS = {
    "HASURA_ENDPOINT": {
        "opitem": "Vision Zero graphql-engine Endpoints",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Query Endpoint",
    },
    "HASURA_ADMIN_KEY": {
        "opitem": "Vision Zero graphql-engine Endpoints",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Admin Key",
    },
    "AWS_BUCKET_NAME": {
        "opitem": "Vision Zero CRIS CR3 Download",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.AWS_CRIS_CR3_BUCKET_NAME",
    },
    "AWS_BUCKET_ENVIRONMENT": {
        "opitem": "Vision Zero CRIS CR3 Download",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.AWS_CRIS_CR3_BUCKET_PATH_ENVIRONMENT",
    },
}

with DAG(
    dag_id="vz_process_missing_pdfs",
    description="Execute housekeeping routine manage missing or malformed CR3 PDFs",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/30 * * * *"
    if DEPLOYMENT_ENVIRONMENT == "production"
    else None,
    dagrun_timeout=duration(minutes=15),
    tags=["repo:atd_vz_data", "vision-zero", "cr3", "pdf"],
    catchup=False,
) as dag:
    docker_image = "atddocker/vz-pdf-maintenance:latest"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    cr3_temp_record_remove_pdf = DockerOperator(
        task_id="cr3_temp_record_remove_pdf",
        image=docker_image,
        auto_remove=True,
        command="python scripts/atd_vzd_cr3_temp_record_remove_pdf.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    cr3_scan_pdf_records = DockerOperator(
        task_id="cr3_scan_pdf_records",
        image=docker_image,
        auto_remove=True,
        command="python scripts/atd_vzd_cr3_scan_pdf_records.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )


cr3_temp_record_remove_pdf >> cr3_scan_pdf_records
