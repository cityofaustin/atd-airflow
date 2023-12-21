import os
from pendulum import datetime, duration
from airflow.decorators import dag, task
from airflow.operators.docker_operator import DockerOperator
from utils.slack_operator import task_fail_slack_alert

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

REQUIRED_SECRETS = {
    "AWS_BUCKET_NAME": {
        "opitem": "Vision Zero CR3 Metadata Extraction",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.AWS Bucket for CR3s",
    },
    "AWS_BUCKET_ENVIRONMENT": {
        "opitem": "Vision Zero CR3 Metadata Extraction",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.AWS S3 Path for CR3s",
    },
    "HASURA_ENDPOINT": {
        "opitem": "Vision Zero graphql-engine Endpoints",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.GraphQL Endpoint",
    },
    "HASURA_ADMIN_KEY": {
        "opitem": "Vision Zero graphql-engine Endpoints",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Admin Key",
    },
    "AWS_ACCESS_KEY_ID": {
        "opitem": "Vision Zero CRIS Import",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.AWS Access key",
    },
    "AWS_SECRET_ACCESS_KEY": {
        "opitem": "Vision Zero CRIS Import",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.AWS Secret key",
    },
}


@dag(
    dag_id="vz-populate-cr3-metadata",
    description="A DAG which populates CR3 metadata in the VZDB",
    schedule_interval="*/5 8-10 * * *"
    if DEPLOYMENT_ENVIRONMENT == "production"
    else None,
    catchup=False,
    tags=["repo:atd-vz-data", "vision-zero", "ocr", "cr3"],
    default_args=default_args,
)
def populate_cr3_metadata():
    @task(
        task_id="get_env_vars",
        execution_timeout=duration(seconds=30),
    )
    def get_env_vars():
        from utils.onepassword import load_dict
        return load_dict(REQUIRED_SECRETS)

    env_vars = get_env_vars()

    DockerOperator(
        task_id="run_cr3_metadata_population",
        environment=env_vars,
        image="atddocker/vz-cr3-metadata-pdfs:production",
        auto_remove=True,
        entrypoint=["/app/populate_cr3_file_metadata.py"],
        #command=["ems"],
        tty=True,
        force_pull=True,
    )


populate_cr3_metadata()
