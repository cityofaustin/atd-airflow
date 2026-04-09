from os import getenv

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag, task
from pendulum import datetime, duration

from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
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


@dag(
    dag_id=f"maximo_geo_emergency_mgmt_email_parser_{DEPLOYMENT_ENVIRONMENT}",
    description="Parse the most recent email received containing Maximo Emergency Management data",
    default_args=default_args,
    schedule="*/30 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:dts-maximo-geo-integration", "maximo", "geo", "emergency-management", "email-parser"],
    catchup=False,
    doc_md="""
## Maximo Geo emergency management email parser

Runs 'atddocker/maximo-geo-emergency-mgmt:production' to parse the latest email that
contains Maximo Emergency Management data. AWS region and credentials are loaded from
1Password for the current deployment environment.

### Task flow

1. 'get_env_vars' — loads 'AWS_DEFAULT_REGION', 'AWS_ACCESS_KEY_ID', and
   'AWS_SECRET_ACCESS_KEY' from 1Password ('Maximo Geo Integrations').
2. 'parse_email' — Docker task that runs the parser with those variables in the container
   environment.

### Docker

Uses connection 'docker_default', 'api_version' 'auto', force-pulls the image, and sets
'mount_tmp_dir' to False.
""",
)
def maximo_geo_emergency_mgmt_email_parser():
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
        docker_conn_id="docker_default",
        auto_remove="force",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )


maximo_geo_emergency_mgmt_email_parser()
