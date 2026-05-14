from os import getenv

from airflow.sdk import DAG
from utils.docker_operator import DockerOperatorWithFallback
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Pavement Ops Reporting ETL

This DAG retrieves reports from agileassets Pavement Management Info System (PMIS) and sends the results to a  socrata dataset.

## Troubleshooting

You **do not** need to be on city VPN to run this locally.

This DAG is not critical that it runs successfully daily, but if it is failing continuously we should investigate the issue.

Feel free to trigger this DAG manually to see if that fixes the issue. 

If there are issues with the credentials you might need to request a new client ID/secret using request_client.py in the dts-pavement-ops-reporting repo 
and update the item in 1pass.
"""

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=30),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "SO_KEY": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SO_SECRET": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SO_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
    "SO_WEB": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.endpoint",
    },
    "USERNAME": {
        "opitem": "Pavement ops reporting ETL",
        "opfield": "agileassets prod.agileassets username",
    },
    "PASSWORD": {
        "opitem": "Pavement ops reporting ETL",
        "opfield": "agileassets prod.agileassets password",
    },
    "CLIENT_ID": {
        "opitem": "Pavement ops reporting ETL",
        "opfield": "agileassets prod.Client ID",
    },
    "CLIENT_SECRET": {
        "opitem": "Pavement ops reporting ETL",
        "opfield": "agileassets prod.Client Secret",
    },
    "BASE_URL": {
        "opitem": "Pavement ops reporting ETL",
        "opfield": "agileassets prod.Base URL",
    },
}

with DAG(
    dag_id=f"dts_pavement_ops_reporting",
    description="Uploads reports from agileassets PMIS to socrata.",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="00 5 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:dts-pavement-ops-reporting", "socrata", "pmis", "pavement"],
    catchup=False,
) as dag:
    docker_image = "atddocker/dts-pavement-ops-reporting:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    projects_segments_to_socrata = DockerOperatorWithFallback(
        force_pull=True,
        task_id="VW_UPDATED_PROJECTS_SEGMENTS_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python etl/report_to_socrata.py --report VW_UPDATED_PROJECTS_SEGMENTS",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    projects_segments_to_socrata
