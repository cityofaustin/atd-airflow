# test locally with: docker compose run --rm airflow-cli dags test atd_knack_services_signal_studies

from os import getenv

from airflow.sdk import task, DAG
from utils.docker_operator import DockerOperatorWithFallback
from pendulum import datetime, duration

from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Knack Services DAG: Signal Studies 

Load signal requests (view_3488) records from Knack to Socrata

## Troubleshooting

**Need VPN access or addition to security group allow list to reach Postgrest**

Most of the time just re-triggering this DAG will likely resolve any issues automatically.

The most common bug is when the underlying Knack view is changed and the corresponding Socrata dataset
was not updated to match.

"""

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "description": "Publishes all signal studies records from Knack to Socrata",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=30),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "KNACK_APP_ID": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": "production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": "production.apiKey",
    },
    "SOCRATA_API_KEY_ID": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SOCRATA_API_KEY_SECRET": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SOCRATA_APP_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
    "PGREST_ENDPOINT": {
        "opitem": "atd-knack-services PostgREST",
        "opfield": "production.endpoint",
    },
    "PGREST_JWT": {
        "opitem": "atd-knack-services PostgREST",
        "opfield": "production.jwt",
    },
}

with DAG(
    dag_id="atd_knack_services_signal_studies",
    description="Load signal requests (view_3488) records from Knack to Socrata",
    doc_md=doc_md,
    default_args=default_args,
    schedule="5 4 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "socrata"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-knack-services:production"
    app_name = "data-tracker"
    container = "view_3488"

    @task(
        task_id="get_env_vars",
        execution_timeout=duration(seconds=30),
    )
    def get_env_vars():
        from utils.onepassword import load_dict

        return load_dict(REQUIRED_SECRETS)

    env_vars = get_env_vars()

    t1 = DockerOperatorWithFallback(
        force_pull=True,
        task_id="atd_knack_signal_studies_to_postgrest",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_postgrest.py -a {app_name} -c {container}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t2 = DockerOperatorWithFallback(
        task_id="atd_knack_signal_studies_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        api_version="auto",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_socrata.py -a {app_name} -c {container}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t1 >> t2
