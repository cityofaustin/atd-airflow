# test locally with: docker compose run --rm airflow-cli dags test atd_knack_inventory_transactions

from os import getenv

from airflow.sdk import DAG
from utils.docker_operator import DockerOperatorWithFallback
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.knack import get_date_filter_arg
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Knack Services DAG: Inventory Transactions

Updates a socrata dataset of AMD inventory transactions.

## Troubleshooting

**Need VPN access or addition to security group allow list to reach Postgrest**

Most of the time just re-triggering this DAG will likely resolve any issues automatically.

The most common bug is when the underlying Knack view is changed and the corresponding Socrata dataset was not updated to match.

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
    dag_id="atd_knack_inventory_transactions",
    description="Updates a socrata dataset of AMD inventory transactions.",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="33 23 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "socrata"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-knack-services:production"
    app_name = "data-tracker"
    container = "view_3897"

    date_filter_arg = get_date_filter_arg(should_replace_monthly=True)

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperatorWithFallback(
        force_pull=True,
        task_id="atd_knack_inventory_transactions_to_postgrest",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_postgrest.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t2 = DockerOperatorWithFallback(
        task_id="atd_knack_inventory_transactions_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/records_to_socrata.py -a {app_name} -c {container} {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    date_filter_arg >> t1 >> t2
