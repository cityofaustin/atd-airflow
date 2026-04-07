# test locally with: docker compose run --rm airflow-cli dags test atd_knack_purchase_request_copier

from os import getenv

from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Knack Services DAG: Purchase Request Copier

Make a copy of records flagged by users in the finance-purchasing knack app.

## Troubleshooting

You should not need to be on VPN to reach Knack.

This DAG runs very frequently, so just waiting may resolve connectivity issues automatically.

"""

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

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
        "opitem": "Knack Finance and Purchasing",
        "opfield": "production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack Finance and Purchasing",
        "opfield": "production.apiKey",
    },
}

with DAG(
    dag_id="atd_knack_purchase_request_copier",
    description="Copy requested records in the finance-purchasing knack app.",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="* * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "finance-purchasing", "finance"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-knack-services:production"
    app_name = "finance-purchasing"
    container = "view_211"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="purchase_request_copier",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/purchase_request_copier.py -a {app_name} -c {container}",
        environment=env_vars,
        tty=True,
        force_pull=False,  # atd_knack_signals pulls this image every 5 minutes
        mount_tmp_dir=False,
    )

    t1
