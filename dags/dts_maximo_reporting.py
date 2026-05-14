from os import getenv

from airflow.sdk import DAG
from utils.docker_operator import DockerOperatorWithFallback
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Maximo Reporting DAG

This DAG runs a series of queries on the maximo data warehouse then sends the results to datasets in Socrata.

## Troubleshooting

You will need to be on city VPN to run this locally.

Re-triggering these DAGs is a good first step to troubleshoot potential issues.

Make the Maximo team aware if there is an issue attempting to connect to the Maximo data warehouse.

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
    "MAXIMO_DB_PASS": {
        "opitem": "Maximo Data Warehouse",
        "opfield": "production.db password",
    },
    "MAXIMO_DB_USER": {
        "opitem": "Maximo Data Warehouse",
        "opfield": "production.db username",
    },
    "MAXIMO_SERVICE_NAME": {
        "opitem": "Maximo Data Warehouse",
        "opfield": "production.service name",
    },
    "MAXIMO_HOST": {
        "opitem": "Maximo Data Warehouse",
        "opfield": "production.host",
    },
    "MAXIMO_PORT": {
        "opitem": "Maximo Data Warehouse",
        "opfield": "production.port",
    },
    "MAXIMO_BASE_URL": {
        "opitem": "Maximo Data Warehouse",
        "opfield": "production.base url",
    },
}

with DAG(
    dag_id=f"dts_maximo_reporting",
    description="Uploads the last 7 days of Maximo work orders to Socrata from the Maximo data warehouse.",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="00 6 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:dts-maximo-reporting", "socrata", "maximo"],
    catchup=False,
) as dag:
    docker_image = "atddocker/dts-maximo-reporting:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    maximo_workorders_to_socrata = DockerOperatorWithFallback(
        force_pull=True,
        task_id="maximo_workorders_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python etl/maximo_to_socrata.py --query work_orders",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    maximo_service_requests_to_socrata = DockerOperatorWithFallback(
        task_id="maximo_service_requests_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python etl/maximo_to_socrata.py --query service_requests",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    maximo_work_order_history_to_socrata = DockerOperatorWithFallback(
        task_id="maximo_work_order_history_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python etl/maximo_to_socrata.py --query work_order_status_history",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    work_order_time_logs_to_socrata = DockerOperatorWithFallback(
        task_id="work_order_time_logs_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python etl/maximo_to_socrata.py --query work_order_time_logs",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    work_order_materials_to_socrata = DockerOperatorWithFallback(
        task_id="work_order_materials_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python etl/maximo_to_socrata.py --query work_order_materials",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    work_order_specifications_to_socrata = DockerOperatorWithFallback(
        task_id="work_order_specifications_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python etl/maximo_to_socrata.py --query work_order_specifications",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    maximo_locations_to_socrata = DockerOperatorWithFallback(
        task_id="maximo_locations_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python etl/maximo_to_socrata.py --query locations",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    (
        maximo_workorders_to_socrata
        >> maximo_service_requests_to_socrata
        >> maximo_work_order_history_to_socrata
        >> work_order_time_logs_to_socrata
        >> work_order_materials_to_socrata
        >> work_order_specifications_to_socrata
        >> maximo_locations_to_socrata
    )
