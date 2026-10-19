from os import getenv

from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime, duration, instance

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Maximo Reporting DAG

This DAG runs a series of queries on the maximo data warehouse then sends the results to datasets in Socrata.

## Troubleshooting

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
    "execution_timeout": duration(minutes=150),
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
    "MAXIMO_BASE_URL": {
        "opitem": "Maximo Data Warehouse",
        "opfield": "production.base url",
    },
    "SNOWFLAKE_ACCOUNT": {
        "opitem": "City of Austin Snowflake",
        "opfield": "shared.Account",
    },
    "SNOWFLAKE_USER": {
        "opitem": "City of Austin Snowflake",
        "opfield": "shared.Snowflake Username",
    },
    "SNOWFLAKE_PRIVATE_KEY": {
        "opitem": "City of Austin Snowflake",
        "opfield": "shared.Private Key",
    },
    "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": {
        "opitem": "City of Austin Snowflake",
        "opfield": "shared.Private Key Passphrase",
    },
    "SNOWFLAKE_WAREHOUSE": {
        "opitem": "City of Austin Snowflake",
        "opfield": "maximo.warehouse",
    },
    "SNOWFLAKE_DATABASE": {
        "opitem": "City of Austin Snowflake",
        "opfield": "maximo.database",
    },
    "SNOWFLAKE_SCHEMA": {
        "opitem": "City of Austin Snowflake",
        "opfield": "maximo.schema",
    },
    "SNOWFLAKE_ROLE": {
        "opitem": "City of Austin Snowflake",
        "opfield": "maximo.role",
    },
}

def is_monday_chicago(dt) -> bool:
    # dag_run.run_after arrives as a plain datetime, so wrap it in pendulum before converting
    return instance(dt).in_timezone("America/Chicago").weekday() == 0


# On Mondays, pass --start so the ETL does a full reload from 2014 instead of the usual window.
# Evaluated by Airflow at task run time (Chicago time), not at DAG parse time.
MONDAY_START_ARG = (
    "{% if is_monday_chicago(dag_run.run_after) %} --start 2014-01-01{% endif %}"
)


def maximo_command(query: str) -> str:
    return f"python etl/maximo_to_socrata.py --query {query}{MONDAY_START_ARG}"


with DAG(
    dag_id=f"dts_maximo_reporting",
    description="Uploads the last 7 days of Maximo work orders to Socrata from the Maximo data warehouse.",
    doc_md=doc_md,
    user_defined_macros={"is_monday_chicago": is_monday_chicago},
    default_args=DEFAULT_ARGS,
    schedule="00 6 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:dts-maximo-reporting", "socrata", "maximo", "snowflake"],
    catchup=False,
) as dag:
    docker_image = "atddocker/dts-maximo-reporting:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="maximo_workorders_to_socrata",
        image=docker_image,
        auto_remove="force",
        command=maximo_command("work_orders"),
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t2 = DockerOperator(
        task_id="maximo_service_requests_to_socrata",
        image=docker_image,
        auto_remove="force",
        command=maximo_command("service_requests"),
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t3 = DockerOperator(
        task_id="maximo_work_order_history_to_socrata",
        image=docker_image,
        auto_remove="force",
        command=maximo_command("work_order_status_history"),
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t4 = DockerOperator(
        task_id="work_order_time_logs_to_socrata",
        image=docker_image,
        auto_remove="force",
        command=maximo_command("work_order_time_logs"),
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t5 = DockerOperator(
        task_id="work_order_materials_to_socrata",
        image=docker_image,
        auto_remove="force",
        command=maximo_command("work_order_materials"),
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t6 = DockerOperator(
        task_id="work_order_specifications_to_socrata",
        image=docker_image,
        auto_remove="force",
        command=maximo_command("work_order_specifications"),
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t7 = DockerOperator(
        task_id="maximo_locations_to_socrata",
        image=docker_image,
        auto_remove="force",
        command=maximo_command("locations"),
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t8 = DockerOperator(
        task_id="sbo_users_to_socrata",
        image=docker_image,
        auto_remove="force",
        command=maximo_command("users"),
        environment=env_vars,
        tty=True,
        force_pull=False,
        mount_tmp_dir=False,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
