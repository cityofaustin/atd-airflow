# stuff to make the airflow, 1Password integration work
import os
import pendulum
from airflow.decorators import dag
from airflow.operators.docker_operator import DockerOperator
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.environ.get(
    "ENVIRONMENT", "development"
)  # our current environment from ['production', 'development']

ENVIRONMENT = {
    "ENVIRONMENT": DEPLOYMENT_ENVIRONMENT,
    "OP_API_TOKEN": os.getenv("OP_API_TOKEN"),
    "OP_CONNECT": os.getenv("OP_CONNECT"),
    "OP_VAULT_ID": os.getenv("OP_VAULT_ID"),
}

# EMS
@dag(
    dag_id="vz-ems-import",
    description="A DAG which imports EMS data into the Vision Zero database.",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="America/Chicago"),
    catchup=False,
    tags=["repo:atd-vz-data", "vision-zero", "ems", "import"],
    on_failure_callback=task_fail_slack_alert,
)
def etl_ems_import():
    DockerOperator(
        task_id="run_ems_import",
        environment=dict(os.environ),
        image="atddocker/vz-afd-ems-import:production",
        auto_remove=True,
        entrypoint=["/entrypoint.sh"],
        command=["ems"],
        tty=True,
        force_pull=True,
    )


etl_ems_import()


# AFD
@dag(
    dag_id="vz-afd-import",
    description="A DAG which imports AFD data into the Vision Zero database.",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="America/Chicago"),
    catchup=False,
    tags=["repo:atd-vz-data", "vision-zero", "afd", "import"],
    on_failure_callback=task_fail_slack_alert,
)
def etl_afd_import():
    DockerOperator(
        task_id="run_ems_import",
        environment=dict(os.environ),
        image="atddocker/vz-afd-ems-import:production",
        auto_remove=True,
        entrypoint=["/entrypoint.sh"],
        command=["afd"],
        tty=True,
        force_pull=True,
    )


etl_afd_import()
