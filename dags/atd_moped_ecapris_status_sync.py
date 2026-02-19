# Test locally with: docker compose run --rm airflow-cli dags test atd_moped_ecapris_status_sync

from os import getenv

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.models.param import Param
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

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


def get_required_secrets(environment):
    return {
        "HASURA_ENDPOINT": {
            "opitem": "Moped Hasura Admin",
            "opfield": f"{environment}.Endpoint",
        },
        "HASURA_ADMIN_SECRET": {
            "opitem": "Moped Hasura Admin",
            "opfield": f"{environment}.Admin Secret",
        },
        "ORACLE_USER": {
            "opitem": "Finance Data Warehouse Oracle DB",
            "opfield": "production.Username",
        },
        "ORACLE_PASSWORD": {
            "opitem": "Finance Data Warehouse Oracle DB",
            "opfield": "production.Password",
        },
        "ORACLE_HOST": {
            "opitem": "Finance Data Warehouse Oracle DB",
            "opfield": "production.Host",
        },
        "ORACLE_PORT": {
            "opitem": "Finance Data Warehouse Oracle DB",
            "opfield": "production.Port",
        },
        "ORACLE_SERVICE": {
            "opitem": "Finance Data Warehouse Oracle DB",
            "opfield": "production.Service",
        },
    }


with DAG(
    dag_id="atd_moped_ecapris_status_sync",
    description="sync eCapris statuses to Moped database",
    default_args=DEFAULT_ARGS,
    schedule_interval=(
        "*/30 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None
    ),
    dagrun_timeout=duration(minutes=30),
    tags=["repo:atd-moped", "moped", "ecapris"],
    catchup=False,
    params={
        "target_environment": Param(
            default=DEPLOYMENT_ENVIRONMENT,
            enum=["production", "staging"],
            description="Target Moped environment. Defaults to the current deployment environment. Override to target staging manually.",
        )
    },
) as dag:
    # There is no staging tag for this image. Test locally with development or run production code against staging or production environments.
    docker_image = f"atddocker/atd-moped-etl-ecapris-statuses:{DEPLOYMENT_ENVIRONMENT}"

    target_environment = "{{ params.target_environment }}"
    REQUIRED_SECRETS = get_required_secrets(target_environment)
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="ecapris_statuses_to_moped",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python3.12 ecapris_statuses_sync.py",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
    )

    t1
