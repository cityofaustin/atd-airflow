# test locally with: docker compose run --rm airflow-cli dags test atd_knack_secondary_signals

from os import getenv

from airflow.sdk import DAG
from utils.docker_operator import DockerOperatorWithFallback
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Knack Services DAG: Secondary Signals Updater

Refreshes primary <-> secondary traffic signal relationships.

## Troubleshooting

You should not need to be on VPN to reach Knack.

Most of the time just re-triggering this DAG will likely resolve any issues automatically.

Further investigation will likely require looking at the supplied Knack view to make sure the required fields are available.
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
}

with DAG(
    dag_id="atd_knack_secondary_signals",
    description="Update traffic signal records with secondary signal relationships.",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="25 2 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-knack-services", "knack", "data-tracker"],
    catchup=False,
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperatorWithFallback(
        force_pull=True,
        task_id="update_secondary_signals",
        image="atddocker/atd-knack-services:production",
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-services/services/secondary_signals_updater.py -a data-tracker -c view_197",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t1
