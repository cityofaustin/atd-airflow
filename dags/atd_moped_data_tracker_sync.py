# Test locally with: docker compose run --rm airflow-cli dags test atd_moped_data_tracker_sync

from os import getenv

from airflow.sdk import DAG
from utils.docker_operator import DockerOperatorWithFallback
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert
from utils.knack import get_date_filter_arg

doc_md = """
⚠️ Warning: Running this DAG with no previous run history is not recommended since it will process many records!

## Troubleshooting
Trigger the DAG again (as long as there is a previous successful run to pick back up on incremental updates) to address any connection errors or timeouts

## Testing
To insert a previous successful DAG run, see the "Inserting a previous DAG run to resume incremental runs using a look-back window" section in the README
"""

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

REQUIRED_SECRETS = {
    "HASURA_ENDPOINT": {
        "opitem": "Moped Hasura Admin",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Endpoint",
    },
    "HASURA_ADMIN_SECRET": {
        "opitem": "Moped Hasura Admin",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.Admin Secret",
    },
    "KNACK_DATA_TRACKER_APP_ID": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.appId",
    },
    "KNACK_DATA_TRACKER_API_KEY": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.apiKey",
    },
}


with DAG(
    dag_id="atd_moped_data_tracker_sync",
    description="sync Moped project data to Knack Data Tracker projects table",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="0 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=duration(minutes=30),
    tags=["repo:atd-moped", "moped", "data-tracker", "knack"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-moped-etl-data-tracker-sync:production"

    date_filter_arg = get_date_filter_arg()

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperatorWithFallback(
        task_id="data_tracker_sync",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python data_tracker_sync.py {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    date_filter_arg >> t1
