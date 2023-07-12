# Test locally with: docker compose run --rm airflow-cli dags test atd_metrobike_trips

from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

from utils.slack_operator import task_fail_slack_alert

default_args = {
    "owner": "airflow",
    "description": "Fetch metrobike trip data from dropbox and publish to Socrata",
    "depend_on_past": False,
    "start_date": datetime(2020, 12, 31),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-metrobike:production"

# assemble env vars

REQUIRED_SECRETS = {
    "SOCRATA_API_KEY_SECRET": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SOCRATA_API_KEY_ID": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SOCRATA_APP_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
    "METROBIKE_DROPBOX_TOKEN": {
        "opitem": "Metrobike",
        "opfield": "production.Dropbox Token",
    },
}

with DAG(
    dag_id="atd_metrobike_trips",
    default_args=default_args,
    schedule_interval="33 1 * * 1",  # runs weekly at 1:33am Monday
    dagrun_timeout=timedelta(minutes=60),
    tags=["repo:atd-metrobike", "metrobike"],
    catchup=False,
) as dag:

    @task(
        task_id="get_env_vars",
        execution_timeout=timedelta(seconds=30),
    )
    def get_env_vars():
        from utils.onepassword import load_dict

        return load_dict(REQUIRED_SECRETS)

    env_vars = get_env_vars()

    t1 = DockerOperator(
        task_id="atd_metrobike_trips_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="python publish_trips.py",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
        force_pull=True,
    )

    t1
