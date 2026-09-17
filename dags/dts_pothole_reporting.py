from os import getenv

from airflow.sdk import DAG, task
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Waymo Reported Potholes

This DAG runs hourly to log a view of a JSON feed provided by Waze of Waymo detected potholes. 

### Troubleshooting

The most common issue with this script will be periodic issues with Socrata especially during maintenance periods.

If it fails to run a few times in a row it is not a big deal. Long term outages should be investigated further.

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
    "SOCRATA_ENDPOINT": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.endpoint",
    },
    "WAZE_PARTNER_ID": {
        "opitem": "Waze Potholes Reporting",
        "opfield": "waze.partner ID",
    },
    "WAZE_TOKEN": {
        "opitem": "Waze Potholes Reporting",
        "opfield": "waze.token",
    },
    "POTHOLE_LOG_DATASET": {
        "opitem": "Waze Potholes Reporting",
        "opfield": "socrata.dataset",
    },
}

with DAG(
    dag_id="dts_pothole_reporting",
    description="Logs a JSON feed of Waymo-reported potholes.",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="4 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=[
        "repo:dts-pothole-reporting",
        "waymo",
        "waze",
        "potholes",
        "socrata",
    ],
    catchup=False,
) as dag:
    docker_image = "atddocker/dts-pothole-reporting:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="log_waze_potholes",
        image=docker_image,
        auto_remove="force",
        command="waze/waze_report_logging.py",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=30),
    )

    env_vars >> t1
