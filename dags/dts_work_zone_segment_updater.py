from os import getenv

from airflow.sdk import task, DAG
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime, duration, now

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Work Zone Datafeed street segment updater (WZDX)

This DAG updates a socrata dataset of directional street segments daily.

## Troubleshooting

You do NOT need to be on city VPN to run this locally.

Please investigate any long term outages of this DAG as if it continually fails we might become out of sync with the source segment dataset. 

"""

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/dts-work-zone-data-feed:production"

REQUIRED_SECRETS = {
    # Socrata
    "SO_USER": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SO_PASS": {
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
    "SEGMENT_DATASET": {
        "opitem": "Work Zone Data Feed",
        "opfield": "production.segment dataset ID",
    },
    "SOURCE_SEGMENT_DATASET": {
        "opitem": "Work Zone Data Feed",
        "opfield": "production.source segment dataset ID",
    },
    # ArcGIS Online
    "AGOL_USERNAME": {
        "opitem": "ArcGIS Online (AGOL) Scripts Publisher",
        "opfield": "production.username",
    },
    "AGOL_PASSWORD": {
        "opitem": "ArcGIS Online (AGOL) Scripts Publisher",
        "opfield": "production.password",
    },
}

with DAG(
    dag_id="dts_work_zone_segment_updater",
    description="Updates street segments for the work zone feed",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="13 3 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:dts-work-zone-data-feed", "amanda", "socrata", "work zone", "wzdx", "segments"],
    catchup=False,
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="directional_segment_updater",
        image=docker_image,
        auto_remove="force",
        command=f"python geometry/street_segment_directionality.py",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=5*60),
        execution_timeout=duration(minutes=15),
    )

    t1
