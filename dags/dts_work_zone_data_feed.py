from os import getenv

from airflow.sdk import task, DAG
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime, duration, now

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Work Zone Datafeed (WZDX)

This DAG creates a json datafeed of road closures in Austin based on data retrieved from AMANDA and Coordinate.

## Troubleshooting

You need to be on city VPN to run this locally.

Please investigate any long term outages of this DAG as the data informs the public about road closures and work zones.

"""

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=10),
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
    "FEED_DATASET": {
        "opitem": "Work Zone Data Feed",
        "opfield": "production.feed dataset ID",
    },
    "FLAT_DATASET": {
        "opitem": "Work Zone Data Feed",
        "opfield": "production.flat dataset ID",
    },
    "SEGMENT_DATASET": {
        "opitem": "Work Zone Data Feed",
        "opfield": "production.segment dataset ID",
    },
    # AMANDA
    "HOST": {
        "opitem": "Amanda Read-Only (RO) replica database",
        "opfield": "production.host",
    },
    "PORT": {
        "opitem": "Amanda Read-Only (RO) replica database",
        "opfield": "production.port",
    },
    "SERVICE_NAME": {
        "opitem": "Amanda Read-Only (RO) replica database",
        "opfield": "production.service",
    },
    "DB_USER": {
        "opitem": "Amanda Read-Only (RO) replica database",
        "opfield": "production.username",
    },
    "DB_PASS": {
        "opitem": "Amanda Read-Only (RO) replica database",
        "opfield": "production.password",
    },
    # Contact Email
    "CONTACT_EMAIL": {
        "opitem": "Work Zone Data Feed",
        "opfield": "production.contact email",
    },
    # Coordinate
    "COORDINATE_USER": {
        "opitem": "Work Zone Data Feed",
        "opfield": "coordinate.coordinate username",
    },
    "COORDINATE_BASE_URL": {
        "opitem": "Work Zone Data Feed",
        "opfield": "coordinate.base url",
    },
    "COORDINATE_PASSWORD": {
        "opitem": "Work Zone Data Feed",
        "opfield": "coordinate.coordinate password",
    },
}

with DAG(
    dag_id="dts_work_zone_data_feed",
    description="Publishing AMANDA work zone data to Socrata.",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="0 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:dts-work-zone-data-feed", "amanda", "socrata", "work zone", "wzdx"],
    catchup=False,
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="work_zone_data_publishing",
        image=docker_image,
        auto_remove="force",
        command=f"python data_sources/amanda_closure_publishing.py",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
    )

    t1
