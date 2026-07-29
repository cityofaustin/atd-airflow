from os import getenv

from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime, duration, now

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert
from utils.knack import get_date_filter_arg

doc_md = """
## Public Safety Incident Reports

Formerly "Traffic incidents", this DAG sends incident data from the CAD data warehouse and sends it to a socrata dataset.

## Troubleshooting

You must be on VPN to run this DAG locally.

This DAG is run every 5 minutes, so most of the errors are transient and will be resolved after a few runs by themselves.

Any outage longer than an hour should be investigated as the active AFD and Traffic incident pages on Socrata are pretty frequently
visited by the public. We also will lose the historical logging of incidents during that downtime.
"""

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=1),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    # Database
    "HOST": {
        "opitem": "Public Safety Incident Reports",
        "opfield": "production.Host",
    },
    "PORT": {
        "opitem": "Public Safety Incident Reports",
        "opfield": "production.Port",
    },
    "USER": {
        "opitem": "Public Safety Incident Reports",
        "opfield": "production.Username",
    },
    "SERVICE": {
        "opitem": "Public Safety Incident Reports",
        "opfield": "production.Service",
    },
    "PASSWORD": {
        "opitem": "Public Safety Incident Reports",
        "opfield": "production.Password",
    },
    # PostgREST
    "PGREST_TOKEN": {
        "opitem": "Public Safety Incident Reports",
        "opfield": "production.Postgrest JWT",
    },
    "PGREST_ENDPOINT": {
        "opitem": "Public Safety Incident Reports",
        "opfield": "production.Postgrest Endpoint",
    },
    # Socrata
    "TRAFFIC_RESOURCE_ID": {
        "opitem": "Public Safety Incident Reports",
        "opfield": "production.Traffic Resource ID",
    },
    "FIRE_RESOURCE_ID": {
        "opitem": "Public Safety Incident Reports",
        "opfield": "production.Fire Resource ID",
    },
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
}


with DAG(
    dag_id="dts_public_safety_incident_reports",
    description="wrapper etl for atd-traffic-incident-reports docker image connects to oracle db and updates postrgrest and socrata with fire and traffic incidents",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="*/5 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-traffic-incident-reports", "postgrest", "socrata"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-traffic-incident-reports:production"

    date_filter_arg = get_date_filter_arg(should_replace_monthly=False)

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    t1 = DockerOperator(
        task_id="public_safety_incident_reports_to_postgres",
        docker_conn_id="docker_default",
        image=docker_image,
        auto_remove="force",
        command=f"python records_to_postgrest.py",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    t2 = DockerOperator(
        task_id="public_safety_incident_reports_to_socrata",
        docker_conn_id="docker_default",
        image=docker_image,
        auto_remove="force",
        command=f"python records_to_socrata.py -date {date_filter_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    date_filter_arg >> t1 >> t2
