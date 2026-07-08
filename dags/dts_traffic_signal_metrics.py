from os import getenv

from airflow.sdk import DAG, task, Param
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Traffic Signal Metrics ETL

Downloads signal metrics from INRIX and uploads them to two socrata datasets:
- [Signal metrics](https://datahub.austintexas.gov/dataset/INRIX-Signal-Metrics/bfmq-ijru)
- [Signal metrics by movement](https://datahub.austintexas.gov/dataset/INRIX-Signal-Metrics-by-Movement/8qqy-h6xg/about_data)

This ETL is scheduled to get the metrics from the last time this DAG was successful minus three days to today.

Default behavior without any run history will get metrics for the last three days.

### Troubleshooting

The most common issue with this script might be periodic timeouts with Socrata as this DAG is upserting ~100k records per date.

If that happens, a retry will usually be successful. Check [socrata status page](https://status.socrata.com/) as well.

If there is a large backlog of dates to process you may need to manually remove the run history of this DAG to get it to complete before the 45 minute timeout.

This DAG is configured to run daily so that should not happen...

"""

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=45),
    "on_failure_callback": task_fail_slack_alert,
}

REQUIRED_SECRETS = {
    "SOCRATA_API_KEY": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SOCRATA_SECRET_KEY": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SOCRATA_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
    "SOCRATA_ENDPOINT": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.endpoint",
    },
    "INRIX_APP_ID": {
        "opitem": "INRIX Signal Analytics API",
        "opfield": "inrix.App ID",
    },
    "INRIX_HASH_TOKEN": {
        "opitem": "INRIX Signal Analytics API",
        "opfield": "inrix.Hash Token",
    },
    "INRIX_AUTH_URL": {
        "opitem": "INRIX Signal Analytics API",
        "opfield": "inrix.Auth URL",
    },
    "INRIX_SIGNALS_URL": {
        "opitem": "INRIX Signal Analytics API",
        "opfield": "inrix.Signals URL",
    },
    "MOVEMENTS_DATASET": {
        "opitem": "INRIX Signal Analytics API",
        "opfield": "datasets.Movements",
    },
    "SIGNALS_DATASET": {
        "opitem": "INRIX Signal Analytics API",
        "opfield": "datasets.Signals",
    },
}


@task(task_id="get_start_date")
def get_start_date(**context):
    # Returns the start date of this ETL. If there is no run history it will get the date 3 days in the past.
    # If there is run history it will get the date it was last run succesfully, minus 3 days.
    from pendulum import now

    prev_start_date = context.get("prev_start_date_success") or now()
    prev_start_date = prev_start_date.subtract(days=3)
    return prev_start_date.strftime("%Y-%m-%d")


@task(
    task_id="get_is_dry_run_arg",
)
def get_is_dry_run_arg(params):
    """Return ` --dry-run` if the dry_run param has been set"""
    if bool(params["dry_run"]):
        return " --dry-run"
    else:
        return ""


with DAG(
    dag_id=f"dts_traffic_signal_metrics",
    description="Uploads INRIX traffic signal metrics to socrata for the past few days.",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="30 5 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:dts-traffic-signal-metrics", "socrata", "inrix", "signals", "metrics"],
    catchup=False,
    params={
        "dry_run": Param(
            title="Dry run",
            default=False,
            type="boolean",
            description_md="Applies the dry-run flag. Only tests authenticating with the INRIX/Socrata APIs",
        ),
    },
) as dag:
    docker_image = "atddocker/dts-traffic-signal-metrics:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    start_date = get_start_date()

    dry_run_arg = get_is_dry_run_arg()

    t1 = DockerOperator(
        task_id="upsert_traffic_signal_metrics",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"-s {start_date}{dry_run_arg}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
    )

    env_vars >> start_date >> t1
