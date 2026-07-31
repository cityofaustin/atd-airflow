from os import getenv

from airflow.sdk import DAG, task, Param
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

doc_md = """
## Transportation Counter ETL

This DAG downloads ecocounter data and publishes it to the open data portal. 

[Dataset link](https://datahub.austintexas.gov/dataset/Active-Transportation-Counter-Traffic-Counts/u4i6-pw3h/about_data)

### Troubleshooting

The most common issue with this script might be periodic timeouts with Socrata as this DAG can upsert a lot of data.

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
    "ECO_COUNTER_OBSERVATIONS_DATASET": {
        "opitem": "Ecocounters",
        "opfield": "socrata.observations dataset",
    },
    "ECO_COUNTER_FLOWS_DATASET": {
        "opitem": "Ecocounters",
        "opfield": "socrata.flows dataset",
    },
    "ECO_VISIO_API_KEY": {
        "opitem": "Ecocounters",
        "opfield": "ecovisio.API Key",
    },
    "ECO_VISIO_API_BASE_URL": {
        "opitem": "Ecocounters",
        "opfield": "ecovisio.base URL",
    },
}


@task(task_id="get_start_date")
def get_start_date(**context):
    # Returns the start date of this ETL. If there is no run history it will get the date 3 days in the past.
    # If there is run history it will get the date it was last run successfully, minus 3 days.
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
    dag_id="dts_transportation_counter_data",
    description="Uploads INRIX traffic signal metrics to socrata for the past few days.",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="30 5 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=[
        "repo:dts-transportation-counter-data",
        "socrata",
        "trail counters",
        "ecocounters",
        "active transportation",
    ],
    catchup=False,
    params={
        "dry_run": Param(
            title="Dry run",
            default=False,
            type="boolean",
            description_md="Applies the dry-run flag. Only tests authenticating with APIs.",
        ),
    },
) as dag:
    docker_image = "atddocker/dts-transportation-counter-data:production"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    start_date = get_start_date()

    dry_run_arg = get_is_dry_run_arg()

    t1 = DockerOperator(
        task_id="ecocounter_data_to_socrata",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"active-transportation-counters/get_eco_counter_data.py -s {start_date}{dry_run_arg}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        retries=3,
        retry_delay=duration(seconds=60),
    )

    env_vars >> start_date >> dry_run_arg >> t1
