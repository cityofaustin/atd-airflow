from os import getenv
from airflow import DAG
from airflow.sdk import task
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert, slack_member_ids

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

REQUIRED_SECRETS = {
    "endpoint": {
        "opitem": "CCTV Service redirect endpoint",
        "opfield": f"{"production" if DEPLOYMENT_ENVIRONMENT == "production" else "development"}.endpoint",
    },
}

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1, tz="America/Chicago"),
    "retries": 0,
    "execution_timeout": duration(seconds=30),
    "on_failure_callback": task_fail_slack_alert,
}

doc_md = """
Checks that the cctv-service endpoint is reachable and that CCTV asset data is fresh.

If the cctv asset data becomes stale, it is possible that some cameras will not direct to the correct IP address. This is a minor issue unless it persists for multiple days.

If the service is down, MMC staff will not be re-directed to CCTV camera IPs from the [traffic cameras dashboard](https://data.mobility.austin.gov/traffic-cameras). This is a more significant issue and should addressed immediately.

See the atd-cctv-service readme for more details on deployment and how to restart.
"""


@task(task_id="healthcheck")
def healthcheck(env_vars):
    import requests
    from airflow.exceptions import AirflowException

    endpoint = env_vars["endpoint"]
    url = f"{endpoint}/healthz"

    try:
        res = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as e:
        # A ConnectionError would raise here, for example
        raise AirflowException(f"Could not reach {url}: {e}")

    try:
        data = res.json()
    except ValueError:
        # Non-JSON response — this would be a 500 or similar unhandled error
        raise AirflowException(f"{res.status_code}): {res.text[:200]}")

    if res.status_code != 200:
        # Get error message from response payload, if available
        message = data.get("message", "unknown error")
        raise AirflowException(f"cctv-service unhealthy: {message}")

    # log healthy check results
    print(data)


with DAG(
    dag_id="cctv_service_healthcheck",
    description="Checks the healthiness of the atd-cctv-service app",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="7 */1 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-cctv-service", "cctv"],
    catchup=False,
) as dag:

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    healthcheck(env_vars)
