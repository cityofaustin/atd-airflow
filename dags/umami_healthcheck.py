from os import getenv
from airflow import DAG
from airflow.sdk import task
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert, slack_member_ids

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

UMAMI_ENDPOINT = 'https://umami.austinmobility.io/api/heartbeat'

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1, tz="America/Chicago"),
    "retries": 0,
    "execution_timeout": duration(seconds=30),
    "on_failure_callback": task_fail_slack_alert,
}

doc_md = """
Checks that the Umami backend endpoint is reachable and returns `{"ok": true}`.

See the tpw-umami-analytics readme for more details on deployment.
"""


@task(task_id="healthcheck")
def healthcheck(url: str):
    import requests
    from airflow.exceptions import AirflowException

    try:
        res = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as e:
        # A ConnectionError would raise here
        raise AirflowException(f"Could not reach {url}: {e}")

    try:
        data = res.json()
    except ValueError:
        # Non-JSON response — this would be a 500 or similar unhandled error
        raise AirflowException(f"{res.status_code}): {res.text[:200]}")

    if res.status_code != 200:
        # Get error message from response payload, if available
        message = data.get("message", "unknown error")
        raise AirflowException(f"Umami backend unhealthy: {message}")

    if data.get("ok") is not True:
        raise AirflowException(
            f"Umami backend unhealthy: expected {{'ok': true}}, got {data}"
        )

    # log healthy check results
    print(data)


with DAG(
    dag_id="umami_healthcheck",
    description="Checks the healthiness of the Umami backend",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="0 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:tpw-umami-analytics", "umami"],
    catchup=False,
) as dag:

    healthcheck(UMAMI_ENDPOINT)
