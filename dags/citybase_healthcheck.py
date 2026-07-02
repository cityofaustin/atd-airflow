from os import getenv
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from pendulum import datetime, duration

from utils.slack_operator import task_fail_slack_alert, slack_member_ids

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1, tz="America/Chicago"),
    "retries": 0,
    "execution_timeout": duration(seconds=30),
    "on_failure_callback": task_fail_slack_alert,
}

doc_md = """
Checks that the citybase postback service is running and reachable.
The application runs on the bastion.

See the atd-citybase readme for more details on deployment and how to restart.

If the postback is down for an extended period of time, inform Hanna so she can check Revenue Managment for missed transactions.
"""

with DAG(
    dag_id="citybase_postback_healthcheck",
    description="Checks citybase postback for 200 response",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="*/10 * * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-citybase", "citybase"],
    catchup=False,
) as dag:

    dag.byline = f"Citybase postback healthcheck failed, {slack_member_ids['Chia']}"

    check_citybase_endpoint = HttpOperator(
        task_id="check_citybase_postback_endpoint",
        http_conn_id="citybase_https",
        endpoint="/",
        method="GET",
        extra_options={"verify": True, "timeout": 5},
        response_check=lambda response: response.json().get("status") == "OK",
    )

    check_citybase_endpoint
