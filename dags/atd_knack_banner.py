from os import getenv

from airflow.sdk import DAG
from utils.docker_operator import DockerOperatorWithFallback
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert, slack_member_ids

doc_md = """
## Troubleshooting

The most common error is when a new record is being added to the HR app but it is duplicating an existing email address.


This can happen if an employee is rehired and they are issued a new employee ID or if a person with the same name as a previous employee is given the duplicate email address.
For whatever reason, the recourse is to contact Diana to triage, contact TPW HR, then ammend records in Knack.
"""

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=30),
    "on_failure_callback": task_fail_slack_alert,
}


REQUIRED_SECRETS = {
    "BANNER_URL": {
        "opitem": "HRD Banner API",
        "opfield": f"production.endpoint",
    },
    "BANNER_API_KEY": {
        "opitem": "HRD Banner API",
        "opfield": f"production.apiKey",
    },
    "KNACK_APP_ID": {
        "opitem": "Knack Human Resources (HR)",
        "opfield": f"production.appId",
    },
    "KNACK_API_KEY": {
        "opitem": "Knack Human Resources (HR)",
        "opfield": f"production.apiKey",
    },
}


with DAG(
    dag_id=f"atd_knack_banner",
    description="Update knack HR app based on records in Banner",
    doc_md=doc_md,
    default_args=DEFAULT_ARGS,
    schedule="45 7 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    dagrun_timeout=duration(minutes=30),
    tags=["repo:atd-knack-banner", "knack", "hr"],
    catchup=False,
) as dag:
    docker_image = f"atddocker/atd-knack-banner:production"
    dag.byline = f"{slack_member_ids['Chia']}"

    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    update_employees = DockerOperatorWithFallback(
        task_id="update_employees",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"./atd-knack-banner/update_employees.py",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
    )

    update_employees
