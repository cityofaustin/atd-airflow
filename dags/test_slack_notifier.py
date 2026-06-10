# Test DAG that verifies the Slack failure notifier is working. It defines a
# single task that deliberately raises an exception, triggering the
# task_fail_slack_alert callback so the resulting Slack alert (including the
# custom byline and icon) can be inspected. 

from __future__ import annotations

from os import getenv

import pendulum

from airflow.sdk import dag, task

from utils.slack_operator import task_fail_slack_alert, slack_member_ids

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")


@dag(
    dag_id=f"test_slack_notifier",
    schedule=None,

    # The following schedule is used in local development to test the slack notifier.
    # In particular, it is used to test the slack notifier's schedule parsing logic.
    # schedule="* * * * *",

    start_date=pendulum.datetime(2015, 12, 1, tz="America/Chicago"),
    catchup=False,
    tags=["slack"],
    default_args={
        "owner": "airflow",
        "retries": 0,
        "execution_timeout": pendulum.duration(minutes=5),
        "on_failure_callback": task_fail_slack_alert,
    },
    doc_md="Test if the Slack notifier is working",
)
def test_slack_notifier():
    """Test Slack notification on task failure."""
    # The usual suspects' slack IDs can be found in the slack_member_ids dictionary,
    # and one-off mentions can be done using the syntax <@UMS32US1E> where the ID can be
    # found in a member's profile, under the hamburger menu > Copy member ID.

    @task(
        task_id="task_fail",
    )
    def task_fail():
        """Deliberately fail to test Slack alert."""
        raise Exception("Task failure test successfully triggered")

    task_fail()


dag_instance = test_slack_notifier()

# Set custom DAG attributes for Slack notifications
if dag_instance:
    dag_instance.byline = (
        f"Example optional byline, which supports mentions: {slack_member_ids['Frank']}"
    )
    dag_instance.icon = ":test_tube:"
