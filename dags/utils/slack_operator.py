import datetime

from cron_descriptor import get_description
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# This is the Conn Id that we set when creating the connection in the Airflow dashboard
# in Admin > Connections.
SLACK_CONN_ID = "slack"


def get_central_time_exec_data(context):
    from pendulum import timezone

    local_tz = timezone("America/Chicago")
    execution_date_timestamp = context.get("data_interval_start")
    return local_tz.convert(execution_date_timestamp).format("MM/DD/YYYY hh:mm:ss A")


def task_fail_slack_alert_critical(context):
    slack_msg = """
            <!channel> :red_circle: Critical Failure
            *Task*: {task}  
            *DAG*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        exec_date=get_central_time_exec_data(context),
        log_url=context.get("task_instance").log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id="slack_critical_failure",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=slack_msg,
        username="airflow",
    )
    return failed_alert.execute(context=context)


def task_fail_slack_alert(context):

    task_instance = context.get("task_instance")
    task = context.get("task")
    exception = context.get("exception")
    exception_type = type(exception).__name__ if exception else "Unknown"
    exception_message = (
        str(exception) if exception else "No exception message available"
    )

    # Extract additional information
    dag = context.get("dag")
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    exec_date = get_central_time_exec_data(context)
    log_url = task_instance.log_url
    try_number = task_instance.try_number
    max_tries = task.retries
    operator = task.__class__.__name__  # Gets the operator class name
    duration = getattr(task_instance, "duration", "Not available")

    schedule_interval = dag.schedule_interval if dag else None

    # Convert schedule_interval to human-readable format
    if schedule_interval is None:
        schedule_description = "None"
    elif isinstance(schedule_interval, str):
        try:
            schedule_description = get_description(schedule_interval)
        except Exception:
            schedule_description = f"Cron expression: {schedule_interval}"
    elif isinstance(schedule_interval, datetime.timedelta):
        # Format timedelta to human-readable string
        total_seconds = int(schedule_interval.total_seconds())
        periods = [
            ("day", 86400),  # 60 * 60 * 24
            ("hour", 3600),  # 60 * 60
            ("minute", 60),
            ("second", 1),
        ]
        parts = []
        for period_name, period_seconds in periods:
            if total_seconds >= period_seconds:
                period_value, total_seconds = divmod(total_seconds, period_seconds)
                part = f"{period_value} {period_name}{'s' if period_value > 1 else ''}"
                parts.append(part)
        schedule_description = "Every " + ", ".join(parts)
    else:
        schedule_description = str(schedule_interval)

    # if no retry number is allowed, this returns 0, so make it "1" to include the implied, non-repeating try
    if max_tries == 0:
        max_tries = 1

    importance = getattr(dag, "importance", None)
    icon = getattr(dag, "icon", ":red_circle:")

    slack_msg = f"""
{icon} *Task failure*
{importance}

*DAG*: `{dag_id}`
*Task*: `{task_id}`
*Execution Time*: `{exec_date}`
*Schedule*: `{schedule_description}`
*Try Number*: `{try_number} of {max_tries}`
*Duration*: `{duration} seconds`
*Operator*: `{operator}`
*Exception Type*: `{exception_type}`
*Exception Message*: `{exception_message}`
<{log_url}|*View Task Log*>
"""

    failed_alert = SlackWebhookOperator(
        task_id="slack_failure",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=slack_msg,
        username="Airflow Alert",
    )
    return failed_alert.execute(context=context)


def task_success_slack_alert(context):
    slack_msg = """
            :white_check_mark: Task Successfully Completed.
            *Task*: {task}
            *DAG*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        exec_date=get_central_time_exec_data(context),
        log_url=context.get("task_instance").log_url,
    )
    success_alert = SlackWebhookOperator(
        task_id="slack_success",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=slack_msg,
        username="airflow",
    )
    return success_alert.execute(context=context)
