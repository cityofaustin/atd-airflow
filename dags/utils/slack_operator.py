import os

from cron_descriptor import get_description
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


# This is the Conn Id that we set when creating the connection in the Airflow dashboard
# in Admin > Connections.
SLACK_CONN_ID = "slack"

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

slack_member_ids = {
    "Frank": "<@UMS32US1E>",
    "Amenity": "<@U0PQDEMRD>",
    "Charlie": "<@U02L4JR52HX>",
    "Chia": "<@UNMD5M36G>",
    "Christina": "<@UU5TQ0ER0>",
    "David": "<@UU5TQ0ER0>",
    "Diana": "<@U0R8RC3MJ>",
    "John": "<@U09P5B7B9>",
    "Karo": "<@U034K23B45D>",
    "Mateo": "<@U6FADKWFJ>",
    "Mike": "<@UK32Y2PMZ>",
    "Rose": "<@U03JZ42NXEK>",
    "Tilly": "<@UJTGDE5A7>",
    "Andrew": "<@U01R4CKT3HT>",
}


def format_schedule(schedule_interval):
    """
    Convert a schedule interval to a human-readable format.
    This function handles different types of schedule intervals:
    - None: Returns "None" as the description.
    - str: Assumes the string is a cron expression and tries to convert it to a
        human-readable format. If conversion fails, it returns the cron
        expression as is.
    - datetime.timedelta: Converts the timedelta to a human-readable string,
        breaking it down into days, hours, minutes, and seconds.
    - Other types: Converts the interval to a string representation.
    Args:
        schedule_interval: The schedule interval to format. It can be None, a
        string (cron expression), or a datetime.timedelta.
    Returns:
        str: A human-readable description of the schedule interval.
    """

    from cron_descriptor import get_description
    import datetime

    if schedule_interval is None:
        schedule_description = "None"
        return schedule_description
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
    return schedule_description


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
            *Log URL*: {log_url} 
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


def extract_exception_from_log(log_text):
    import re

    # Find the last occurrence of 'Traceback (most recent call last):'
    traceback_start = log_text.rfind("Traceback (most recent call last):")
    if traceback_start == -1:
        return None, None  # No traceback found

    # Extract the traceback portion
    traceback_text = log_text[traceback_start:]

    # Find the last line (which usually contains the exception type and message)
    last_line = traceback_text.strip().split("\n")[-1]

    # Extract exception type and message
    match = re.match(r"([\w.]+): (.*)", last_line)
    if match:
        return match.group(1), match.group(2)

    return None, None


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
    duration = getattr(task_instance, "duration", "Not available")

    schedule_interval = dag.schedule_interval if dag else None

    schedule_description = format_schedule(schedule_interval)

    byline = getattr(dag, "byline", "")
    icon = getattr(dag, "icon", ":red_circle:")

    # Add deployment environment indication if not production
    env_indicator = ""
    if DEPLOYMENT_ENVIRONMENT != "production":
        env_indicator = f" *{DEPLOYMENT_ENVIRONMENT.capitalize()} Environment*"

    slack_msg = f"""
        {icon}{env_indicator} *Task failure* 
        {'\n\t\t' + byline if byline else ''}
        *DAG*: `{dag_id}`
        *Task*: `{task_id}`
        *Execution Time*: `{exec_date}`
        *Schedule*: `{schedule_description}`
        *Duration*: `{duration} seconds`
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
            *Log URL*: {log_url}
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
