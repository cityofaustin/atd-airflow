from os import getenv

from cron_descriptor import get_description
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


# This is the Conn Id that we set when creating the connection in the Airflow dashboard
# in Admin > Connections.
SLACK_CONN_ID = "slack"

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

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


def extract_all_exceptions_from_log(log_text):
    import re

    exceptions = []
    
    # Split log into lines for easier processing
    lines = log_text.split('\n')
    
    # Track whether we're in a traceback section
    in_traceback = False
    current_traceback_lines = []
    
    for line in lines:
        line = line.strip()
        
        # Start of a new traceback
        if "Traceback (most recent call last):" in line:
            # Process previous traceback if we have one
            if in_traceback and current_traceback_lines:
                exception = _extract_exception_from_traceback_lines(current_traceback_lines)
                if exception:
                    exceptions.append(exception)
            
            # Start new traceback
            in_traceback = True
            current_traceback_lines = [line]
            
        elif in_traceback:
            # Check if this line ends the current traceback
            # Lines that typically end a traceback: empty lines, INFO logs, or new sections
            if (line == "" or 
                "INFO -" in line or 
                "ERROR -" in line or 
                "WARNING -" in line or
                "The above exception was the direct cause of the following exception:" in line):
                
                # Process current traceback before ending
                if current_traceback_lines:
                    exception = _extract_exception_from_traceback_lines(current_traceback_lines)
                    if exception:
                        exceptions.append(exception)
                
                # Reset for potential next traceback
                if "The above exception was the direct cause of the following exception:" in line:
                    # This indicates chained exceptions, stay in traceback mode
                    current_traceback_lines = []
                else:
                    # End of traceback section
                    in_traceback = False
                    current_traceback_lines = []
            else:
                # Add line to current traceback
                current_traceback_lines.append(line)
    
    # Process final traceback if we ended while in one
    if in_traceback and current_traceback_lines:
        exception = _extract_exception_from_traceback_lines(current_traceback_lines)
        if exception:
            exceptions.append(exception)
    
    print("Found exceptions: ", exceptions)
    return exceptions if exceptions else [(None, None)]


def _extract_exception_from_traceback_lines(traceback_lines):
    import re
    
    # Look for the exception line (usually the last non-empty line)
    for line in reversed(traceback_lines):
        line = line.strip()
        if line and not line.startswith('File ') and not line.startswith('Traceback'):
            # Try to match exception pattern: ExceptionType: message
            match = re.match(r'^([\w.]+):\s*(.*)', line)
            if match:
                return (match.group(1), match.group(2))
            
            # Sometimes exceptions don't have messages, just the type
            match = re.match(r'^([\w.]+)$', line)
            if match:
                return (match.group(1), "")
    
    return None


def task_fail_slack_alert(context):

    task_instance = context.get("task_instance")
    task = context.get("task")
    exception = context.get("exception")
    exception_type = type(exception).__name__ if exception else "Unknown"
    exception_message = (
        str(exception) if exception else "No exception message available"
    )

    # Extract all exceptions from logs if available
    all_exceptions = []
    
    # First, get exceptions from Docker container logs if available
    if exception and hasattr(exception, "logs") and exception.logs:
        logs = exception.logs
        container_exceptions = extract_all_exceptions_from_log("\n".join(logs))
        
        # Add container exceptions (filter out None entries)
        for exc in container_exceptions:
            if exc[0] is not None:
                all_exceptions.append(exc)
    
    # Always add the Airflow-level exception as well
    airflow_exception = (exception_type, exception_message)
    all_exceptions.append(airflow_exception)
    
    # Use the last exception (Airflow-level) as primary for backward compatibility
    if all_exceptions:
        exception_type = all_exceptions[-1][0]
        exception_message = all_exceptions[-1][1]

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

    # Format all exceptions for display
    exceptions_text = ""
    if len(all_exceptions) == 1:
        # Single exception - use original format
        exceptions_text = f"*Exception Type*: `{exception_type}`\n        *Exception Message*: `{exception_message}`"
    else:
        # Multiple exceptions - list them all
        exceptions_text = f"*Exceptions Found ({len(all_exceptions)} total)*:"
        for i, (exc_type, exc_msg) in enumerate(all_exceptions, 1):
            exceptions_text += f"\n        {i}. *{exc_type}*: `{exc_msg}`"

    slack_msg = f"""
        {icon}{env_indicator} *Task failure* 
        {'\n\t\t' + byline if byline else ''}
        *DAG*: `{dag_id}`
        *Task*: `{task_id}`
        *Execution Time*: `{exec_date}`
        *Schedule*: `{schedule_description}`
        *Duration*: `{duration} seconds`
        {exceptions_text}
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
