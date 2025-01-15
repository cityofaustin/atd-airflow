from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator


# This is the Conn Id that we set when creating the connection in the Airflow dashboard
# in Admin > Connections.
SLACK_CONN_ID = "slack"


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
            *Dag*: {dag} 
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
    dag = context.get("dag")
    schedule_interval = dag.schedule_interval if dag else None
    schedule_description = format_schedule(schedule_interval)

    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *DAG*: {dag} 
            *Schedule*: {schedule_description}
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        schedule_description=schedule_description,
        exec_date=get_central_time_exec_data(context),
        log_url=context.get("task_instance").log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id="slack_failure",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=slack_msg,
        username="airflow",
    )
    return failed_alert.execute(context=context)


def task_success_slack_alert(context):
    slack_msg = """
            :white_check_mark: Task Successfully Completed.
            *Task*: {task}
            *Dag*: {dag}
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
