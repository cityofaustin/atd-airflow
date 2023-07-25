from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from pendulum import timezone

# This is the Conn Id that we set when creating the connection in the Airflow dashboard
# in Admin > Connections.
SLACK_CONN_ID = "slack"


def get_central_time_exec_data(context):
    local_tz = timezone("America/Chicago")
    execution_date_timestamp = context.get("data_interval_start")
    return local_tz.convert(execution_date_timestamp).format("MM/DD/YYYY hh:mm:ss A")


def task_fail_slack_alert_critical(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            <!channel> :red_circle: Critical Failure
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=get_central_time_exec_data(context),
        log_url=context.get("task_instance").log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id="slack_critical_failure",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )
    return failed_alert.execute(context=context)


def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=get_central_time_exec_data(context),
        log_url=context.get("task_instance").log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id="slack_failure",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )
    return failed_alert.execute(context=context)


def task_success_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :white_check_mark: Task Sucessfully Completed.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=get_central_time_exec_data(context),
        log_url=context.get("task_instance").log_url,
    )
    success_alert = SlackWebhookOperator(
        task_id="slack_success",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )
    return success_alert.execute(context=context)
