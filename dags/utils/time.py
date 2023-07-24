from airflow.decorators import task
from pendulum import now, parse


@task(
    task_id="get_previous_run_date",
    multiple_outputs=True,
)
def get_previous_run_date(**context):
    """Task to return the last successful run date in UTC datetime format.

    Args:
        context (dict): Airflow task context, which contains the prev_start_date_success
            variable.

    Returns:
        Dict: dict containing the last run datetime and other future formats
    """
    last_run_datetime = context.get("prev_start_date_success") or parse("1970-01-01")

    return {
        "last_run_datetime": last_run_datetime,
        # add other formats here
    }
