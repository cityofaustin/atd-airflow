from airflow.decorators import task
from pendulum import now, parse


@task(
    task_id="get_previous_run_date",
    multiple_outputs=True,
)
def get_previous_run_date(fallback_date="1970-01-01", **context):
    """Task to return the last successful run date in UTC datetime format.

    Args:
        context (dict): Airflow task context, which contains the prev_start_date_success
            variable.

    Returns:
        Dict: dict containing the last run datetime and other future formats
    """
    last_run_datetime = context.get("prev_start_date_success") or parse(fallback_date)

    return {
        "last_run_datetime": last_run_datetime,
        "last_run_datetime_iso": last_run_datetime.isoformat(),
    }
