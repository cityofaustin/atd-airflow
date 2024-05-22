from airflow.decorators import task
from pendulum import now


@task(
    task_id="get_date_filter_arg",
)
def get_date_filter_arg(should_replace_monthly=False, **context):
    """Task to get a date filter based on previous success date. If there
    is no prev success date, today's date is returned.

    Args:
        should_replace_monthly (boolean): if true, no date filter will be returned,
            which has the effect in knack services scripts of triggering a full
            truncate/replace of downstream datasets.
        context (dict): Airflow task context, which contains the prev_start_date_success
            variable.

    Returns:
        Str or None: the -d flag and ISO date string or None
    """

    today = now()
    prev_start_date = context.get("prev_start_date_success") or today

    if should_replace_monthly and today.day == 1:
        return ""

    return f"-d {prev_start_date.isoformat()}"
