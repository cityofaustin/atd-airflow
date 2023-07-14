from airflow.decorators import task
from pendulum import now


@task(
    task_id="get_date_filter_arg",
)
def get_date_filter_arg(should_replace_monthly=False, **context):
    """Task to get a date filter based on previous success date.

    Args:
        should_replace_monthly (boolean): if true, no date filter will be returned,
            which has the effect in knack services scripts of triggering a full
            truncate/replace of downstream datasets.
        context (dict): Airflow task context, which contains the prev_start_date_success
            variable.

    Returns:
        Str or None: and ISO date string or None
    """
    prev_start_date = context.get("prev_start_date_success")
    if should_replace_monthly and now().day == 1:
        prev_start_date = None
    return f"-d {prev_start_date}" if prev_start_date else ""
