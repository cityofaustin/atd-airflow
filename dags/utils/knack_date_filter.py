from pendulum import now

def get_date_filter_arg(start_date, should_replace_date=None):
    """Returns a date filter argument that can be included in atd-knack-services
    run commands

    Args:
        prev_starstart_datet_date (Str): The 

    Returns:
        _type_: _description_
    """
    # construct date filter based on prev run
    curr_day = now().day
    if not start_date or (should_replace_date and curr_day == should_replace_date):
        # completely replace dataset by not providing a date filter arg
        return ""
    else:
        return f"-d '{start_date}'"
