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
    """
    Extract all Python exceptions from raw Airflow logs.
    Returns list of tuples (exception_type, exception_message, source).
    Source is "ETL" for exceptions before "Task failed with exception" line,
    "Airflow" for exceptions after that line.
    """

    exceptions = []
    lines = log_text.split("\n")

    # Find the "Task failed with exception" dividing line
    task_failed_line_idx = None
    for i, line in enumerate(lines):
        if "Task failed with exception" in line:
            task_failed_line_idx = i
            break

    # Look for all traceback sections
    traceback_starts = []
    for i, line in enumerate(lines):
        if "Traceback (most recent call last):" in line:
            traceback_starts.append(i)

    # Process each traceback section
    for start_idx in traceback_starts:
        # Determine source based on position relative to "Task failed with exception"
        if task_failed_line_idx is None:
            source = "ETL"  # Default to ETL if no dividing line found
        elif start_idx < task_failed_line_idx:
            source = "ETL"
        else:
            source = "Airflow"

        # Find the exception line for this traceback
        exception_line = _find_exception_line_in_traceback(lines, start_idx)

        if exception_line:
            exc_type, exc_msg = _parse_exception_line(exception_line)
            if exc_type:
                exceptions.append((exc_type, exc_msg, source))

    return exceptions if exceptions else [(None, None, "Unknown")]


def _find_exception_line_in_traceback(lines, traceback_start_idx):
    """
    Find the actual exception line (final line) in a traceback section.
    """
    # Start from the traceback line and look forward
    i = traceback_start_idx + 1

    while i < len(lines):
        line = lines[i].strip()

        # Stop if we hit another traceback
        if "Traceback (most recent call last):" in line:
            break

        # Stop if we hit certain log markers that indicate end of traceback
        if line.startswith("During handling of the above exception") or line.startswith(
            "The above exception was the direct cause"
        ):
            i += 1
            continue

        # Check if this is an exception line
        if _is_exception_line(line):
            return line

        i += 1

    return None


def _is_exception_line(line):
    """Check if a line contains a Python exception"""
    import re

    line = line.strip()

    # Skip obvious non-exception lines
    if (
        not line
        or line.startswith("File ")
        or line.startswith("    ")
        or line.startswith("^")
        or "Traceback" in line
        or line.startswith("During handling")
        or line.startswith("The above exception")
    ):
        return False

    # Look for Python exception patterns
    # Must start with a capital letter followed by word characters, dots, underscores
    # Common exception endings but not required
    exception_pattern = (
        r"^[A-Z][A-Za-z0-9_.]*(?:Error|Exception|Warning|Timeout)?(?::\s|$)"
    )
    return re.match(exception_pattern, line) is not None


def _parse_exception_line(line):
    """Parse an exception line into (type, message)"""
    import re

    line = line.strip()

    # Pattern for ExceptionType: message
    match = re.match(r"^([A-Z][A-Za-z0-9_.]*)\s*:\s*(.*)", line)
    if match:
        return match.group(1), match.group(2)

    # Pattern for just ExceptionType (no colon/message)
    match = re.match(r"^([A-Z][A-Za-z0-9_.]*)$", line)
    if match:
        return match.group(1), ""

    return None, None
