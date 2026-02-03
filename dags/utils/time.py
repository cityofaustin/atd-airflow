from __future__ import annotations

"""
Time utilities for DAGs.

Airflow 3 supports TaskFlow authoring via `airflow.sdk`. This module provides a
small, importable task that returns the current time in an XCom-safe format.
"""

from typing import Any

import pendulum

from airflow.sdk import get_current_context, task


@task
def get_current_time(tz: str = "UTC") -> str:
    """
    Return the current time as an ISO-8601 string (timezone-aware).

    Returning a string keeps XCom serialization simple and consistent.
    """

    return pendulum.now(tz).to_iso8601_string()

@task
def get_previous_success_end_time(
    context: dict[str, Any] | None = None,
    tz: str = "UTC",
    fallback_date: str = "1970-01-01",
) -> str | None:
    """
    TaskFlow task: previous successful run end time (ISO-8601) or a fallback date.

    If the task instance context doesn't contain a previous-success end time, return
    `fallback_date` (expected format: "YYYY-MM-DD"). Defaults to the Unix epoch date.
    """

    ctx = context or get_current_context()
    prev = ctx.get("prev_data_interval_end_success") or ctx.get("prev_end_date_success")

    if prev is None:
        # Validate strict date format (e.g. "1981-03-27") and return as-is.
        pendulum.from_format(fallback_date, "YYYY-MM-DD")
        return fallback_date

    try:
        return prev.in_timezone(tz).to_iso8601_string()
    except Exception:
        pendulum.from_format(fallback_date, "YYYY-MM-DD")
        return fallback_date

@task
def get_previous_success_start_time(
    context: dict[str, Any] | None = None,
    tz: str = "UTC",
    fallback_date: str = "1970-01-01",
) -> str | None:
    """
    TaskFlow task: previous successful run start time (ISO-8601) or a fallback date.

    If the task instance context doesn't contain a previous-success start time, return
    `fallback_date` (expected format: "YYYY-MM-DD"). Defaults to the Unix epoch date.
    """

    ctx = context or get_current_context()
    prev = ctx.get("prev_data_interval_start_success") or ctx.get("prev_start_date_success")
    if prev is None:
        pendulum.from_format(fallback_date, "YYYY-MM-DD")
        return fallback_date

    try:
        return prev.in_timezone(tz).to_iso8601_string()
    except Exception:
        pendulum.from_format(fallback_date, "YYYY-MM-DD")
        return fallback_date
