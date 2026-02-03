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
def get_previous_success_end_time(tz: str = "UTC") -> str | None:
    """TaskFlow task: previous successful run end time (ISO-8601) or None."""

    prev = get_previous_run_end_date_from_context()
    if prev is None:
        return None
    return prev.in_timezone(tz).to_iso8601_string()

def get_previous_run_end_date_from_context(context: dict[str, Any] | None = None) -> pendulum.DateTime | None:
    """
    Best-effort previous successful run end time from Airflow context.

    Airflow exposes these in templates (and typically in the task context):
    - `prev_data_interval_end_success` (preferred)
    - `prev_end_date_success` (fallback)
    """

    ctx = context or get_current_context()
    prev = ctx.get("prev_data_interval_end_success") or ctx.get("prev_end_date_success")
    if prev is None:
        return None
    return pendulum.instance(prev)







@task
def get_previous_success_start_time(tz: str = "UTC") -> str | None:
    """TaskFlow task: previous successful run start time (ISO-8601) or None."""

    prev = get_previous_run_start_date_from_context()
    if prev is None:
        return None
    return prev.in_timezone(tz).to_iso8601_string()

def get_previous_run_start_date_from_context(context: dict[str, Any] | None = None) -> pendulum.DateTime | None:
    """
    Best-effort previous successful run start time from Airflow context.

    Airflow exposes these in templates (and typically in the task context):
    - `prev_data_interval_start_success` (preferred)
    - `prev_start_date_success` (fallback)
    """

    ctx = context or get_current_context()
    prev = ctx.get("prev_data_interval_start_success") or ctx.get("prev_start_date_success")
    if prev is None:
        return None
    return pendulum.instance(prev)

