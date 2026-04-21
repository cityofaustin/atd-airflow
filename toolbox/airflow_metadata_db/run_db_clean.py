#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys

import urllib.parse

def _redact_url(url: str) -> str:
    """Redact credentials from a database URL for logging purposes."""
    # Parse the URL so we can safely remove any userinfo from the authority.
    parsed = urllib.parse.urlsplit(url)
    # If there is no network location (netloc), there is nothing obvious to redact.
    if not parsed.netloc:
        return url
    # Separate any userinfo from the host/port by splitting at the last "@",
    # so that "@" characters in passwords are safely discarded with the userinfo.
    userinfo, sep, hostport = parsed.netloc.rpartition("@")
    if not sep:
        # No "@" in netloc → no credentials to redact.
        return url
    redacted_netloc = f"***@{hostport}"
    redacted = parsed._replace(netloc=redacted_netloc)
    return urllib.parse.urlunsplit(redacted)




def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run `airflow db clean` using an explicit metadata DB connection URL. "
            "This is intended for Airflow 3 task runtime, where metadata DB access "
            "is blocked by default."
        )
    )
    parser.add_argument(
        "--clean-before-timestamp",
        required=True,
        help="ISO8601 timestamp used by `airflow db clean`.",
    )
    args = parser.parse_args()

    db_url = os.getenv("AIRFLOW_DB_CLEAN_SQL_ALCHEMY_CONN")
    if not db_url:
        print(
            "Missing required env var AIRFLOW_DB_CLEAN_SQL_ALCHEMY_CONN. "
            "Set it to the metadata DB SQLAlchemy URL.",
            file=sys.stderr,
        )
        return 2

    env = os.environ.copy()
    env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = db_url
    # Ensure command-based URL resolution does not override the explicit URL.
    env.pop("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD", None)

    # dag_version is excluded: Airflow 3 incorrectly tries to delete dag_version rows
    # that are still referenced by task_instance rows within the retention window,
    # causing a FK RestrictViolation. dag_version is small and low-value to prune.
    tables = "callback_request,task_instance_history,xcom,task_instance,import_error,trigger,deadline,dag_run"
    cmd = [
        "airflow",
        "db",
        "clean",
        "--yes",
        "--tables",
        tables,
        "--clean-before-timestamp",
        args.clean_before_timestamp,
    ]
    print(f"Executing: {' '.join(cmd)}")
    print(
        "Using metadata DB URL from AIRFLOW_DB_CLEAN_SQL_ALCHEMY_CONN: "
        f"{_redact_url(db_url)}"
    )

    result = subprocess.run(cmd, env=env, check=False)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
