#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys


def _redact_url(url: str) -> str:
    if "@" not in url:
        return url
    prefix, suffix = url.split("@", 1)
    if "://" in prefix:
        scheme, _rest = prefix.split("://", 1)
        return f"{scheme}://***@{suffix}"
    return f"***@{suffix}"


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

    cmd = [
        "airflow",
        "db",
        "clean",
        "--yes",
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
