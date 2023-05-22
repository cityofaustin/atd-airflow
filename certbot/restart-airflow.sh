#!/usr/bin/env bash

set -o errexit

export ATD_AIRFLOW_HOMEDIR="/usr/airflow/airflow";

echo "Restarting Airflow @ ${ATD_AIRFLOW_HOMEDIR}";

cd $ATD_AIRFLOW_HOMEDIR;

echo "Shutting down services...";
docker compose down;


echo "Starting Services...";
docker compose up -d;

echo "Done: $(date)";
