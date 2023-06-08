#!/bin/bash

echo "Let's renew the certificates for the Airflow stack"
/usr/airflow/atd-airflow/toolbox/certbot/renew.sh airflow.austinmobility.io
/usr/airflow/atd-airflow/toolbox/certbot/renew.sh airflow-webhook.austinmobility.io
/usr/airflow/atd-airflow/toolbox/certbot/renew.sh airflow-workers.austinmobility.io
/usr/airflow/atd-airflow/toolbox/certbot/renew.sh airflow-weather.austinmobility.io

# Build the HAProxy image with the renewed certificates
cd /usr/airflow/atd-airflow
docker compose build haproxy

# Restart the HAProxy stack to use renewed certificates
docker compose restart haproxy
