#!/bin/bash

echo "Renewing the certificates for the Airflow stack"
/usr/airflow/atd-airflow/toolbox/certbot/renew_domain_with_certbot.sh airflow.austinmobility.io
/usr/airflow/atd-airflow/toolbox/certbot/renew_domain_with_certbot.sh airflow-webhook.austinmobility.io
/usr/airflow/atd-airflow/toolbox/certbot/renew_domain_with_certbot.sh airflow-workers.austinmobility.io
/usr/airflow/atd-airflow/toolbox/certbot/renew_domain_with_certbot.sh airflow-weather.austinmobility.io

# Build the HAProxy image with the renewed certificates
cd /usr/airflow/atd-airflow
docker compose build haproxy

# Restart the HAProxy stack to use renewed certificates
docker compose restart haproxy
