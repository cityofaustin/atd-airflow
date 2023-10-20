#!/bin/bash

echo ""
echo "$(date '+%Y-%m-%d %H:%M:%S')"
echo "Renewing the certificates for the Airflow stack"


/usr/airflow/atd-airflow/toolbox/certbot/renew_domain_with_certbot.sh airflow.austinmobility.io
/usr/airflow/atd-airflow/toolbox/certbot/renew_domain_with_certbot.sh airflow-workers.austinmobility.io

cd /usr/airflow/atd-airflow

# Restart the HAProxy stack to use renewed certificates
BUILDKIT_PROGRESS=plain docker compose restart haproxy
