#!/bin/bash

echo "Let's renew the certificates for the Airflow stack"
/usr/airflow/atd-airflow/toolbox/certbot/renew.sh airflow.austinmobility.io
/usr/airflow/atd-airflow/toolbox/certbot/renew.sh airflow-webhook.austinmobility.io
/usr/airflow/atd-airflow/toolbox/certbot/renew.sh airflow-workers.austinmobility.io
/usr/airflow/atd-airflow/toolbox/certbot/renew.sh airflow-weather.austinmobility.io

# Restart the Airflow stack to use renewed certificates
/usr/airflow/atd-airflow/toolbox/certbot/restart-airflow.sh
