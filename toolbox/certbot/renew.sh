#!/bin/bash

echo "Let's renew the certificate for airflow"

# Load the same environment variables as the Airflow stack
source /Users/atd/atd/atd-airflow/.env

# Pull op v2 (latest is currently outdated and does not include read command)
docker pull 1password/op:2
PASSWORD=$(docker run -it --rm --name op \
-e OP_CONNECT_HOST=$OP_CONNECT \
-e OP_CONNECT_TOKEN=$OP_API_TOKEN \
1password/op:2 op read op://$OP_VAULT_ID/Test/password)
echo $PASSWORD

# Renew
# CERT_PATH="/usr/local/etc/haproxy/ssl/certs/"
# cd $CERT_PATH
# rm cert.key
# rm cert.crt

# docker pull certbot/dns-route53:latest

# Use the ID and secret key from the atd-data03 IAM account
# docker run -it --rm --name certbot -e AWS_ACCESS_KEY_ID='KEY' -e AWS_SECRET_ACCESS_KEY='SECRETKEY' -v "/etc/letsencrypt:/etc/letsencrypt" -v "/var/lib/letsencrypt:/var/lib/letsencrypt"  certbot/dns-route53 certonly --dns-route53 -d airflow.austinmobility.io

# cp /etc/letsencrypt/live/airflow.austinmobility.io/privkey.pem /usr/local/etc/haproxy/ssl/cert.key

# cp /etc/letsencrypt/live/airflow.austinmobility.io/fullchain.pem /usr/local/etc/haproxy/ssl/cert.crt

# cat /etc/letsencrypt/live/airflow.austinmobility.io/cert.pem > /usr/airflow/atd-airflow/haproxy/ssl/airflow.austinmobility.io.pem

# cat /etc/letsencrypt/live/airflow.austinmobility.io/privkey.pem >> /usr/airflow/atd-airflow/haproxy/ssl/airflow.austinmobility.io.pem

# /restart-airflow.sh
