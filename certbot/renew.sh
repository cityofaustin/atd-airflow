#!/bin/bash

echo "Let's renew the certificate for airflow"
CERT_PATH="/root/airflow/atd-airflow/certs/"
cd $CERT_PATH
DATE="$(date +"%Y-%m-%d")"
mkdir $DATE
mv cert.key $DATE
mv cert.crt $DATE

docker pull certbot/dns-route53:latest

# Use the ID and secret key from the atd-data03 IAM account
docker run -it --rm --name certbot -e AWS_ACCESS_KEY_ID='KEY' -e AWS_SECRET_ACCESS_KEY='SECRETKEY' -v "/etc/letsencrypt:/etc/letsencrypt" -v "/var/lib/letsencrypt:/var/lib/letsencrypt"  certbot/dns-route53 certonly --dns-route53 -d airflow.austinmobility.io

cp /etc/letsencrypt/live/airflow.austinmobility.io/privkey.pem /usr/local/etc/haproxy/ssl/cert.key

cp /etc/letsencrypt/live/airflow.austinmobility.io/fullchain.pem /usr/local/etc/haproxy/ssl/cert.crt

cat /etc/letsencrypt/live/airflow.austinmobility.io/cert.pem > /usr/airflow/atd-airflow/haproxy/ssl/airflow.austinmobility.io.pem

cat /etc/letsencrypt/live/airflow.austinmobility.io/privkey.pem >> /usr/airflow/atd-airflow/haproxy/ssl/airflow.austinmobility.io.pem

/root/automated-tasks/atd-airflow/restart-airflow.sh