#!/bin/bash

docker pull certbot/dns-route53:latest

# Use the ID and secret key from the atd-data03 IAM account
docker run -it --rm --name certbot -e AWS_ACCESS_KEY_ID='key' -e AWS_SECRET_ACCESS_KEY='secret key' -v "/etc/letsencrypt:/etc/letsencrypt" -v "/var/lib/letsencrypt:/var/lib/letsencrypt"  certbot/dns-route53 certonly --dns-route53 -d airflow.austinmobility.io

cat /etc/letsencrypt/live/airflow.austinmobility.io/cert.pem > /usr/airflow/atd-airflow/haproxy/ssl/airflow.austinmobility.io.pem

cat /etc/letsencrypt/live/airflow.austinmobility.io/privkey.pem >> /usr/airflow/atd-airflow/haproxy/ssl/airflow.austinmobility.io.pem