#!/bin/bash

echo "Let's renew a certificate"
export ATD_AIRFLOW_HOMEDIR="/usr/airflow/atd-airflow";
# Assign the domain that we are renewing from the script arg
DOMAIN=$1

# Load the same environment variables as the Airflow stack
source $ATD_AIRFLOW_HOMEDIR/.env

# Pull op v2 (latest is currently outdated and does not include the op read command needed below)
docker pull 1password/op:2

# Retrieve and store the AWS Access Keys from 1Password
AWS_ACCESS_KEY_ID=$(docker run -it --rm --name op \
-e OP_CONNECT_HOST=$OP_CONNECT \
-e OP_CONNECT_TOKEN=$OP_API_TOKEN \
1password/op:2 op read op://$OP_VAULT_ID/Certbot\ IAM\ Access\ Key\ and\ Secret/accessKeyId)

AWS_SECRET_ACCESS_KEY=$(docker run -it --rm --name op \
-e OP_CONNECT_HOST=$OP_CONNECT \
-e OP_CONNECT_TOKEN=$OP_API_TOKEN \
1password/op:2 op read op://$OP_VAULT_ID/Certbot\ IAM\ Access\ Key\ and\ Secret/accessSecret)

# Now, remove the old concatenated certificates, renew the certificate, and replace with the new concatenated certificates
CERT_PATH="/usr/airflow/atd-airflow/haproxy/ssl"
cd $CERT_PATH
rm $DOMAIN.pem

docker pull certbot/dns-route53:latest

docker run -it --rm --name certbot \
-e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
-e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \ 
-v "/etc/letsencrypt:/etc/letsencrypt" \
-v "/var/lib/letsencrypt:/var/lib/letsencrypt" \ 
certbot/dns-route53 certonly --dns-route53 -d $DOMAIN

cat /etc/letsencrypt/live/$DOMAIN/cert.pem > $ATD_AIRFLOW_HOMEDIR/haproxy/ssl/$DOMAIN.pem

cat /etc/letsencrypt/live/$DOMAIN/privkey.pem >> $ATD_AIRFLOW_HOMEDIR/haproxy/ssl/$DOMAIN.pem
