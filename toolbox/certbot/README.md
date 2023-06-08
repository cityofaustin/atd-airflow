# Airflow scripts on atd-data03

## Request certificate

The request script has already been run when Airflow was set up on atd-data03. The initial certificate expires three months from its request. The request command is identical to the certbot command used in the renew script.

## Renew certificate

The renew script is set up as a cron job with a monthly frequency with output logged to `/var/log/airflow_cert_renewal.log`. 

## Restart Airflow

The script is used to restart Airflow after renewing the certificate.