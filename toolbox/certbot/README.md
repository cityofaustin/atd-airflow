# Airflow scripts on atd-data03

Heads up that these scripts assume that the Airflow project lives in `/usr/airflow/atd-airflow/`.

## Request certificate

The request script has already been run when Airflow was set up on atd-data03. The initial certificate expires three months from its request, and the renew script is set to run every month after to keep it valid.

## Renew certificate

The renew script is set up as a cron job with a frequency of 0 0 1 * *.

## Restart Airflow

The script is used to restart Airflow after renewing the certificate.