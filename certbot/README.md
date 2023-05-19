# Airflow scripts on atd-data03

## Request certificate

The request script has already been run when Airflow was set up on atd-data03. The initial certificate expires three months from its request, and the renew script is set to run every month after to keep it valid.

## Renew certificate

The renew script is set up as a cron job with a frequency of 0 0 1 * *.

## Restart Airflow

The script is used to restart Airflow with docker compose and is stored in /root/automated-tasks/atd-airflow on atd-data03.