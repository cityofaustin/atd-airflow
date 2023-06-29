# Airflow SSL scripts

For SSL termination management of the resources in this Airflow stack, we use [certbot](https://certbot.eff.org/).

## Request certificate

The request script has already been run when Airflow was set up on atd-data03. The initial certificate expires three months from its request. The request command is identical to the certbot command used in the renew script.

## Renew certificate

The renew script is set up as a cron job as root user that runs every 12 hours with output logged to `/var/log/airflow_cert_renewal.log`. The cron entry is tracked in this directory and is symlinked to `/etc/cron.d/renew_airflow_ssl_certificates`. The `renew.sh` script renews all of the domains used by the stack using the `renew_domain_with_certbot.sh` script that takes one argument like:

```bash
$ renew_domain_with_certbot.sh airflow.austinmobility.io
```
