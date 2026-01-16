# Airflow Toolbox

This directory contains utility scripts and tools for managing and configuring the Airflow deployment.

## Available Tools

### [certbot](./certbot/)

SSL certificate management tools for Let's Encrypt certificates using certbot with AWS Route53 DNS validation. Handles certificate renewal and account management for Airflow domains.

See the [certbot README](./certbot/README.md) for detailed documentation.

---

### [log-purge](./log-purge/)

Automated log cleanup utility that removes old log files and empty directories. Runs as a scheduled cron job to maintain disk space by cleaning up logs older than 30 days.

---

### [pyfail](./pyfail/)

Test utility for validating error handling and exception propagation in Airflow DAGs. Intentionally raises exceptions to test error handling, logging, and alerting mechanisms.

---

