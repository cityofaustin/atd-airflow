# ATD - Airflow Docker Image

### Instructions

This is the airflow image used for Vision Zero ETL processes. To get started, be sure you have docker-compose installed, and run the command below.

Airflow's image is based off of Matthieu "Puckel_" Roisil's docker image: https://github.com/puckel/docker-airflow

### Environment Variables

You will need some environment variables to spin it up:

```
export AIRFLOW__CORE__FERNET_KEY="__your_fernet_key__"
export AIRFLOW__CORE__REMOTE_BASE_LOG_FOLDER="s3://__your_s3_bucket__/logs";
export AIRFLOW_CONN_ATD_AIRFLOW_S3_PRODUCTION="s3://__your_aws_s3_access_key__:__your_aws_s3_secret_key__@__your_s3_bucket__/__subfolder__";
```

### Docker-Compose

To run, Airflow depends on PostgreSQL and Redis (if you run with celery). These components are provided together as a small cluster in docker-compose. The cluster also includes an nginx reverse proxy to centralize web access to airflow's UI and/or any other additional services.

### PostgreSQL

Keep the postgres-data folder empty, docker will try to establish a postgres volume on here to persist its data.

##### Run Airflow for Development:

To run in dettached mode, from the docker folder run: `docker-compose -f docker-compose-dev.yml up -d`

Remove the `-d` (dettached) flag if you hare having issues, this will cause docker compose
to run in attached mode, which will show the console output of the cluster, including any errors.

Check the Airflow webclient: http://localhost:8080

Stopping the cluster: `docker-compose -f docker-compose-dev.yml down`

### Read the Docs

Once you have a local instance of Airflow running, you should read the docs folder.
