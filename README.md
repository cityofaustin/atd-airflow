# ATD - Airflow Docker Image

### Instructions

This is the airflow image used for Vision Zero ETL processes. To get started, first build the image, then you can start running, developing and troubleshooting existing ETL scripts.

This image is based off of Matthieu "Puckel_" Roisil's docker image: https://github.com/puckel/docker-airflow

### Docker-Compose

Once the image is built, you will want to run docker-compose. To run, Airflow depends on PostgreSQL and Redis. These components are provided together as a small cluster in docker-compose. Redis runs within the Airflow container, while PostgreSQL will run separately. 

##### Run Airflow (in detached mode):

From the docker folder: `docker-compose -f docker-compose-localexecutor.yml up -d`

Check the Airflow webclient: http://localhost:8080

Stopping the cluster: `docker-compose -f docker-compose-localexecutor.yml down`

### Read the Docs

Once you have a local instance of Airflow running, you should read the docs folder.
