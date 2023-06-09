# Airflow 2.5.3 for local/production use

## Features
* [local development](https://github.com/frankhereford/airflow#local-setup) with a high quality DX
  * you get a full, local airflow stack
    * so you can trigger it as if in airflow, via [the UI](http://localhost:8080/home)
      * stack traces available in UI
  * you can run the ETL in a terminal and get `stdout` from the program and also a color-coded output of the DAG's interactions with the airflow orchestration
    * `docker compose run --rm airflow-cli dags test weather-checker`, for example
    * continue to make changes to the code outside of docker, and they will show up in airflow as you save
* [1Password secret support](https://github.com/frankhereford/airflow/blob/main/dags/weather.py#L27-L38)
  * built in, zero-config. You give it the secret name in 1Password, it gives you the value, right in the DAG
* [working CI](https://github.com/frankhereford/airflow/blob/main/.github/workflows/production_deployment.yml), triggered [via](https://github.com/frankhereford/airflow/blob/main/haproxy/haproxy.cfg#L64) [webhook](https://github.com/frankhereford/airflow/blob/main/webhook/webhook.py#L33-L46), [secured](https://github.com/frankhereford/airflow/blob/main/webhook/webhook.py#L37) using a 1Password entry 
  * automatically pulls from `production` when PRs are merged into the branch
  * you can rotate the webhook token by opening 1Password, editing [the entry](https://github.com/frankhereford/airflow/blob/main/webhook/webhook.py#L13), generating a new password, and saving it. 10 seconds, tops. 🏁
* support for picking [environment based secrets](https://github.com/frankhereford/airflow/blob/main/dags/weather.py#L19-L23) based on local/production
  * zero-config in DAG, based out of `.env`
* supports remote workers
  * monitor their status with a [web UI](https://workers.airflow.fyi/)
    * shared credentials with admin airflow account
* [production environment](https://airflow.fyi) which runs on a `t3a.xlarge` class instance comfortably
  * full control over [production server configuration](https://github.com/frankhereford/airflow/blob/main/airflow.cfg), yet keeping the perks of a docker stack
* [customizable python environment](https://github.com/frankhereford/airflow/blob/main/requirements.txt) for DAGs, including [external, binary libraries](https://github.com/frankhereford/airflow/blob/main/Dockerfile#L1414-L1415) built right into the container
  * based on bog standard `requirements.txt` & ubuntu `apt` commands
* access to the [server's docker service](https://github.com/frankhereford/airflow/blob/main/docker-compose.yaml#L93)
  * available on worker containers and available for DAGs
  * you can package your ETL up as an image and then run it in the DAG 📦🐳
    * skip installing libraries on the server
* [flexible reverse proxy](https://github.com/frankhereford/airflow/blob/main/haproxy/haproxy.cfg#L38-L68) to distribute HTTP requests over stack
* Interface with Airflow via Web UI, CLI and API
  * CLI interface locally via: `docker compose run --rm airflow-cli <command>`
* [very minimal production deployment changes](https://github.com/frankhereford/airflow/pull/34/files)
  * server is EC2's vanilla Ubuntu LTS AMI + docker from docker's official PPA

## Building multi-architecture docker images

* Images created locally by default are ARM64 on a modern mac. They need to also support the server's architecture, which is AMD64.
* This is helpful when building images to be called as the heavy-lifting component of an ETL.
* This is not a part of building or operating the stack locally.

```
docker buildx build \
--push \
--platform linux/arm/v7,linux/arm64/v8,linux/amd64 \
--tag frankinaustin/signal-annotate:latest .
```

## Local Setup
* `.env` file:

```
AIRFLOW_UID=<the numeric output of the following command: id -u>
ENVIRONMENT=<development|production>
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=<pick your initial admin pw here>
AIRFLOW_PROJ_DIR=<absolute path of airflow checkout>
OP_API_TOKEN=<Get from 1Password here: 'name TBD'>
OP_CONNECT=<URL of the 1Password Connect install>
OP_VAULT_ID=<OP Vault ID>
```

* `docker compose build`
* `docker compose up -d`
* Airflow is available at http://localhost:8080
* The test weather DAG output at http://localhost:8081
* The webhook flask app at http://localhost:8082
* The workers' status page at http://localhost:8083


## Useful Commands
* 🐚 get a shell on a worker, for example
```
docker exec -it airflow-airflow-worker-1 bash
```

* ⛔ Stop all containers and execute this to reset your local database.
  * Do not run in production unless you feel really great about your backups. 
  * This will reset the history of your dag runs and switch states.
```
docker compose down --volumes --remove-orphans
```

## Utilities

### 1Password utility

The 1Password utility is a light wrapper of the [1Password Connect Python SDK](https://github.com/1Password/connect-sdk-python) methods. The purpose of the utility is to reduce imports of the 1Password library, its methods, and vault ID in each DAG that requires secrets.

### Slack operator utility

The Slack operator utility makes use of the integration between the Airflow and a Slack app webhook. The purpose of the utility is to add Slack notifications to DAGs using the [callback](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html#callback-types) parameters. Failure, critical failure, and success notifications are implemented.

## Ideas
* Make it disable all DAGs on start locally so it fails to safe
* Fix UID being applied by `webhook` image on `git pull`
* Create remote worker image example
  * Use `docker compose` new `profile` support
* Add slack integration
* 🤔 Extend webhook to rotate key in 1Password after every use
  * a true rolling token, 1 use per value

## Example DAGs
* You can turn on [this field](https://github.com/frankhereford/airflow/blob/main/docker-compose.yaml#L65) to get about 50 example DAGs of various complexity to borrow from
