# DTS Airflow stack

This stack is used to run DTS ETL processes and the production instance is deployed on `atd-data03`. Local development is available, and instructions are below.

The stack is composed of:

- Airflow v2 ([Docker image](https://hub.docker.com/r/apache/airflow))
- [HAProxy](https://www.haproxy.org/) to distribute HTTP requests over the stack
- [Flower](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/security/flower.html) workers dashboard to monitor remote workers
- Webhook to trigger git pulls using the [smee.io client](https://github.com/probot/smee-client)

## Getting Started

### Local Setup

To get started, create a `.env` file with the following variables:

```
AIRFLOW_UID=0
ENVIRONMENT=development
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=<Pick your initial admin password here>
AIRFLOW_PROJ_DIR=<The absolute path of your Airflow repository checkout>
OP_API_TOKEN=<Get from 1Password entry named "Connect Server: Production Access Token: API Accessible Secrets">
OP_CONNECT=<Get from 1Password entry named "Endpoint for 1Password Connect Server API">
OP_VAULT_ID=<Get from 1Password entry named "Vault ID of API Accessible Secrets vault">
```

Then, to build and start the stack:

```bash
$ docker compose build
$ docker compose up -d
```

Now,

- Airflow is available at http://localhost:8080
  - You can log in to the dashboard using the username and password set in your `.env` file
- The test weather DAG output is available at http://localhost:8081
- The webhook flask app is available at http://localhost:8082
- The workers' status page is available at http://localhost:8083

### Developing a DAG

### Updating the stack

## CI/CD

## Utilities

### 1Password utility

The 1Password utility is a light wrapper of the [1Password Connect Python SDK](https://github.com/1Password/connect-sdk-python) methods. The purpose of the utility is to reduce imports of the 1Password library, its methods, and vault ID in each DAG that requires secrets.

### Slack operator utility

The Slack operator utility makes use of the integration between the Airflow and a Slack app webhook. The purpose of the utility is to add Slack notifications to DAGs using the [callback](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html#callback-types) parameters. Failure, critical failure, and success notifications are implemented.

## Useful Commands

- 🐚 get a shell on a worker, for example

```
docker exec -it airflow-airflow-worker-1 bash
```

- ⛔ Stop all containers and execute this to reset your local database.
  - Do not run in production unless you feel really great about your backups.
  - This will reset the history of your dag runs and switch states.

```
docker compose down --volumes --remove-orphans
```

## Ideas

- Make it disable all DAGs on start locally so it fails to safe
- Create remote worker image example
  - Use `docker compose` new `profile` support
- 🤔 Extend webhook to rotate key in 1Password after every use
  - a true rolling token, 1 use per value
