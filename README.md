# DTS Airflow

This repository hosts Data & Technology Service's [Airflow](https://airflow.apache.org/) code. We use Airflow as our primary orchestration platform for scheduling and monitoring our automated scripts and integrations.

The production Airflow instance is available at `https://airflow.austinmobility.io/`. It requires COA network access.

Our Airflow instance is hosted on `atd-data03` at `/usr/airflow/atd-airflow`. Local development is available, and instructions are below.

The stack is composed of:

- Airflow v2 ([Docker image](https://hub.docker.com/r/apache/airflow))
- [HAProxy](https://www.haproxy.org/) to distribute HTTP requests over the stack
- [Flower](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/security/flower.html) workers dashboard to monitor remote workers

## Table of Contents

- [Getting Started](#getting-started)
  - [Developing a new DAG](#developing-a-new-dag)
  - [Tags](#tags)
  - [Moving to production](#moving-to-production)
- [Utilities](#utilities)
  - [1Password utility](#1password-utility)
  - [Slack operator utility](#slack-operator-utility)
- [Useful Commands](#useful-commands)
- [Updating the stack](#updating-the-stack)
- [HAProxy and SSL](#haproxy-and-ssl)
  - [HAProxy operation](#haproxy-operation)

## Getting Started

1. Clone this repository and start a new development branch based on the `production` branch.

2. Create a `.env` file with the following variables:

```
AIRFLOW_UID=0
ENVIRONMENT=development
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=<Pick your initial admin password here>
AIRFLOW_PROJ_DIR=<The absolute path of your Airflow repository checkout>
# this fernet key is for testing purposes only
_AIRFLOW__CORE__FERNET_KEY=PTkIRwL-c46jgnaohlkkXfVikC-roKa95ipXfqST7JM=
_AIRFLOW__WEBSERVER__BASE_URL=http://localhost:8080
OP_API_TOKEN=<Get from 1Password entry named "TPW DTS API Accessible Secrets 1Password Connect Server Access Token">
OP_CONNECT=<Get from 1Password entry named "TPW DTS API Accessible Secrets 1Password Connect Server Access Token">
OP_VAULT_ID=<Get from 1Password entry named "TPW DTS API Accessible Secrets 1Password Connect Server Access Token">
DOCKER_HUB_USERNAME=<Get from 1Password entry named "Docker Hub">
DOCKER_HUB_TOKEN=<A docker hub access token assigned to specifically to you>
```

3. Start the Docker the stack (optionlly use the `-d` flag to run containers in the background):

```bash
$ docker compose up -d
```

4. Log in to the dashboard at ` http://localhost:8080` using the username and password set in your `.env` file.

5. The Flower workers' status page available at `http://localhost:8081`

### Developing a new DAG

Once the local stack is up and running, you can start writing a new DAG by adding a script to the `dags/` folder. Any new utilities can be placed in the `dags/utils/` folder. As you develop, you can check the local Airflow webserver for any errors that are encountered when loading the DAG.

If any new files that are not DAGs or folders that don't contain DAGs are placed within the `dags/` folder, they should be added to the `dags/.airflowignore` file so the stack doesn't log errors about files that are not recognized as DAGs.

Once a DAG is recognized by Airflow as valid, it will appear in the local webserver where you can trigger the DAG for testing.

You can also use this example command to execute a DAG in development. This is the CLI version of triggering the DAG manually in the web UI. Exchange `<dag-id>` with the ID you've given your DAG in the DAG decorator or configuration.

```
docker compose run --rm airflow-cli dags test <dag-id>
```

### Tags

If a DAG corresponds with another repo, be sure to add a [tag](https://airflow.apache.org/docs/apache-airflow/stable/howto/add-dag-tags.html) with the naming convention of `repo:name-of-the-repo`.

### Moving to production

Never commit directly to the `production` branch. Commit your changes to a development branch, push the branch to Github, and open a pull request against `production`. Once your PR is reviewed and approved, merge the branch to `production`.

Once merged, you will need to connect to our production Airflow host on the COA network, then pull down your changes from Github. Airflow will automatically load any DAG changes within five minutes. Activate your DAG through the Airflow web interface at `https://airflow.austinmobility.io/`.

```shell
# dts-int-data-p01

# become the superuser
su -;

# enter into the production airflow directory
cd /srv/atd-airflow;

# pull the changes
git pull;

# return to user-land
exit;
```

The production Airflow deployment uses a second Docker compose file which provides haproxy configuration overrides. To start the production docker compose stack use you must load both files in order:

```shell
$ docker compose -f docker-compose.yaml -f docker-compose-production.yaml up -d
```

## Utilities

Utilities used by multiple DAGs

### 1Password utility

Secrets stored in 1Password can be directly integrated into Airflow DAGs. As a best practice, DAGs should always use the 1Password utilities when accessing secrets.

The 1Password utility is a light wrapper of the [1Password Connect Python SDK](https://github.com/1Password/connect-sdk-python) methods. The utility communicates with our [self-hosted 1Password connect server](https://github.com/cityofaustin/dts-onepassword-connect) using the `OP` environment variables set in your `.env` file.

You can model your code off of existing DAGs which use our 1Password utility.

For example, this snippet fetches 1Password secrets from a task so that they can be used by subsequent tasks.

```python
from utils.onepassword import get_env_vars_task

REQUIRED_SECRETS = {
    "SOME_SECRET": {
        "opitem": "My Secret 1Pass Item",  # must match item name in 1Password vault
        "opfield": f"My Secret Value",  # must match field name in 1Password item
    },
}

with DAG(
    dag_id=f"my_dag",
    # ...other DAG settings
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    task_1 = DockerOperator(
      task_id="my_docker_task",
      image="some-image-name",
      auto_remove=True,
      command="hello_world.py",
      environment=env_vars,
      tty=True,
      force_pull=True,
    )
```

### Slack operator utility

The Slack operator utility makes use of the integration between the Airflow and a Slack app webhook. The purpose of the utility is to add Slack notifications to DAGs using the [callback](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html#callback-types) parameters. Failure, critical failure, and success notifications are implemented.

To configure the Slack operator in your local instance, from the Airflow UI go to **Admin** > **Connections** and choose **Slack API** as the **connection type**. You can find the remaining settings in 1Password under the **Airflow - Slack Bot** item.

## Useful Commands

- 🐚 get a shell on a worker, for example

```shell
$ docker exec -it airflow-airflow-worker-1 bash
```

- ⛔ Stop all containers and execute this to reset your local database.
  - Do not run in production unless you feel really great about your backups.
  - This will reset the history of your dag runs and switch states.

```shell
$ docker compose down --volumes --remove-orphans
```

Start the production docker compose stack with haproxy overrides:

```shell
$ docker compose -f docker-compose.yaml -f docker-compose-production.yaml up -d
```

## Updating the stack

Follow these steps to update the Airflow docker step. Reasons for doing this include:

- Adding new requirements to the `requirements.txt` for the local stack
- Modifying the `Dockerfile` to upgrade the Airflow version
- Modifying the haproxy configuration

#### Update Process

- Read the "Significant Changes" sections of the Airflow release notes between the versions in question: https://github.com/apache/airflow/releases/
  - Apache Airflow is a very active project, and these release notes are pretty dense. Keeping a regular update cadence will be helpful to keep up the task of updating airflow from becoming an "information overload" job.
- Snap a backup of the Airflow postgreSQL database
  - You shouldn't need it, but it can't hurt.
  - The following command requires that the stack being updated is running.
  - The string `postgres` in the following command is denoting the `docker compose` service name and not the `postgres` system database which is present on all postgres database servers. The target database is set via the environment variable `PGDATABASE`.
  - `docker compose exec -t -e PGUSER=airflow -e PGPASSWORD=airflow -e PGDATABASE=airflow postgres pg_dump > DB_backup.sql`
- Stop the Airflow stack
  - `docker compose stop`
- Compare the `docker-compose.yaml` file in a way that is easily sharable with the team if needed
  - Start a new, blank gist at https://gist.github.com/
  - Copy the source code of the older version, for example: https://raw.githubusercontent.com/apache/airflow/2.5.3/docs/apache-airflow/howto/docker-compose/docker-compose.yaml
  - Paste that into your gist and save it. Make it public if you want to demonstrate the diff to anyone.
  - Copy the source code of the newer, target version and replace the contents of the file in your gist. An example URL would be: https://raw.githubusercontent.com/apache/airflow/2.6.1/docs/apache-airflow/howto/docker-compose/docker-compose.yaml.
  - Look at the revisions of this gist and find the most recent one. This diff represents the changes from the older to the newer versions of the upstream `docker-compose.yaml` file. For example (2.5.3 to 2.6.1): https://gist.github.com/frankhereford/c844d0674e9ad13ece8e2354c657854e/revisions.
  - Consider each change, and generally, you'll want to apply these changes to the `docker-compose.yaml` file.
- Update the `FROM` line in the `Dockerfile` found in the top of the repo to the target version.
- Update the comments in the docker-compose file that reference the version number. (2X)
- Build the core docker images
  - `docker compose build`
- Build the `airflow-cli` image, which the Airflow team keeps in its own profile
  - `docker compose build airflow-cli`
- Restart the Airflow stack
  - `docker compose up -d`

## HAProxy and SSL

This Airflow stack uses [HAProxy](https://www.haproxy.org/) as a reverse proxy to terminate incoming SSL/TLS connections and then to route the requests over HTTP to the appropriate backend web service. The SSL certificates are stored in the `haproxy/ssl` folder and are maintained by a `bash` script, executed monthly by `cron`. This script uses the EFF's CertBot service to renew and replace the SSL certificates used by HAProxy to secure the Airflow services.

The Airflow stack contains the following web services:

- The Airflow main web UI
- The Airflow workers dashboard

In local development, we don't have any special host names we can use to differentiate which back-end service a request needs to be routed to, so we do this by listening on multiple, local ports. Depending on what port you request from, the local HAProxy will pick the correct backend web service to send your request to. Local development also does not require auth for the Flower service.

In production, however, we do have different host names assigned for each resource, so we're able to listen on a single port. Based on the hostname specified in the HTTP header which available to HAProxy after terminating the SSL connection, the proxy is able to pick which backend to route the request to. Production does require auth for the Flower service, and the username and password is set in `docker-compose.yaml` where it reuses the Airflow username and password set in the stack's environment variables.

### HAProxy operation

- The service needs to be restarted when the SSL certificates are rotated. This is normally handled by the automated renewal scripts.
- The service can be restarted independently of the rest of the stack if needed, as well. This can be done using `docker compose stop haproxy; docker compose build haproxy; docker compose up -d haproxy;` or similar, for example.

## Ideas

- Make it disable all DAGs on start locally so it fails to safe
- Create remote worker image example
  - Use `docker compose` new `profile` support
