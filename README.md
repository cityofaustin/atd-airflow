# DTS Airflow stack

This stack is used to run DTS ETL processes and the production instance is deployed on `atd-data03` at `/usr/airflow/atd-airflow`. Local development is available, and instructions are below.

The stack is composed of:

- Airflow v2 ([Docker image](https://hub.docker.com/r/apache/airflow))
- [HAProxy](https://www.haproxy.org/) to distribute HTTP requests over the stack
- [Flower](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/security/flower.html) workers dashboard to monitor remote workers

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

- The Airflow webserver is available at http://localhost:8080
  - You can log in to the dashboard using the username and password set in your `.env` file
- The Flower workers' status page available at http://localhost:8082

### DAGs

#### Developing

Once the local stack is up and running, you can start writing a new DAG by adding a script to the `dags/` folder. Any new utilities can be placed in the `dags/utils/` folder. As you develop, you can check the local Airflow webserver for any errors that are encountered when loading the DAG.

If any new files that are not DAGs or folders that don't contain DAGs are placed within the `dags/` folder, they should be added to the `dags/.airflowignore` file so the stack doesn't log errors about files that are not recognized as DAGs.

#### Tags

If a DAG corresponds with another repo, be sure to add a [tag](https://airflow.apache.org/docs/apache-airflow/stable/howto/add-dag-tags.html) with the naming convention of `repo:name-of-the-repo`.

#### Executing

Once a DAG is recognized by Airflow as valid, it will appear in the local webserver where you can trigger the DAG for testing.

You can also use this example command to execute a DAG in development. This is the CLI version of triggering the DAG manually in the web UI. Exchange `<dag-id>` with the ID you've given your DAG in the DAG decorator or configuration.
```
docker compose run --rm airflow-cli dags test <dag-id>
```

### Updating the stack

These instructions were created while updating a local Airflow stack from 2.5.3 to 2.6.1. Updates which
span larger intervals of versions or time should be given extra care in testing and in terms of reviewing
change logs.

The same process can be used when updating the stack with changes that originate from our team. These might include:
  * Adding new requirements to the `requirements.txt` for the local stack
  * Modifying the `Dockerfile`
  * Modifying the haproxy configuration

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

## HAProxy as part of the stack

This Airflow stack uses [HAProxy](https://www.haproxy.org/) as a reverse proxy to terminate incoming SSL/TLS connections and then to route the requests over HTTP to the appropriate backend web service. The SSL certificates are stored in the `haproxy/ssl` folder and are maintained by a `bash` script, executed monthly by `cron`. This script uses the EFF's CertBot service to renew and replace the SSL certificates used by HAProxy to secure the Airflow services. 

The Airflow stack contains the following web services:

* The Airflow main web UI
* The Airflow workers dashboard

In local development, we don't have any special host names we can use to differentiate which back-end service a request needs to be routed to, so we do this by listening on multiple, local ports. Depending on what port you request from, the local HAProxy will pick the correct backend web service to send your request to. Local development also does not require auth for the Flower service.

In production, however, we do have different host names assigned for each resource, so we're able to listen on a single port. Based on the hostname specified in the HTTP header which available to HAProxy after terminating the SSL connection, the proxy is able to pick which backend to route the request to. Production does require auth for the Flower service, and the username and password is set in `docker-compose.yaml` where it reuses the Airflow username and password set in the stack's environment variables.

### HAProxy operation

* The service needs to be restarted when the SSL certificates are rotated. This is normally handled by the automated renewal scripts.
* The service can be restarted independently of the rest of the stack if needed, as well. This can be done using `docker compose stop haproxy; docker compose build haproxy; docker compose up -d haproxy;` or similar, for example.

## Utilities

Utilities used by multiple DAGs 

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
