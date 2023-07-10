# stuff to make the airflow, 1Password integration work
import os
import pendulum
from airflow.decorators import dag, task

DEPLOYMENT_ENVIRONMENT = os.environ.get("ENVIRONMENT", 'development')   # our current environment from ['production', 'development']

ENVIRONMENT = {
    "ENVIRONMENT": DEPLOYMENT_ENVIRONMENT,
    "OP_API_TOKEN": os.getenv("OP_API_TOKEN"),
    "OP_CONNECT": os.getenv("OP_CONNECT"),
    "OP_VAULT_ID": os.getenv("OP_VAULT_ID"),
}

# EMS DAG

# define the parameters of the DAG
@dag(
    dag_id="vz-ems-import",
    description="A DAG which imports EMS data into the Vision Zero database.",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["repo:atd-vz-data", "vision-zero", "ems", "import"],
)

def etl_ems_import():
    import docker

    @task()
    def run_ems_import_in_docker():
        client = docker.from_env()
        docker_image = "atddocker/vz-afd-ems-import:latest"
        # pull at run time; remember to publish a amd64 image to docker hub
        client.images.pull(docker_image)
        logs = client.containers.run(
            image=docker_image, 
            environment=ENVIRONMENT,
            entrypoint=["/entrypoint.sh"],
            command=['ems'],
            auto_remove=True,
            )
        return logs.decode("utf-8")

    run_ems_import_in_docker()

etl_ems_import()




# AFD DAG

# define the parameters of the DAG
@dag(
    dag_id="vz-afd-import",
    description="A DAG which imports AFD data into the Vision Zero database.",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["repo:atd-vz-data", "vision-zero", "afd", "import"],
)

def etl_afd_import():

    @task()
    def run_afd_import_in_docker():
        client = docker.from_env()
        docker_image = "atddocker/vz-afd-ems-import:latest"
        # pull at run time; remember to publish a amd64 image to docker hub
        client.images.pull(docker_image)
        logs = client.containers.run(
            image=docker_image, 
            environment=ENVIRONMENT,
            entrypoint=["/entrypoint.sh"],
            command=['afd'],
            auto_remove=True,
            )
        return logs.decode("utf-8")

    run_afd_import_in_docker()

etl_afd_import()
