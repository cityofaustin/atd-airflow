import os
import docker
from datetime import datetime
from airflow.decorators import dag, task

from utils.slack_operator import task_fail_slack_alert
from utils.onepassword import load_dict

DEPLOYMENT_ENVIRONMENT = os.environ.get("ENVIRONMENT", 'development') # operating environment

@dag(
    dag_id="vz-cris-import",
    description="A process which will import the VZ CRIS data into the database on a daily basis using data on the SFTP endpoint",
    schedule="0 7 * * *",
    start_date=datetime(2023, 6, 1),
    catchup=False,
    tags=["vision-zero", "cris"],
    on_failure_callback=task_fail_slack_alert,
)
def cris_import():
    @task()
    def invoke_cris_import_via_docker():
        client = docker.from_env()
        docker_image = "atddocker/vz-cris-import:latest"
        client.images.pull(docker_image)
        logs = client.containers.run(
            image=docker_image, 
            environment= {
                "ENVIRONMENT": DEPLOYMENT_ENVIRONMENT,
                "OP_API_TOKEN": os.getenv("OP_API_TOKEN"),
                "OP_CONNECT": os.getenv("OP_CONNECT"),
                "OP_VAULT_ID": os.getenv("OP_VAULT_ID"),
            },
            auto_remove=True,
            )
        return logs.decode('utf-8')

    invoke_cris_import_via_docker()

cris_import()