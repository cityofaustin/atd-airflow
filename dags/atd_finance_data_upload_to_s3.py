from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator

default_args = {
    "owner": "airflow",
    "description": "Extract finance data from controller's office database and upload to S3.",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

docker_image = "atddocker/atd-finance-data:production"

# assemble env vars
env_vars_aws = Variable.get("atd_knack_aws", deserialize_json=True)
env_vars_finance_db = Variable.get(
    "controllers_office_finance_db", deserialize_json=True
)
env_vars = {**env_vars_finance_db, **env_vars_aws}

with DAG(
    dag_id="atd_finance_data_to_s3",
    default_args=default_args,
    schedule_interval="13 4 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "atd-finance-data"],
    catchup=False,
) as dag:
    t1 = DockerOperator(
        task_id="task_orders",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="python /app/upload_to_s3.py task_orders",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1

if __name__ == "__main__":
    dag.cli()
