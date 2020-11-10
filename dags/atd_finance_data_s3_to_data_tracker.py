from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator

default_args = {
    "owner": "airflow",
    "description": "Download finance data from S3 and upsert to AMD Data Tracker.",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

docker_image = "atddocker/atd-finance-data:production"
app_name = "data-tracker"

# assemble env vars
env_vars = Variable.get("atd_knack_aws", deserialize_json=True)
atd_knack_auth = Variable.get("atd_knack_auth", deserialize_json=True)
env_vars["KNACK_APP_ID"] = atd_knack_auth[app_name][env]["app_id"]
env_vars["KNACK_API_KEY"] = atd_knack_auth[app_name][env]["api_key"]

with DAG(
    dag_id="atd_finance_data_to_data_tracker",
    default_args=default_args,
    schedule_interval="21 4 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "atd-finance-data", "knack"],
    catchup=False,
) as dag:
    t1 = DockerOperator(
        task_id="task_orders",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="python /app/s3_to_knack.py task_orders data-tracker",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t2 = DockerOperator(
        task_id="units",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="python /app/s3_to_knack.py task_orders units",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1 >> t2

if __name__ == "__main__":
    dag.cli()
